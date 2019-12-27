//! The `packet` module defines data structures and methods to pull data from the network.
use crate::recvmmsg::recvmmsg;
use crate::result::{Error, Result};
use crate::bvm_types::*;
use bincode;
use byteorder::{ByteOrder, LittleEndian};
use serde::Serialize;
use morgan_metricbot::inc_new_counter_debug;
use morgan_interface::hash::Hash;
pub use morgan_interface::constants::PACKET_DATA_SIZE;
use morgan_interface::bvm_address::BvmAddr;
use std::borrow::Borrow;
use std::cmp;
use std::fmt;
use std::io;
use std::io::Cursor;
use std::io::Write;
use std::mem::size_of;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};
use morgan_helper::logHelper::*;

pub type ArcBlb = Arc<RwLock<Blob>>;
pub type ArcBlbBndl = Vec<ArcBlb>;


#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
#[repr(C)]
pub struct PktMeta {
    pub size: usize,
    pub forward: bool,
    pub addr: [u16; 8],
    pub port: u16,
    pub v6: bool,
}

#[derive(Clone)]
#[repr(C)]
pub struct Pkt {
    pub data: [u8; PACKET_DATA_SIZE],
    pub meta: PktMeta,
}

impl Pkt {
    pub fn new(data: [u8; PACKET_DATA_SIZE], meta: PktMeta) -> Self {
        Self { data, meta }
    }
}

impl fmt::Debug for Pkt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Pkt {{ size: {:?}, addr: {:?} }}",
            self.meta.size,
            self.meta.addr()
        )
    }
}

impl Default for Pkt {
    fn default() -> Pkt {
        Pkt {
            data: unsafe { std::mem::uninitialized() },
            meta: PktMeta::default(),
        }
    }
}

impl PartialEq for Pkt {
    fn eq(&self, other: &Pkt) -> bool {
        let self_data: &[u8] = self.data.as_ref();
        let other_data: &[u8] = other.data.as_ref();
        self.meta == other.meta && self_data == other_data
    }
}

impl PktMeta {
    pub fn addr(&self) -> SocketAddr {
        if !self.v6 {
            let addr = [
                self.addr[0] as u8,
                self.addr[1] as u8,
                self.addr[2] as u8,
                self.addr[3] as u8,
            ];
            let ipv4: Ipv4Addr = From::<[u8; 4]>::from(addr);
            SocketAddr::new(IpAddr::V4(ipv4), self.port)
        } else {
            let ipv6: Ipv6Addr = From::<[u16; 8]>::from(self.addr);
            SocketAddr::new(IpAddr::V6(ipv6), self.port)
        }
    }

    pub fn set_addr(&mut self, a: &SocketAddr) {
        match *a {
            SocketAddr::V4(v4) => {
                let ip = v4.ip().octets();
                self.addr[0] = u16::from(ip[0]);
                self.addr[1] = u16::from(ip[1]);
                self.addr[2] = u16::from(ip[2]);
                self.addr[3] = u16::from(ip[3]);
                self.addr[4] = 0;
                self.addr[5] = 0;
                self.addr[6] = 0;
                self.addr[7] = 0;
                self.v6 = false;
            }
            SocketAddr::V6(v6) => {
                self.addr = v6.ip().segments();
                self.v6 = true;
            }
        }
        self.port = a.port();
    }
}

#[derive(Debug, Clone)]
pub struct BndlPkt {
    pub packets: Vec<Pkt>,
}

impl Default for BndlPkt {
    fn default() -> BndlPkt {
        BndlPkt {
            packets: Vec::with_capacity(NUM_RCVMMSGS),
        }
    }
}

impl BndlPkt {
    pub fn new(packets: Vec<Pkt>) -> Self {
        Self { packets }
    }

    pub fn set_addr(&mut self, addr: &SocketAddr) {
        for m in self.packets.iter_mut() {
            m.meta.set_addr(&addr);
        }
    }
}

#[repr(align(16))] 
pub struct BlobContext {
    pub data: [u8; BLOB_SIZE],
}

impl Clone for BlobContext {
    fn clone(&self) -> Self {
        BlobContext { data: self.data }
    }
}

impl Default for BlobContext {
    fn default() -> Self {
        BlobContext {
            data: [0u8; BLOB_SIZE],
        }
    }
}

impl PartialEq for BlobContext {
    fn eq(&self, other: &BlobContext) -> bool {
        let self_data: &[u8] = self.data.as_ref();
        let other_data: &[u8] = other.data.as_ref();
        self_data == other_data
    }
}


impl Deref for Blob {
    type Target = BlobContext;

    fn deref(&self) -> &Self::Target {
        &self._data
    }
}
impl DerefMut for Blob {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self._data
    }
}

#[derive(Clone, Default, PartialEq)]
pub struct Blob {
    _data: BlobContext, 
    pub meta: PktMeta,
}

impl fmt::Debug for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Blob {{ size: {:?}, addr: {:?} }}",
            self.meta.size,
            self.meta.addr()
        )
    }
}

#[derive(Debug)]
pub enum BlobErr {
    BadState,
    VerificationFailed,
}

impl BndlPkt {
    pub fn listen_to(&mut self, socket: &UdpSocket) -> Result<usize> {
        let mut i = 0;
        socket.set_nonblocking(false)?;
        trace!("receiving on {}", socket.local_addr().unwrap());
        loop {
            self.packets.resize(i + NUM_RCVMMSGS, Pkt::default());
            match recvmmsg(socket, &mut self.packets[i..]) {
                Err(_) if i > 0 => {
                    break;
                }
                Err(e) => {
                    trace!("listen_to err {:?}", e);
                    return Err(Error::IO(e));
                }
                Ok(npkts) => {
                    if i == 0 {
                        socket.set_nonblocking(true)?;
                    }
                    trace!("got {} packets", npkts);
                    i += npkts;
                    if npkts != NUM_RCVMMSGS || i >= 1024 {
                        break;
                    }
                }
            }
        }
        self.packets.truncate(i);
        inc_new_counter_debug!("packets-recv_count", i);
        Ok(i)
    }

    pub fn snd_pkt(&self, socket: &UdpSocket) -> Result<()> {
        for p in &self.packets {
            let a = p.meta.addr();
            socket.send_to(&p.data[..p.meta.size], &a)?;
        }
        Ok(())
    }
}

pub fn pkt_chunk<T: Serialize>(xs: &[T], chunks: usize) -> Vec<BndlPkt> {
    let mut out = vec![];
    for x in xs.chunks(chunks) {
        let mut p = BndlPkt::default();
        p.packets.resize(x.len(), Pkt::default());
        for (i, o) in x.iter().zip(p.packets.iter_mut()) {
            let mut wr = io::Cursor::new(&mut o.data[..]);
            bincode::serialize_into(&mut wr, &i).expect("serialize request");
            let len = wr.position() as usize;
            o.meta.size = len;
        }
        out.push(p);
    }
    out
}

pub fn pkt_bndl<T: Serialize>(xs: &[T]) -> Vec<BndlPkt> {
    pkt_chunk(xs, NUM_PACKETS)
}

pub fn blb_from_strm<T: Serialize>(resp: T, rsp_addr: SocketAddr) -> Result<Blob> {
    let mut b = Blob::default();
    let v = bincode::serialize(&resp)?;
    let len = v.len();
    assert!(len <= BLOB_SIZE);
    b.data[..len].copy_from_slice(&v);
    b.meta.size = len;
    b.meta.set_addr(&rsp_addr);
    Ok(b)
}

pub fn blb_bndl<T: Serialize>(rsps: Vec<(T, SocketAddr)>) -> Result<Vec<Blob>> {
    let mut blobs = Vec::new();
    for (resp, rsp_addr) in rsps {
        blobs.push(blb_from_strm(resp, rsp_addr)?);
    }
    Ok(blobs)
}

pub fn arc_blb<T: Serialize>(resp: T, rsp_addr: SocketAddr) -> Result<ArcBlb> {
    let blob = Arc::new(RwLock::new(blb_from_strm(resp, rsp_addr)?));
    Ok(blob)
}

pub fn arc_blb_bndl<T: Serialize>(rsps: Vec<(T, SocketAddr)>) -> Result<ArcBlbBndl> {
    let mut blobs = Vec::new();
    for (resp, rsp_addr) in rsps {
        blobs.push(arc_blb(resp, rsp_addr)?);
    }
    Ok(blobs)
}

pub fn packets_to_blobs<T: Borrow<Pkt>>(packets: &[T]) -> Vec<Blob> {
    let mut current_index = 0;
    let mut blobs = vec![];
    while current_index < packets.len() {
        let mut blob = Blob::default();
        current_index += blob.store_packets(&packets[current_index..]) as usize;
        blobs.push(blob);
    }

    blobs
}

pub fn de_pkts_in_blb(
    data: &[u8],
    serialized_packet_size: usize,
    serialized_meta_size: usize,
) -> Result<Vec<Pkt>> {
    let mut packets: Vec<Pkt> = Vec::with_capacity(data.len() / serialized_packet_size);
    let mut pos = 0;
    while pos + serialized_packet_size <= data.len() {
        let packet = de_singleton_pkt(
            &data[pos..pos + serialized_packet_size],
            serialized_meta_size,
        )?;
        pos += serialized_packet_size;
        packets.push(packet);
    }
    Ok(packets)
}

fn de_singleton_pkt(data: &[u8], serialized_meta_size: usize) -> Result<Pkt> {
    let meta = bincode::deserialize(&data[..serialized_meta_size])?;
    let mut packet_data = [0; PACKET_DATA_SIZE];
    packet_data
        .copy_from_slice(&data[serialized_meta_size..serialized_meta_size + PACKET_DATA_SIZE]);
    Ok(Pkt::new(packet_data, meta))
}






impl Blob {
    pub fn new(data: &[u8]) -> Self {
        let mut blob = Self::default();

        assert!(data.len() <= blob.data.len());

        let data_len = cmp::min(data.len(), blob.data.len());

        let bytes = &data[..data_len];
        blob.data[..data_len].copy_from_slice(bytes);
        blob.meta.size = blob.data_size() as usize;
        blob
    }

    pub fn from_serializable<T: Serialize + ?Sized>(data: &T) -> Self {
        let mut blob = Self::default();
        let pos = {
            let mut out = Cursor::new(blob.data_mut());
            bincode::serialize_into(&mut out, data).expect("failed to serialize output");
            out.position() as usize
        };
        blob.set_size(pos);
        blob
    }

    pub fn parent(&self) -> u64 {
        LittleEndian::read_u64(&self.data[PARENT_RANGE])
    }
    pub fn set_parent(&mut self, ix: u64) {
        LittleEndian::write_u64(&mut self.data[PARENT_RANGE], ix);
    }
    pub fn slot(&self) -> u64 {
        LittleEndian::read_u64(&self.data[SLOT_RANGE])
    }
    pub fn set_slot(&mut self, ix: u64) {
        LittleEndian::write_u64(&mut self.data[SLOT_RANGE], ix);
    }
    pub fn index(&self) -> u64 {
        LittleEndian::read_u64(&self.data[INDEX_RANGE])
    }
    pub fn set_index(&mut self, ix: u64) {
        LittleEndian::write_u64(&mut self.data[INDEX_RANGE], ix);
    }

    pub fn id(&self) -> BvmAddr {
        BvmAddr::new(&self.data[ID_RANGE])
    }

    pub fn set_id(&mut self, id: &BvmAddr) {
        self.data[ID_RANGE].copy_from_slice(id.as_ref())
    }

    pub fn should_forward(&self) -> bool {
        self.data[FORWARDED_RANGE][0] & 0x1 == 0
    }

    pub fn set_forwarded(&mut self, forward: bool) {
        self.data[FORWARDED_RANGE][0] = u8::from(forward)
    }

    pub fn set_genesis_transaction_seal(&mut self, transaction_seal: &Hash) {
        self.data[GENESIS_RANGE].copy_from_slice(transaction_seal.as_ref())
    }

    pub fn genesis_transaction_seal(&self) -> Hash {
        Hash::new(&self.data[GENESIS_RANGE])
    }

    pub fn flags(&self) -> u32 {
        LittleEndian::read_u32(&self.data[FLAGS_RANGE])
    }
    pub fn set_flags(&mut self, ix: u32) {
        LittleEndian::write_u32(&mut self.data[FLAGS_RANGE], ix);
    }

    pub fn is_coding(&self) -> bool {
        (self.flags() & BLOB_FLAG_IS_CODING) != 0
    }

    pub fn set_coding(&mut self) {
        let flags = self.flags();
        self.set_flags(flags | BLOB_FLAG_IS_CODING);
    }

    pub fn set_is_last_in_slot(&mut self) {
        let flags = self.flags();
        self.set_flags(flags | BLOB_FLAG_IS_LAST_IN_SLOT);
    }

    pub fn is_last_in_slot(&self) -> bool {
        (self.flags() & BLOB_FLAG_IS_LAST_IN_SLOT) != 0
    }

    pub fn data_size(&self) -> u64 {
        LittleEndian::read_u64(&self.data[SIZE_RANGE])
    }

    pub fn set_data_size(&mut self, size: u64) {
        LittleEndian::write_u64(&mut self.data[SIZE_RANGE], size);
    }

    pub fn data(&self) -> &[u8] {
        &self.data[BLOB_HEADER_SIZE..]
    }
    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data[BLOB_HEADER_SIZE..]
    }
    pub fn size(&self) -> usize {
        let size = self.data_size() as usize;

        if size > BLOB_HEADER_SIZE && size == self.meta.size {
            size - BLOB_HEADER_SIZE
        } else {
            0
        }
    }

    pub fn set_size(&mut self, size: usize) {
        let new_size = size + BLOB_HEADER_SIZE;
        self.meta.size = new_size;
        self.set_data_size(new_size as u64);
    }

    pub fn store_packets<T: Borrow<Pkt>>(&mut self, packets: &[T]) -> u64 {
        let size = self.size();
        let mut cursor = Cursor::new(&mut self.data_mut()[size..]);
        let mut written = 0;
        let mut last_index = 0;
        for packet in packets {
            if bincode::serialize_into(&mut cursor, &packet.borrow().meta).is_err() {
                break;
            }
            if cursor.write_all(&packet.borrow().data[..]).is_err() {
                break;
            }

            written = cursor.position() as usize;
            last_index += 1;
        }

        self.set_size(size + written);
        last_index
    }

    pub fn recv_blob(socket: &UdpSocket, r: &ArcBlb) -> io::Result<()> {
        let mut p = r.write().unwrap();
        trace!("receiving on {}", socket.local_addr().unwrap());

        let (nrecv, from) = socket.recv_from(&mut p.data)?;
        p.meta.size = nrecv;
        p.meta.set_addr(&from);
        trace!("got {} bytes from {}", nrecv, from);
        Ok(())
    }

    pub fn listen_to(socket: &UdpSocket) -> Result<ArcBlbBndl> {
        let mut v = Vec::new();
        socket.set_nonblocking(false)?;
        for i in 0..NUM_BLOBS {
            let r = ArcBlb::default();

            match Blob::recv_blob(socket, &r) {
                Err(_) if i > 0 => {
                    trace!("got {:?} messages on {}", i, socket.local_addr().unwrap());
                    break;
                }
                Err(e) => {
                    if e.kind() != io::ErrorKind::WouldBlock {
                        println!("{}",
                            printLn(
                                format!("listen_to err {:?}", e).to_string(),
                                module_path!().to_string()
                            )
                        );
                    }
                    return Err(Error::IO(e));
                }
                Ok(()) => {
                    if i == 0 {
                        socket.set_nonblocking(true)?;
                    }
                }
            }
            v.push(r);
        }
        Ok(v)
    }
    pub fn snd_pkt(socket: &UdpSocket, v: ArcBlbBndl) -> Result<()> {
        for r in v {
            {
                let p = r.read().unwrap();
                let a = p.meta.addr();
                if let Err(e) = socket.send_to(&p.data[..p.meta.size], &a) {

                    println!(
                        "{}",
                        Warn(
                            format!("error sending {} byte packet to {:?}: {:?}",
                                p.meta.size, a, e).to_string(),
                            module_path!().to_string()
                        )
                    );
                    Err(e)?;
                }
            }
        }
        Ok(())
    }
}

pub fn index_blobs(blobs: &[ArcBlb], id: &BvmAddr, blob_index: u64, slot: u64, parent: u64) {
    index_blobs_with_genesis(blobs, id, &Hash::default(), blob_index, slot, parent)
}

pub fn index_blobs_with_genesis(
    blobs: &[ArcBlb],
    id: &BvmAddr,
    genesis: &Hash,
    mut blob_index: u64,
    slot: u64,
    parent: u64,
) {
    for blob in blobs.iter() {
        let mut blob = blob.write().unwrap();

        blob.set_index(blob_index);
        blob.set_genesis_transaction_seal(genesis);
        blob.set_slot(slot);
        blob.set_parent(parent);
        blob.set_id(id);
        blob_index += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode;
    use rand::Rng;
    use morgan_interface::hash::Hash;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::sys_controller;
    use std::io;
    use std::io::Write;
    use std::net::{SocketAddr, UdpSocket};

    #[test]
    fn test_packets_set_addr() {
        let send_addr = socketaddr!([127, 0, 0, 1], 123);
        let packets = vec![Pkt::default()];
        let mut msgs = BndlPkt { packets };
        msgs.set_addr(&send_addr);
        assert_eq!(SocketAddr::from(msgs.packets[0].meta.addr()), send_addr);
    }

    #[test]
    pub fn packet_send_recv() {
        morgan_logger::setup();
        let recv_socket = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = recv_socket.local_addr().unwrap();
        let send_socket = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let saddr = send_socket.local_addr().unwrap();
        let mut p = BndlPkt::default();

        p.packets.resize(10, Pkt::default());

        for m in p.packets.iter_mut() {
            m.meta.set_addr(&addr);
            m.meta.size = PACKET_DATA_SIZE;
        }
        p.snd_pkt(&send_socket).unwrap();

        let recvd = p.listen_to(&recv_socket).unwrap();

        assert_eq!(recvd, p.packets.len());

        for m in p.packets {
            assert_eq!(m.meta.size, PACKET_DATA_SIZE);
            assert_eq!(m.meta.addr(), saddr);
        }
    }

    #[test]
    fn test_to_packets() {
        let keypair = Keypair::new();
        let hash = Hash::new(&[1; 32]);
        let tx = sys_controller::create_user_account(&keypair, &keypair.pubkey(), 1, hash);
        let rv = pkt_bndl(&vec![tx.clone(); 1]);
        assert_eq!(rv.len(), 1);
        assert_eq!(rv[0].packets.len(), 1);

        let rv = pkt_bndl(&vec![tx.clone(); NUM_PACKETS]);
        assert_eq!(rv.len(), 1);
        assert_eq!(rv[0].packets.len(), NUM_PACKETS);

        let rv = pkt_bndl(&vec![tx.clone(); NUM_PACKETS + 1]);
        assert_eq!(rv.len(), 2);
        assert_eq!(rv[0].packets.len(), NUM_PACKETS);
        assert_eq!(rv[1].packets.len(), 1);
    }

    #[test]
    pub fn blob_send_recv() {
        trace!("start");
        let reader = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = reader.local_addr().unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let p = ArcBlb::default();
        p.write().unwrap().meta.set_addr(&addr);
        p.write().unwrap().meta.size = 1024;
        let v = vec![p];
        Blob::snd_pkt(&sender, v).unwrap();
        trace!("snd_pkt");
        let rv = Blob::listen_to(&reader).unwrap();
        trace!("listen_to");
        assert_eq!(rv.len(), 1);
        assert_eq!(rv[0].read().unwrap().meta.size, 1024);
    }

    #[cfg(all(feature = "ipv6", test))]
    #[test]
    pub fn blob_ipv6_send_recv() {
        let reader = UdpSocket::bind("[::1]:0").expect("bind");
        let addr = reader.local_addr().unwrap();
        let sender = UdpSocket::bind("[::1]:0").expect("bind");
        let p = ArcBlb::default();
        p.as_mut().unwrap().meta.set_addr(&addr);
        p.as_mut().unwrap().meta.size = 1024;
        let mut v = VecDeque::default();
        v.push_back(p);
        Blob::snd_pkt(&r, &sender, &mut v).unwrap();
        let mut rv = Blob::listen_to(&reader).unwrap();
        let rp = rv.pop_front().unwrap();
        assert_eq!(rp.as_mut().meta.size, 1024);
    }

    #[test]
    pub fn debug_trait() {
        write!(io::sink(), "{:?}", Pkt::default()).unwrap();
        write!(io::sink(), "{:?}", BndlPkt::default()).unwrap();
        write!(io::sink(), "{:?}", Blob::default()).unwrap();
    }
    #[test]
    pub fn blob_test() {
        let mut b = Blob::default();
        b.set_index(<u64>::max_value());
        assert_eq!(b.index(), <u64>::max_value());
        b.data_mut()[0] = 1;
        assert_eq!(b.data()[0], 1);
        assert_eq!(b.index(), <u64>::max_value());
        assert_eq!(b.meta, PktMeta::default());
    }
    #[test]
    fn test_blob_forward() {
        let mut b = Blob::default();
        assert!(b.should_forward());
        b.set_forwarded(true);
        assert!(!b.should_forward());
    }

    #[test]
    fn test_store_blobs_max() {
        let meta = PktMeta::default();
        let serialized_meta_size = bincode::serialized_size(&meta).unwrap() as usize;
        let serialized_packet_size = serialized_meta_size + PACKET_DATA_SIZE;
        let num_packets = (BLOB_SIZE - BLOB_HEADER_SIZE) / serialized_packet_size + 1;
        let mut blob = Blob::default();
        let packets: Vec<_> = (0..num_packets).map(|_| Pkt::default()).collect();
        assert_eq!(blob.store_packets(&packets[..]), (num_packets - 1) as u64);
        blob = Blob::default();
        assert_eq!(
            blob.store_packets(&packets[..num_packets - 2]),
            (num_packets - 2) as u64
        );
        assert_eq!(blob.store_packets(&packets[..num_packets - 2]), 1);
        assert_eq!(blob.store_packets(&packets), 0);
    }

    #[test]
    fn test_packets_to_blobs() {
        let mut rng = rand::thread_rng();
        let meta = PktMeta::default();
        let serialized_meta_size = bincode::serialized_size(&meta).unwrap() as usize;
        let serialized_packet_size = serialized_meta_size + PACKET_DATA_SIZE;
        let packets_per_blob = (BLOB_SIZE - BLOB_HEADER_SIZE) / serialized_packet_size;
        assert!(packets_per_blob > 1);
        let num_packets = packets_per_blob * 10 + packets_per_blob - 1;
        let packets: Vec<_> = (0..num_packets)
            .map(|_| {
                let mut packet = Pkt::default();
                for i in 0..packet.meta.addr.len() {
                    packet.meta.addr[i] = rng.gen_range(1, std::u16::MAX);
                }
                for i in 0..packet.data.len() {
                    packet.data[i] = rng.gen_range(1, std::u8::MAX);
                }
                packet
            })
            .collect();
        let blobs = packets_to_blobs(&packets[..]);
        assert_eq!(blobs.len(), 11);

        let reconstructed_packets: Vec<Pkt> = blobs
            .iter()
            .flat_map(|b| {
                de_pkts_in_blb(
                    &b.data()[..b.size()],
                    serialized_packet_size,
                    serialized_meta_size,
                )
                .unwrap()
            })
            .collect();

        assert_eq!(reconstructed_packets, packets);
    }

    #[test]
    fn test_deserialize_packets_in_blob() {
        let meta = PktMeta::default();
        let serialized_meta_size = bincode::serialized_size(&meta).unwrap() as usize;
        let serialized_packet_size = serialized_meta_size + PACKET_DATA_SIZE;
        let num_packets = 10;
        let mut rng = rand::thread_rng();
        let packets: Vec<_> = (0..num_packets)
            .map(|_| {
                let mut packet = Pkt::default();
                for i in 0..packet.meta.addr.len() {
                    packet.meta.addr[i] = rng.gen_range(1, std::u16::MAX);
                }
                for i in 0..packet.data.len() {
                    packet.data[i] = rng.gen_range(1, std::u8::MAX);
                }
                packet
            })
            .collect();

        let mut blob = Blob::default();
        assert_eq!(blob.store_packets(&packets[..]), num_packets);
        let result = de_pkts_in_blb(
            &blob.data()[..blob.size()],
            serialized_packet_size,
            serialized_meta_size,
        )
        .unwrap();

        assert_eq!(result, packets);
    }

    #[test]
    fn test_blob_data_align() {
        assert_eq!(std::mem::align_of::<BlobContext>(), BLOB_DATA_ALIGN);
    }

    #[test]
    fn test_packet_partial_eq() {
        let p1 = Pkt::default();
        let mut p2 = Pkt::default();

        assert!(p1 == p2);
        p2.data[1] = 4;
        assert!(p1 != p2);
    }
    #[test]
    fn test_blob_partial_eq() {
        let p1 = Blob::default();
        let mut p2 = Blob::default();

        assert!(p1 == p2);
        p2.data[1] = 4;
        assert!(p1 != p2);
    }

    #[test]
    fn test_blob_genesis_transaction_seal() {
        let mut blob = Blob::default();
        assert_eq!(blob.genesis_transaction_seal(), Hash::default());

        let hash = Hash::new(&BvmAddr::new_rand().as_ref());
        blob.set_genesis_transaction_seal(&hash);
        assert_eq!(blob.genesis_transaction_seal(), hash);
    }

}
