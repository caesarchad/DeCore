//! The `data_filter` module defines a set of services for efficiently pulling data from UDP sockets.
//!

use crate::packet::{
    de_pkts_in_blb, Blob, PktMeta, BndlPkt, 
};
use crate::bvm_types::{
    PcktAcptr,
    PcktSndr,
    BlobSndr,
    BlobAcptr,
};
use morgan_interface::constants::PACKET_DATA_SIZE;
use crate::result::{Error, Result};
use bincode;
use morgan_interface::timing::duration_as_ms;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::thread::{Builder, JoinHandle};
use std::time::{Duration, Instant};
use morgan_helper::logHelper::*;
use walkdir::WalkDir;


fn acpt_window(sock: &UdpSocket, exit: Arc<AtomicBool>, channel: &PcktSndr) -> Result<()> {
    loop {
        let mut msgs = BndlPkt::default();
        loop {
            // Check for exit signal, even if socket is busy
            // (for instance the leader trasaction socket)
            if exit.load(Ordering::Relaxed) {
                return Ok(());
            }
            if let Ok(_len) = msgs.listen_to(sock) {
                channel.send(msgs)?;
                break;
            }
        }
    }
}

pub fn acptor(
    sock: Arc<UdpSocket>,
    exit: &Arc<AtomicBool>,
    packet_sender: PcktSndr,
) -> JoinHandle<()> {
    let res = sock.set_read_timeout(Some(Duration::new(1, 0)));
    if res.is_err() {
        panic!("data_filter::receiver set_read_timeout error");
    }
    let exit = exit.clone();
    Builder::new()
        .name("morgan-receiver".to_string())
        .spawn(move || {
            let _ = acpt_window(&sock, exit, &packet_sender);
        })
        .unwrap()
}

fn forward_msg(sock: &UdpSocket, r: &BlobAcptr) -> Result<()> {
    let timer = Duration::new(1, 0);
    let msgs = r.recv_timeout(timer)?;
    Blob::snd_pkt(sock, msgs)?;
    Ok(())
}

pub fn batch_forward(recvr: &PcktAcptr, max_batch: usize) -> Result<(Vec<BndlPkt>, usize, u64)> {
    let timer = Duration::new(1, 0);
    let msgs = recvr.recv_timeout(timer)?;
    let recv_start = Instant::now();
    trace!("got msgs");
    let mut len = msgs.packets.len();
    let mut batch = vec![msgs];
    while let Ok(more) = recvr.try_recv() {
        trace!("got more msgs");
        len += more.packets.len();
        batch.push(more);

        if len > max_batch {
            break;
        }
    }
    trace!("batch len {}", batch.len());
    Ok((batch, len, duration_as_ms(&recv_start.elapsed())))
}

pub fn handle_forward_srvc(name: &'static str, sock: Arc<UdpSocket>, r: BlobAcptr) -> JoinHandle<()> {
    Builder::new()
        .name(format!("morgan-hfs-{}", name))
        .spawn(move || loop {
            if let Err(e) = forward_msg(&sock, &r) {
                match e {
                    Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => break,
                    Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                    _ => {
                        // warn!("{} responder error: {:?}", name, e),
                        println!(
                            "{}",
                            Warn(
                                format!("{} responder error: {:?}", name, e).to_string(),
                                module_path!().to_string()
                            )
                        );
                    }
                }
            }
        })
        .unwrap()
}

//TODO, we would need to stick block authentication before we create the
//window.
fn blob_catcher(sock: &UdpSocket, s: &BlobSndr) -> Result<()> {
    trace!("blob_catcher is listening to {}", sock.local_addr().unwrap());
    let dq = Blob::listen_to(sock)?;
    if !dq.is_empty() {
        s.send(dq)?;
    }
    Ok(())
}

pub fn acptr_srvc(
    sock: Arc<UdpSocket>,
    exit: &Arc<AtomicBool>,
    s: BlobSndr,
) -> JoinHandle<()> {
    //DOCUMENTED SIDE-EFFECT
    //1 second timeout on socket read
    let timer = Duration::new(1, 0);
    sock.set_read_timeout(Some(timer))
        .expect("set socket timeout");
    let exit = exit.clone();
    Builder::new()
        .name("morgan-blob_catcher".to_string())
        .spawn(move || loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }
            let _ = blob_catcher(&sock, &s);
        })
        .unwrap()
}

use std::path::{Path, PathBuf};

/// Helper function to iterate through all the files in the given directory, skipping hidden files,
/// and return an iterator of their paths.
pub fn traverse_folder(path: &Path) -> impl Iterator<Item = PathBuf> {
    walkdir::WalkDir::new(path)
        .into_iter()
        .map(::std::result::Result::unwrap)
        .filter(|entry| {
            entry.file_type().is_file()
                && entry
                    .file_name()
                    .to_str()
                    .map_or(false, |s| !s.starts_with('.')) // Skip hidden files
        })
        .map(|entry| entry.path().to_path_buf())
}

pub fn derive_test_name(root: &Path, path: &Path, test_name: &str) -> String {
    let relative = path.strip_prefix(root).unwrap_or_else(|_| {
        panic!(
            "failed to strip prefix '{}' from path '{}'",
            root.display(),
            path.display()
        )
    });
    let mut test_name = test_name.to_string();
    test_name.push_str(&format!("::{}", relative.display()));
    test_name
}


fn blob_filter(sock: &UdpSocket, s: &PcktSndr) -> Result<()> {
    trace!(
        "blob_filter: listening to {}",
        sock.local_addr().unwrap()
    );

    let meta = PktMeta::default();
    let se_met_sz = bincode::serialized_size(&meta)? as usize;
    let se_pkt_sz = se_met_sz + PACKET_DATA_SIZE;
    let blobs = Blob::listen_to(sock)?;
    for blob in blobs {
        let r_blob = blob.read().unwrap();
        let data = {
            let msg_size = r_blob.size();
            &r_blob.data()[..msg_size]
        };

        let packets =
            de_pkts_in_blb(data, se_pkt_sz, se_met_sz);

        if packets.is_err() {
            continue;
        }

        let packets = packets?;
        s.send(BndlPkt::new(packets))?;
    }

    Ok(())
}

pub fn blob_filter_window(
    sock: Arc<UdpSocket>,
    exit: &Arc<AtomicBool>,
    s: PcktSndr,
) -> JoinHandle<()> {
    //DOCUMENTED SIDE-EFFECT
    //1 second timeout on socket read
    let timer = Duration::new(1, 0);
    sock.set_read_timeout(Some(timer))
        .expect("set socket timeout");
    let exit = exit.clone();
    Builder::new()
        .name("morgan-blob_filter_window".to_string())
        .spawn(move || loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }
            let _ = blob_filter(&sock, &s);
        })
        .unwrap()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::packet::{Blob, Pkt, BndlPkt, ArcBlb};
    use morgan_interface::constants::PACKET_DATA_SIZE;
    use crate::data_filter::{acptor, handle_forward_srvc};
    use std::io;
    use std::io::Write;
    use std::net::UdpSocket;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::time::Duration;

    fn get_msgs(r: PcktAcptr, num: &mut usize) -> Result<()> {
        for _ in 0..10 {
            let m = r.recv_timeout(Duration::new(1, 0))?;

            *num -= m.packets.len();

            if *num == 0 {
                break;
            }
        }

        Ok(())
    }
    #[test]
    fn streamer_debug() {
        write!(io::sink(), "{:?}", Pkt::default()).unwrap();
        write!(io::sink(), "{:?}", BndlPkt::default()).unwrap();
        write!(io::sink(), "{:?}", Blob::default()).unwrap();
    }
    #[test]
    fn streamer_send_test() {
        let read = UdpSocket::bind("127.0.0.1:0").expect("bind");
        read.set_read_timeout(Some(Duration::new(1, 0))).unwrap();

        let addr = read.local_addr().unwrap();
        let send = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let exit = Arc::new(AtomicBool::new(false));
        let (s_reader, r_reader) = channel();
        let t_receiver = acptor(Arc::new(read), &exit, s_reader);
        let t_responder = {
            let (s_responder, r_responder) = channel();
            let t_responder = handle_forward_srvc("streamer_send_test", Arc::new(send), r_responder);
            let mut msgs = Vec::new();
            for i in 0..5 {
                let b = ArcBlb::default();
                {
                    let mut w = b.write().unwrap();
                    w.data[0] = i as u8;
                    w.meta.size = PACKET_DATA_SIZE;
                    w.meta.set_addr(&addr);
                }
                msgs.push(b);
            }
            s_responder.send(msgs).expect("send");
            t_responder
        };

        let mut num = 5;
        get_msgs(r_reader, &mut num).expect("get_msgs");
        assert_eq!(num, 0);
        exit.store(true, Ordering::Relaxed);
        t_receiver.join().expect("join");
        t_responder.join().expect("join");
    }
}
