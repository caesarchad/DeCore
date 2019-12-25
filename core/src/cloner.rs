use crate::fetch_spot_phase::BlobFetchPhase;
use crate::block_buffer_pool::BlockBufferPool;
#[cfg(feature = "chacha")]
use crate::chacha::{chacha_cbc_encrypt_ledger, CHACHA_BLOCK_SIZE};
use crate::node_group_info::{NodeGroupInfo, Node};
use crate::connection_info::ContactInfo;
use crate::gossip_service::GossipService;
use crate::packet::to_shared_blob;
use crate::fix_missing_spot_service::{FixSlotLength, FixPlan};
use crate::result::Result;
use crate::service::Service;
use crate::streamer::{receiver, responder};
use crate::spot_transmit_service::SpotTransmitService;
use bincode::deserialize;
use rand::thread_rng;
use rand::Rng;
use morgan_client::rpc_client::RpcClient;
use morgan_client::rpc_request::RpcRequest;
use morgan_client::slim_account_host::SlimAccountHost;
use solana_ed25519_dalek as ed25519_dalek;
use morgan_runtime::treasury::Treasury;
use morgan_interface::account_host::{OfflineAccount, OnlineAccount};
use morgan_interface::genesis_block::GenesisBlock;
use morgan_interface::hash::{Hash, Hasher};
use morgan_interface::message::Message;
use morgan_interface::signature::{Keypair, KeypairUtil, Signature};
use morgan_interface::timing::timestamp;
use morgan_interface::transaction::Transaction;
use morgan_interface::transport::TransportError;
use morgan_storage_api::{get_segment_from_slot, storage_opcode, SLOTS_PER_SEGMENT};
use std::fs::File;
use std::io::{self, BufReader, Error, ErrorKind, Read, Seek, SeekFrom};
use std::mem::size_of;
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, RwLock};
use std::thread::{sleep, spawn, JoinHandle};
use std::time::Duration;
use morgan_helper::logHelper::*;

#[derive(Serialize, Deserialize)]
pub enum StorageMinerRequest {
    GetSlotHeight(SocketAddr),
}

pub struct StorageMiner {
    gossip_service: GossipService,
    fetch_phase: BlobFetchPhase,
    spot_service: SpotTransmitService,
    thread_handles: Vec<JoinHandle<()>>,
    exit: Arc<AtomicBool>,
    slot: u64,
    ledger_path: String,
    keypair: Arc<Keypair>,
    storage_keypair: Arc<Keypair>,
    signature: ed25519_dalek::Signature,
    node_group_info: Arc<RwLock<NodeGroupInfo>>,
    ledger_data_file_encrypted: PathBuf,
    sampling_offsets: Vec<u64>,
    hash: Hash,
    #[cfg(feature = "chacha")]
    num_chacha_blocks: usize,
    #[cfg(feature = "chacha")]
    block_buffer_pool: Arc<BlockBufferPool>,
}

pub(crate) fn sample_file(in_path: &Path, sample_offsets: &[u64]) -> io::Result<Hash> {
    let in_file = File::open(in_path)?;
    let metadata = in_file.metadata()?;
    let mut buffer_file = BufReader::new(in_file);

    let mut hasher = Hasher::default();
    let sample_size = size_of::<Hash>();
    let sample_size64 = sample_size as u64;
    let mut buf = vec![0; sample_size];

    let file_len = metadata.len();
    if file_len < sample_size64 {
        return Err(Error::new(ErrorKind::Other, "file too short!"));
    }
    for offset in sample_offsets {
        if *offset > (file_len - sample_size64) / sample_size64 {
            return Err(Error::new(ErrorKind::Other, "offset too large"));
        }
        buffer_file.seek(SeekFrom::Start(*offset * sample_size64))?;
        trace!("sampling @ {} ", *offset);
        match buffer_file.read(&mut buf) {
            Ok(size) => {
                assert_eq!(size, buf.len());
                hasher.hash(&buf);
            }
            Err(e) => {
                // warn!("Error sampling file");
                println!(
                    "{}",
                    Warn(
                        format!("Error sampling file").to_string(),
                        module_path!().to_string()
                    )
                );
                return Err(e);
            }
        }
    }

    Ok(hasher.result())
}

fn get_slot_from_transaction_seal(signature: &ed25519_dalek::Signature, storage_slot: u64) -> u64 {
    let signature_vec = signature.to_bytes();
    let mut segment_index = u64::from(signature_vec[0])
        | (u64::from(signature_vec[1]) << 8)
        | (u64::from(signature_vec[1]) << 16)
        | (u64::from(signature_vec[2]) << 24);
    let max_segment_index = get_segment_from_slot(storage_slot);
    segment_index %= max_segment_index as u64;
    segment_index * SLOTS_PER_SEGMENT
}

fn create_request_processor(
    socket: UdpSocket,
    exit: &Arc<AtomicBool>,
    slot: u64,
) -> Vec<JoinHandle<()>> {
    let mut thread_handles = vec![];
    let (s_reader, r_reader) = channel();
    let (s_responder, r_responder) = channel();
    let storage_socket = Arc::new(socket);
    let t_receiver = receiver(storage_socket.clone(), exit, s_reader);
    thread_handles.push(t_receiver);

    let t_responder = responder("storage-miner-responder", storage_socket.clone(), r_responder);
    thread_handles.push(t_responder);

    let exit = exit.clone();
    let t_processor = spawn(move || loop {
        let packets = r_reader.recv_timeout(Duration::from_secs(1));
        if let Ok(packets) = packets {
            for packet in &packets.packets {
                let req: result::Result<StorageMinerRequest, Box<bincode::ErrorKind>> =
                    deserialize(&packet.data[..packet.meta.size]);
                match req {
                    Ok(StorageMinerRequest::GetSlotHeight(from)) => {
                        if let Ok(blob) = to_shared_blob(slot, from) {
                            let _ = s_responder.send(vec![blob]);
                        }
                    }
                    Err(e) => {
                        // info!("{}", Info(format!("invalid request: {:?}", e).to_string()));
                        println!("{}",
                            printLn(
                                format!("invalid request: {:?}", e).to_string(),
                                module_path!().to_string()
                            )
                        );
                    }
                }
            }
        }
        if exit.load(Ordering::Relaxed) {
            break;
        }
    });
    thread_handles.push(t_processor);
    thread_handles
}

impl StorageMiner {
    /// Returns a Result that contains a storage-miner on success
    ///
    /// # Arguments
    /// * `ledger_path` - path to where the ledger will be stored.
    /// Causes panic if none
    /// * `node` - The storage-miner node
    /// * `node_group_entrypoint` - ContactInfo representing an entry into the network
    /// * `keypair` - Keypair for this storage-miner
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        ledger_path: &str,
        node: Node,
        node_group_entrypoint: ContactInfo,
        keypair: Arc<Keypair>,
        storage_keypair: Arc<Keypair>,
    ) -> Result<Self> {
        let exit = Arc::new(AtomicBool::new(false));

        // info!("{}", Info(format!("StorageMiner: id: {}", keypair.pubkey()).to_string()));
        // info!("{}", Info(format!("Creating cluster info....").to_string()));
        println!("{}",
            printLn(
                format!("StorageMiner: id: {}", keypair.pubkey()).to_string(),
                module_path!().to_string()
            )
        );
        println!("{}",
            printLn(
                format!("Node group information is being composed...").to_string(),
                module_path!().to_string()
            )
        );
        let mut node_group_info = NodeGroupInfo::new(node.info.clone(), keypair.clone());
        node_group_info.set_entrypoint(node_group_entrypoint.clone());
        let node_group_info = Arc::new(RwLock::new(node_group_info));

        // Note for now, this ledger will not contain any of the existing entries
        // in the ledger located at ledger_path, and will only append on newly received
        // entries after being passed to spot_service
        let genesis_block =
            GenesisBlock::load(ledger_path).expect("Expected to successfully open genesis block");
        let treasury = Treasury::new_with_paths(&genesis_block, None);
        let genesis_transaction_seal = treasury.last_transaction_seal();
        let block_buffer_pool = Arc::new(
            BlockBufferPool::open_ledger_file(ledger_path).expect("Expected to be able to open database ledger"),
        );

        let gossip_service = GossipService::new(
            &node_group_info,
            Some(block_buffer_pool.clone()),
            None,
            node.sockets.gossip,
            &exit,
        );

        // info!("{}", Info(format!("Connecting to the cluster via {:?}", node_group_entrypoint).to_string()));
        println!("{}",
            printLn(
                format!("Connecting to the node group on connection url {:?}", node_group_entrypoint).to_string(),
                module_path!().to_string()
            )
        );
        let (nodes, _) = crate::gossip_service::find_node_group_host(&node_group_entrypoint.gossip, 1)?;
        let client = crate::gossip_service::get_client(&nodes);

        let (storage_transaction_seal, storage_slot) = Self::poll_for_transaction_seal_and_slot(&node_group_info)?;

        let signature = storage_keypair.sign(storage_transaction_seal.as_ref());
        let slot = get_slot_from_transaction_seal(&signature, storage_slot);
        // info!("{}", Info(format!("replicating slot: {}", slot).to_string()));
        println!("{}",
            printLn(
                format!("replicating slot: {}", slot).to_string(),
                module_path!().to_string()
            )
        );
        let mut repair_slot_range = FixSlotLength::default();
        repair_slot_range.end = slot + SLOTS_PER_SEGMENT;
        repair_slot_range.start = slot;

        let fix_socket = Arc::new(node.sockets.repair);
        let mut blob_sockets: Vec<Arc<UdpSocket>> =
            node.sockets.blaze_unit.into_iter().map(Arc::new).collect();
        blob_sockets.push(fix_socket.clone());
        let (blob_fetch_sender, blob_fetch_receiver) = channel();
        let fetch_phase = BlobFetchPhase::new_multi_socket(blob_sockets, &blob_fetch_sender, &exit);

        let (retransmit_sender, retransmit_receiver) = channel();

        let spot_service = SpotTransmitService::new(
            block_buffer_pool.clone(),
            node_group_info.clone(),
            blob_fetch_receiver,
            retransmit_sender,
            fix_socket,
            &exit,
            FixPlan::FixSlotList(repair_slot_range),
            &genesis_transaction_seal,
            |_, _, _| true,
        );

        Self::setup_mining_account(&client, &keypair, &storage_keypair)?;
        let mut thread_handles =
            create_request_processor(node.sockets.storage.unwrap(), &exit, slot);

        // receive blobs from retransmit and drop them.
        let t_retransmit = {
            let exit = exit.clone();
            spawn(move || loop {
                let _ = retransmit_receiver.recv_timeout(Duration::from_secs(1));
                if exit.load(Ordering::Relaxed) {
                    break;
                }
            })
        };
        thread_handles.push(t_retransmit);

        let t_replicate = {
            let exit = exit.clone();
            let block_buffer_pool = block_buffer_pool.clone();
            let node_group_info = node_group_info.clone();
            let node_info = node.info.clone();
            spawn(move || {
                Self::wait_for_ledger_download(slot, &block_buffer_pool, &exit, &node_info, node_group_info)
            })
        };
        //always push this last
        thread_handles.push(t_replicate);

        Ok(Self {
            gossip_service,
            fetch_phase,
            spot_service,
            thread_handles,
            exit,
            slot,
            ledger_path: ledger_path.to_string(),
            keypair,
            storage_keypair,
            signature,
            node_group_info,
            ledger_data_file_encrypted: PathBuf::default(),
            sampling_offsets: vec![],
            hash: Hash::default(),
            #[cfg(feature = "chacha")]
            num_chacha_blocks: 0,
            #[cfg(feature = "chacha")]
            block_buffer_pool,
        })
    }

    pub fn run(&mut self) {
        // info!("{}", Info(format!("waiting for ledger download").to_string()));
        println!("{}",
            printLn(
                format!("waiting for ledger download").to_string(),
                module_path!().to_string()
            )
        );
        self.thread_handles.pop().unwrap().join().unwrap();
        self.encrypt_ledger()
            .expect("ledger encrypt not successful");
        loop {
            self.create_sampling_offsets();
            if let Err(err) = self.sample_file_to_create_mining_hash() {
                // warn!("Error sampling file, exiting: {:?}", err);
                println!(
                    "{}",
                    Warn(
                        format!("Error sampling file, exiting: {:?}", err).to_string(),
                        module_path!().to_string()
                    )
                );
                break;
            }
            self.submit_mining_proof();
            // TODO: Miners should be submitting proofs as fast as possible
            sleep(Duration::from_secs(2));
        }
    }

    fn wait_for_ledger_download(
        start_slot: u64,
        block_buffer_pool: &Arc<BlockBufferPool>,
        exit: &Arc<AtomicBool>,
        node_info: &ContactInfo,
        node_group_info: Arc<RwLock<NodeGroupInfo>>,
    ) {
        // info!(
        //     "{}",
        //     Info(format!("window created, waiting for ledger download starting at slot {:?}",
        //     start_slot).to_string())
        // );
        println!("{}",
            printLn(
                format!("window created, waiting for ledger download starting at slot {:?}",
                    start_slot
                ).to_string(),
                module_path!().to_string()
            )
        );
        let mut current_slot = start_slot;
        'outer: loop {
            while let Ok(meta) = block_buffer_pool.meta(current_slot) {
                if let Some(meta) = meta {
                    if meta.is_full() {
                        current_slot += 1;
                        // info!("{}", Info(format!("current slot: {}", current_slot).to_string()));
                        println!("{}",
                            printLn(
                                format!("current slot: {}", current_slot).to_string(),
                                module_path!().to_string()
                            )
                        );
                        if current_slot >= start_slot + SLOTS_PER_SEGMENT {
                            break 'outer;
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            if exit.load(Ordering::Relaxed) {
                break;
            }
            sleep(Duration::from_secs(1));
        }

        // info!("{}", Info(format!("Done receiving entries from spot_service").to_string()));
        println!("{}",
            printLn(
                format!("Done receiving entries from spot_service").to_string(),
                module_path!().to_string()
            )
        );
        // Remove storage-miner from the data plane
        let mut contact_info = node_info.clone();
        contact_info.blaze_unit = "0.0.0.0:0".parse().unwrap();
        contact_info.wallclock = timestamp();
        {
            let mut node_group_info_w = node_group_info.write().unwrap();
            node_group_info_w.insert_self(contact_info);
        }
    }

    fn encrypt_ledger(&mut self) -> Result<()> {
        let ledger_path = Path::new(&self.ledger_path);
        self.ledger_data_file_encrypted = ledger_path.join("ledger.enc");

        #[cfg(feature = "chacha")]
        {
            let mut ivec = [0u8; 64];
            ivec.copy_from_slice(&self.signature.to_bytes());

            let num_encrypted_bytes = chacha_cbc_encrypt_ledger(
                &self.block_buffer_pool,
                self.slot,
                &self.ledger_data_file_encrypted,
                &mut ivec,
            )?;

            self.num_chacha_blocks = num_encrypted_bytes / CHACHA_BLOCK_SIZE;
        }

        // info!(
        //     "{}", Info(format!("Done encrypting the ledger: {:?}",
        //     self.ledger_data_file_encrypted).to_string())
        // );
        println!("{}",
            printLn(
                format!("Done encrypting the ledger: {:?}",
                    self.ledger_data_file_encrypted
                ).to_string(),
                module_path!().to_string()
            )
        );
        Ok(())
    }

    fn create_sampling_offsets(&mut self) {
        self.sampling_offsets.clear();

        #[cfg(not(feature = "chacha"))]
        self.sampling_offsets.push(0);

        #[cfg(feature = "chacha")]
        {
            use crate::storage_stage::NUM_STORAGE_SAMPLES;
            use rand::SeedableRng;
            use rand_chacha::ChaChaRng;

            let mut rng_seed = [0u8; 32];
            rng_seed.copy_from_slice(&self.signature.to_bytes()[0..32]);
            let mut rng = ChaChaRng::from_seed(rng_seed);
            for _ in 0..NUM_STORAGE_SAMPLES {
                self.sampling_offsets
                    .push(rng.gen_range(0, self.num_chacha_blocks) as u64);
            }
        }
    }

    fn sample_file_to_create_mining_hash(&mut self) -> Result<()> {
        self.hash = sample_file(&self.ledger_data_file_encrypted, &self.sampling_offsets)?;
        // info!("{}", Info(format!("sampled hash: {}", self.hash).to_string()));
        println!("{}",
            printLn(
                format!("sampled hash: {}", self.hash).to_string(),
                module_path!().to_string()
            )
        );
        Ok(())
    }

    fn setup_mining_account(
        client: &SlimAccountHost,
        keypair: &Keypair,
        storage_keypair: &Keypair,
    ) -> Result<()> {
        // make sure storage-miner has some balance
        if client.poll_get_balance(&keypair.pubkey())? == 0 {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "keypair account has no balance",
            ))?
        }

        // check if the storage account exists
        let balance = client.poll_get_balance(&storage_keypair.pubkey());
        if balance.is_err() || balance.unwrap() == 0 {
            let (transaction_seal, _fee_calculator) = client.get_recent_transaction_seal().expect("transaction_seal");

            let ix = storage_opcode::create_miner_storage_account(
                &keypair.pubkey(),
                &storage_keypair.pubkey(),
                1,
            );
            let tx = Transaction::new_s_opcodes(&[keypair], ix, transaction_seal);
            let signature = client.send_offline_transaction(tx)?;
            client
                .poll_for_signature(&signature)
                .map_err(|err| match err {
                    TransportError::IoError(e) => e,
                    TransportError::TransactionError(_) => io::Error::new(
                        ErrorKind::Other,
                        "setup_mining_account: signature not found",
                    ),
                })?;
        }
        Ok(())
    }

    fn submit_mining_proof(&self) {
        // No point if we've got no storage account...
        let nodes = self.node_group_info.read().unwrap().fetch_blaze_node_list();
        let client = crate::gossip_service::get_client(&nodes);
        assert!(
            client
                .poll_get_balance(&self.storage_keypair.pubkey())
                .unwrap()
                > 0
        );
        // ...or no difs for fees
        assert!(client.poll_get_balance(&self.keypair.pubkey()).unwrap() > 0);

        let (transaction_seal, _) = client.get_recent_transaction_seal().expect("No recent transaction_seal");
        let instruction = storage_opcode::mining_proof(
            &self.storage_keypair.pubkey(),
            self.hash,
            self.slot,
            Signature::new(&self.signature.to_bytes()),
        );
        let message = Message::new_with_payer(vec![instruction], Some(&self.keypair.pubkey()));
        let mut transaction = Transaction::new(
            &[self.keypair.as_ref(), self.storage_keypair.as_ref()],
            message,
            transaction_seal,
        );
        client
            .send_and_confirm_transaction(
                &[&self.keypair, &self.storage_keypair],
                &mut transaction,
                10,
                0,
            )
            .expect("transfer didn't work!");
    }

    pub fn close(self) {
        self.exit.store(true, Ordering::Relaxed);
        self.join()
    }

    pub fn join(self) {
        self.gossip_service.join().unwrap();
        self.fetch_phase.join().unwrap();
        self.spot_service.join().unwrap();
        for handle in self.thread_handles {
            handle.join().unwrap();
        }
    }

    fn poll_for_transaction_seal_and_slot(
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
    ) -> Result<(String, u64)> {
        for _ in 0..10 {
            let rpc_client = {
                let node_group_info = node_group_info.read().unwrap();
                let rpc_peers = node_group_info.rpc_peers();
                debug!("rpc peers: {:?}", rpc_peers);
                let node_index = thread_rng().gen_range(0, rpc_peers.len());
                RpcClient::new_socket(rpc_peers[node_index].rpc)
            };
            let storage_transaction_seal = rpc_client
                .retry_make_rpc_request(&RpcRequest::GetStorageTransactionSeal, None, 0)
                .expect("rpc request")
                .to_string();
            let storage_slot = rpc_client
                .retry_make_rpc_request(&RpcRequest::GetStorageSlot, None, 0)
                .expect("rpc request")
                .as_u64()
                .unwrap();
            // info!("{}", Info(format!("storage slot: {}", storage_slot).to_string()));
            println!("{}",
                printLn(
                    format!("storage slot: {}", storage_slot).to_string(),
                    module_path!().to_string()
                )
            );
            if get_segment_from_slot(storage_slot) != 0 {
                return Ok((storage_transaction_seal, storage_slot));
            }
            // info!("{}", Info(format!("waiting for segment...").to_string()));
            println!("{}",
                printLn(
                    format!("waiting for segment...").to_string(),
                    module_path!().to_string()
                )
            );
            sleep(Duration::from_secs(5));
        }
        Err(Error::new(
            ErrorKind::Other,
            "Couldn't get transaction_seal or slot",
        ))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{create_dir_all, remove_file};
    use std::io::Write;

    fn tmp_file_path(name: &str) -> PathBuf {
        use std::env;
        let out_dir = env::var("OUT_DIR").unwrap_or_else(|_| "target".to_string());
        let keypair = Keypair::new();

        let mut path = PathBuf::new();
        path.push(out_dir);
        path.push("tmp");
        create_dir_all(&path).unwrap();

        path.push(format!("{}-{}", name, keypair.pubkey()));
        path
    }

    #[test]
    fn test_sample_file() {
        morgan_logger::setup();
        let in_path = tmp_file_path("test_sample_file_input.txt");
        let num_strings = 4096;
        let string = "12foobar";
        {
            let mut in_file = File::create(&in_path).unwrap();
            for _ in 0..num_strings {
                in_file.write(string.as_bytes()).unwrap();
            }
        }
        let num_samples = (string.len() * num_strings / size_of::<Hash>()) as u64;
        let samples: Vec<_> = (0..num_samples).collect();
        let res = sample_file(&in_path, samples.as_slice());
        let ref_hash: Hash = Hash::new(&[
            173, 251, 182, 165, 10, 54, 33, 150, 133, 226, 106, 150, 99, 192, 179, 1, 230, 144,
            151, 126, 18, 191, 54, 67, 249, 140, 230, 160, 56, 30, 170, 52,
        ]);
        let res = res.unwrap();
        assert_eq!(res, ref_hash);

        // Sample just past the end
        assert!(sample_file(&in_path, &[num_samples]).is_err());
        remove_file(&in_path).unwrap();
    }

    #[test]
    fn test_sample_file_invalid_offset() {
        let in_path = tmp_file_path("test_sample_file_invalid_offset_input.txt");
        {
            let mut in_file = File::create(&in_path).unwrap();
            for _ in 0..4096 {
                in_file.write("123456foobar".as_bytes()).unwrap();
            }
        }
        let samples = [0, 200000];
        let res = sample_file(&in_path, &samples);
        assert!(res.is_err());
        remove_file(in_path).unwrap();
    }

    #[test]
    fn test_sample_file_missing_file() {
        let in_path = tmp_file_path("test_sample_file_that_doesnt_exist.txt");
        let samples = [0, 5];
        let res = sample_file(&in_path, &samples);
        assert!(res.is_err());
    }
}
