//! The `gossip_service` module implements the network control plane.

// use crate::treasury_forks::TreasuryForks;
use crate::treasury_forks::TreasuryForks;
use crate::block_buffer_pool::BlockBufferPool;
use crate::node_group_info::NodeGroupInfo;
use crate::node_group_info::FULLNODE_PORT_RANGE;
use crate::connection_info::ContactInfo;
use crate::service::Service;
use crate::streamer;
use rand::{thread_rng, Rng};
use morgan_client::thin_client::{create_client, ThinClient};
use morgan_interface::pubkey::Pubkey;
use morgan_interface::signature::{Keypair, KeypairUtil};
use std::net::SocketAddr;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use morgan_helper::logHelper::*;
use rand::RngCore;
use std::{
    fs, io,
    path::{Path, PathBuf},
};

pub struct GossipService {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl GossipService {
    pub fn new(
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        block_buffer_pool: Option<Arc<BlockBufferPool>>,
        treasury_forks: Option<Arc<RwLock<TreasuryForks>>>,
        gossip_socket: UdpSocket,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        let (request_sender, request_receiver) = channel();
        let gossip_socket = Arc::new(gossip_socket);
        trace!(
            "GossipService: id: {}, listening on: {:?}",
            &node_group_info.read().unwrap().my_data().id,
            gossip_socket.local_addr().unwrap()
        );
        let t_receiver = streamer::blob_receiver(gossip_socket.clone(), &exit, request_sender);
        let (response_sender, response_receiver) = channel();
        let t_responder = streamer::responder("gossip", gossip_socket, response_receiver);
        let t_listen = NodeGroupInfo::listen(
            node_group_info.clone(),
            block_buffer_pool,
            request_receiver,
            response_sender.clone(),
            exit,
        );
        let t_gossip = NodeGroupInfo::gossip(node_group_info.clone(), treasury_forks, response_sender, exit);
        let thread_hdls = vec![t_receiver, t_responder, t_listen, t_gossip];
        Self { thread_hdls }
    }
}

/// Discover Nodes and miners in a cluster
pub fn find_node_group_host(
    entry_point: &SocketAddr,
    num_nodes: usize,
) -> std::io::Result<(Vec<ContactInfo>, Vec<ContactInfo>)> {
    discover(entry_point, Some(num_nodes), Some(30), None, None)
}

pub fn discover(
    entry_point: &SocketAddr,
    num_nodes: Option<usize>,
    timeout: Option<u64>,
    find_node: Option<Pubkey>,
    gossip_addr: Option<&SocketAddr>,
) -> std::io::Result<(Vec<ContactInfo>, Vec<ContactInfo>)> {
    let exit = Arc::new(AtomicBool::new(false));
    let (gossip_service, spy_ref) = make_gossip_node(entry_point, &exit, gossip_addr);

    let id = spy_ref.read().unwrap().keypair.pubkey();
    // info!("{}", Info(format!("Gossip entry point: {:?}", entry_point).to_string()));
    // info!("{}", Info(format!("Spy node id: {:?}", id).to_string()));
    println!("{}",
        printLn(
            format!("Gossip entry point: {:?}", entry_point).to_string(),
            module_path!().to_string()
        )
    );
    println!("{}",
        printLn(
            format!("Spy node id: {:?}", id).to_string(),
            module_path!().to_string()
        )
    );
    let (met_criteria, secs, tvu_peers, miners) =
        spy(spy_ref.clone(), num_nodes, timeout, find_node);

    exit.store(true, Ordering::Relaxed);
    gossip_service.join().unwrap();

    if met_criteria {
        // info!(
        //     "{}",
        //     Info(format!("discover success in {}s...\n{}",
        //     secs,
        //     spy_ref.read().unwrap().contact_info_trace()).to_string())
        // );
        println!("{}",
            printLn(
                format!("discover success in {}s...\n{}",
                    secs,
                    spy_ref.read().unwrap().contact_info_trace()
                ).to_string(),
                module_path!().to_string()
            )
        );
        return Ok((tvu_peers, miners));
    }

    if !tvu_peers.is_empty() {
        // info!(
        //     "{}",
        //     Info(format!("discover failed to match criteria by timeout...\n{}",
        //     spy_ref.read().unwrap().contact_info_trace()).to_string())
        // );
        println!("{}",
            printLn(
                format!("discover failed to match criteria by timeout...\n{}",
                    spy_ref.read().unwrap().contact_info_trace()
                ).to_string(),
                module_path!().to_string()
            )
        );
        return Ok((tvu_peers, miners));
    }

    // info!(
    //     "{}",
    //     Info(format!("discover failed...\n{}",
    //     spy_ref.read().unwrap().contact_info_trace()).to_string())
    // );
    println!("{}",
        printLn(
            format!("discover failed...\n{}",
                spy_ref.read().unwrap().contact_info_trace()
            ).to_string(),
            module_path!().to_string()
        )
    );
    Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        "Failed to converge",
    ))
}

/// A simple wrapper for creating a temporary directory that is
/// automatically deleted when it's dropped.
///
/// We use this in lieu of tempfile because tempfile brings in too many
/// dependencies.
#[derive(Debug, PartialEq)]
pub struct TempPath {
    path_buf: PathBuf,
    persist: bool,
}

impl Drop for TempPath {
    fn drop(&mut self) {
        if !self.persist {
            fs::remove_dir_all(&self.path_buf)
                .or_else(|_| fs::remove_file(&self.path_buf))
                .unwrap_or(());
        }
    }
}

impl TempPath {
    /// Create new uninitialized temporary path, i.e. a file or directory
    /// isn't created automatically
    pub fn new() -> Self {
        let tmpdir = create_path();
        TempPath {
            path_buf: tmpdir,
            persist: false,
        }
    }

    /// Return the underlying path to this temporary directory.
    pub fn path(&self) -> &Path {
        &self.path_buf
    }

    pub fn persist(&mut self) {
        self.persist = true;
    }

    pub fn create_as_file(&self) -> io::Result<()> {
        let mut builder = fs::OpenOptions::new();
        builder.write(true).create_new(true);

        builder.open(self.path())?;
        Ok(())
    }

    pub fn create_as_dir(&self) -> io::Result<()> {
        let builder = fs::DirBuilder::new();
        builder.create(self.path())?;
        Ok(())
    }
}

fn create_path() -> PathBuf {
    create_path_in_dir(std::env::temp_dir())
}

fn create_path_in_dir(path: PathBuf) -> PathBuf {
    let mut path = path;
    let mut rng = rand::thread_rng();
    let mut bytes = [0_u8; 16];
    rng.fill_bytes(&mut bytes);
    let path_string = hex::encode(&bytes);

    path.push(path_string);
    path
}

impl std::convert::AsRef<Path> for TempPath {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}

/// Creates a ThinClient per valid node
pub fn get_clients(nodes: &[ContactInfo]) -> Vec<ThinClient> {
    nodes
        .iter()
        .filter_map(ContactInfo::valid_client_facing_addr)
        .map(|addrs| create_client(addrs, FULLNODE_PORT_RANGE))
        .collect()
}

/// Creates a ThinClient by selecting a valid node at random
pub fn get_client(nodes: &[ContactInfo]) -> ThinClient {
    let nodes: Vec<_> = nodes
        .iter()
        .filter_map(ContactInfo::valid_client_facing_addr)
        .collect();
    let select = thread_rng().gen_range(0, nodes.len());
    create_client(nodes[select], FULLNODE_PORT_RANGE)
}

fn spy(
    spy_ref: Arc<RwLock<NodeGroupInfo>>,
    num_nodes: Option<usize>,
    timeout: Option<u64>,
    find_node: Option<Pubkey>,
) -> (bool, u64, Vec<ContactInfo>, Vec<ContactInfo>) {
    let now = Instant::now();
    let mut met_criteria = false;
    let mut tvu_peers: Vec<ContactInfo> = Vec::new();
    let mut miners: Vec<ContactInfo> = Vec::new();
    let mut i = 0;
    loop {
        if let Some(secs) = timeout {
            if now.elapsed() >= Duration::from_secs(secs) {
                break;
            }
        }
        // collect tvu peers but filter out miners since their tvu is transient and we do not want
        // it to show up as a "node"
        tvu_peers = spy_ref
            .read()
            .unwrap()
            .tvu_peers()
            .into_iter()
            .filter(|node| !NodeGroupInfo::is_storage_miner(&node))
            .collect::<Vec<_>>();
        miners = spy_ref.read().unwrap().storage_peers();
        if let Some(num) = num_nodes {
            if tvu_peers.len() + miners.len() >= num {
                if let Some(pubkey) = find_node {
                    if tvu_peers
                        .iter()
                        .chain(miners.iter())
                        .any(|x| x.id == pubkey)
                    {
                        met_criteria = true;
                        break;
                    }
                } else {
                    met_criteria = true;
                    break;
                }
            }
        }
        if let Some(pubkey) = find_node {
            if num_nodes.is_none()
                && tvu_peers
                    .iter()
                    .chain(miners.iter())
                    .any(|x| x.id == pubkey)
            {
                met_criteria = true;
                break;
            }
        }
        if i % 20 == 0 {
            // info!(
            //     "{}",
            //     Info(format!("discovering...\n{}",
            //     spy_ref.read().unwrap().contact_info_trace()).to_string())
            // );
            println!("{}",
                printLn(
                    format!("discovering...\n{}",
                        spy_ref.read().unwrap().contact_info_trace())
                    .to_string(),
                    module_path!().to_string()
                )
            );
        }
        sleep(Duration::from_millis(
            crate::node_group_info::GOSSIP_SLEEP_MILLIS,
        ));
        i += 1;
    }
    (
        met_criteria,
        now.elapsed().as_secs(),
        tvu_peers,
        miners,
    )
}

/// Makes a spy or gossip node based on whether or not a gossip_addr was passed in
/// Pass in a gossip addr to fully participate in gossip instead of relying on just pulls
fn make_gossip_node(
    entry_point: &SocketAddr,
    exit: &Arc<AtomicBool>,
    gossip_addr: Option<&SocketAddr>,
) -> (GossipService, Arc<RwLock<NodeGroupInfo>>) {
    let keypair = Arc::new(Keypair::new());
    let (node, gossip_socket) = if let Some(gossip_addr) = gossip_addr {
        NodeGroupInfo::gossip_node(&keypair.pubkey(), gossip_addr)
    } else {
        NodeGroupInfo::spy_node(&keypair.pubkey())
    };
    let mut node_group_info = NodeGroupInfo::new(node, keypair);
    node_group_info.set_entrypoint(ContactInfo::new_gossip_entry_point(entry_point));
    let node_group_info = Arc::new(RwLock::new(node_group_info));
    let gossip_service =
        GossipService::new(&node_group_info.clone(), None, None, gossip_socket, &exit);
    (gossip_service, node_group_info)
}

impl Service for GossipService {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_group_info::{NodeGroupInfo, Node};
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, RwLock};

    #[test]
    #[ignore]
    // test that phase will exit when flag is set
    fn test_exit() {
        let exit = Arc::new(AtomicBool::new(false));
        let tn = Node::new_localhost();
        let node_group_info = NodeGroupInfo::new_with_invalid_keypair(tn.info.clone());
        let c = Arc::new(RwLock::new(node_group_info));
        let d = GossipService::new(&c, None, None, tn.sockets.gossip, &exit);
        exit.store(true, Ordering::Relaxed);
        d.join().unwrap();
    }

    #[test]
    fn test_gossip_services_spy() {
        let keypair = Keypair::new();
        let peer0 = Pubkey::new_rand();
        let peer1 = Pubkey::new_rand();
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        let peer0_info = ContactInfo::new_localhost(&peer0, 0);
        let peer1_info = ContactInfo::new_localhost(&peer1, 0);
        let mut node_group_info = NodeGroupInfo::new(contact_info.clone(), Arc::new(keypair));
        node_group_info.insert_info(peer0_info);
        node_group_info.insert_info(peer1_info);

        let spy_ref = Arc::new(RwLock::new(node_group_info));

        let (met_criteria, secs, tvu_peers, _) = spy(spy_ref.clone(), None, Some(1), None);
        assert_eq!(met_criteria, false);
        assert_eq!(secs, 1);
        assert_eq!(tvu_peers, spy_ref.read().unwrap().tvu_peers());

        // Find num_nodes
        let (met_criteria, _, _, _) = spy(spy_ref.clone(), Some(1), None, None);
        assert_eq!(met_criteria, true);
        let (met_criteria, _, _, _) = spy(spy_ref.clone(), Some(2), None, None);
        assert_eq!(met_criteria, true);

        // Find specific node by pubkey
        let (met_criteria, _, _, _) = spy(spy_ref.clone(), None, None, Some(peer0));
        assert_eq!(met_criteria, true);
        let (met_criteria, _, _, _) = spy(spy_ref.clone(), None, Some(0), Some(Pubkey::new_rand()));
        assert_eq!(met_criteria, false);

        // Find num_nodes *and* specific node by pubkey
        let (met_criteria, _, _, _) = spy(spy_ref.clone(), Some(1), None, Some(peer0));
        assert_eq!(met_criteria, true);
        let (met_criteria, _, _, _) = spy(spy_ref.clone(), Some(3), Some(0), Some(peer0));
        assert_eq!(met_criteria, false);
        let (met_criteria, _, _, _) =
            spy(spy_ref.clone(), Some(1), Some(0), Some(Pubkey::new_rand()));
        assert_eq!(met_criteria, false);
    }
}
