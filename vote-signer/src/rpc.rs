//! The `rpc` module implements the Vote signing service RPC interface.

use jsonrpc_core::{Error, MetaIoHandler, Metadata, Result};
use jsonrpc_derive::rpc;
use jsonrpc_http_server::{hyper, AccessControlAllowOrigin, DomainsValidation, ServerBuilder};
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::{Keypair, KeypairUtil, Signature};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, sleep, Builder, JoinHandle};
use std::time::Duration;
use morgan_helper::logHelper::*;

pub struct VoteSignerRpcService {
    thread_hdl: JoinHandle<()>,
}

impl VoteSignerRpcService {
    pub fn new(rpc_addr: SocketAddr, exit: &Arc<AtomicBool>) -> Self {
        let request_processor = LocalVoteSigner::default();
        let exit = exit.clone();
        let thread_hdl = Builder::new()
            .name("morgan-vote-signer-jsonrpc".to_string())
            .spawn(move || {
                let mut io = MetaIoHandler::default();
                let rpc = VoteSignerRpcImpl;
                io.extend_with(rpc.to_delegate());

                let server =
                    ServerBuilder::with_meta_extractor(io, move |_req: &hyper::Request<hyper::Body>| Meta {
                        request_processor: request_processor.clone(),
                    }).threads(4)
                        .cors(DomainsValidation::AllowOnly(vec![
                            AccessControlAllowOrigin::Any,
                        ]))
                        .start_http(&rpc_addr);
                if server.is_err() {
                    // warn!("JSON RPC service unavailable: unable to bind to RPC port {}. \nMake sure this port is not already in use by another application", rpc_addr.port());
                    println!(
                        "{}",
                        Warn(
                            format!("JSON RPC service unavailable: unable to bind to RPC port {}. \nMake sure this port is not already in use by another application", rpc_addr.port()).to_string(),
                            module_path!().to_string())
                    );
                    return;
                }
                while !exit.load(Ordering::Relaxed) {
                    sleep(Duration::from_millis(100));
                }
                server.unwrap().close();
            })
            .unwrap();
        Self { thread_hdl }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[derive(Clone)]
pub struct Meta {
    pub request_processor: LocalVoteSigner,
}
impl Metadata for Meta {}

#[rpc(server)]
pub trait VoteSignerRpc {
    type Metadata;

    #[rpc(meta, name = "registerNode")]
    fn register(&self, _: Self::Metadata, _: BvmAddr, _: Signature, _: Vec<u8>) -> Result<BvmAddr>;

    #[rpc(meta, name = "signVote")]
    fn sign(&self, _: Self::Metadata, _: BvmAddr, _: Signature, _: Vec<u8>) -> Result<Signature>;

    #[rpc(meta, name = "deregisterNode")]
    fn deregister(&self, _: Self::Metadata, _: BvmAddr, _: Signature, _: Vec<u8>) -> Result<()>;
}

pub struct VoteSignerRpcImpl;
impl VoteSignerRpc for VoteSignerRpcImpl {
    type Metadata = Meta;

    fn register(
        &self,
        meta: Self::Metadata,
        id: BvmAddr,
        sig: Signature,
        signed_msg: Vec<u8>,
    ) -> Result<BvmAddr> {
        // info!("{}", Info(format!("register rpc request received: {:?}", id).to_string()));
        println!("{}",
            printLn(
                format!("register rpc request received: {:?}", id).to_string(),
                module_path!().to_string()
            )
        );
        meta.request_processor.register(&id, &sig, &signed_msg)
    }

    fn sign(
        &self,
        meta: Self::Metadata,
        id: BvmAddr,
        sig: Signature,
        signed_msg: Vec<u8>,
    ) -> Result<Signature> {
        // info!("{}", Info(format!("sign rpc request received: {:?}", id).to_string()));
        println!("{}",
            printLn(
                format!("sign rpc request received: {:?}", id).to_string(),
                module_path!().to_string()
            )
        );
        meta.request_processor.sign(&id, &sig, &signed_msg)
    }

    fn deregister(
        &self,
        meta: Self::Metadata,
        id: BvmAddr,
        sig: Signature,
        signed_msg: Vec<u8>,
    ) -> Result<()> {
        // info!("{}", Info(format!("deregister rpc request received: {:?}", id).to_string()));
        println!("{}",
            printLn(
                format!("deregister rpc request received: {:?}", id).to_string(),
                module_path!().to_string()
            )
        );
        meta.request_processor.deregister(&id, &sig, &signed_msg)
    }
}

fn verify_signature(sig: &Signature, pubkey: &BvmAddr, msg: &[u8]) -> Result<()> {
    if sig.verify(pubkey.as_ref(), msg) {
        Ok(())
    } else {
        Err(Error::invalid_request())
    }
}

pub trait VoteSigner {
    fn register(&self, pubkey: &BvmAddr, sig: &Signature, signed_msg: &[u8]) -> Result<BvmAddr>;
    fn sign(&self, pubkey: &BvmAddr, sig: &Signature, msg: &[u8]) -> Result<Signature>;
    fn deregister(&self, pubkey: &BvmAddr, sig: &Signature, msg: &[u8]) -> Result<()>;
}

#[derive(Clone)]
pub struct LocalVoteSigner {
    nodes: Arc<RwLock<HashMap<BvmAddr, Keypair>>>,
}
impl VoteSigner for LocalVoteSigner {
    /// Process JSON-RPC request items sent via JSON-RPC.
    fn register(&self, pubkey: &BvmAddr, sig: &Signature, msg: &[u8]) -> Result<BvmAddr> {
        verify_signature(&sig, &pubkey, &msg)?;
        {
            if let Some(voting_keypair) = self.nodes.read().unwrap().get(&pubkey) {
                return Ok(voting_keypair.pubkey());
            }
        }
        let voting_keypair = Keypair::new();
        let voting_pubkey = voting_keypair.pubkey();
        self.nodes.write().unwrap().insert(*pubkey, voting_keypair);
        Ok(voting_pubkey)
    }
    fn sign(&self, pubkey: &BvmAddr, sig: &Signature, msg: &[u8]) -> Result<Signature> {
        verify_signature(&sig, &pubkey, &msg)?;
        match self.nodes.read().unwrap().get(&pubkey) {
            Some(voting_keypair) => Ok(voting_keypair.sign_context(&msg)),
            None => Err(Error::invalid_request()),
        }
    }
    fn deregister(&self, pubkey: &BvmAddr, sig: &Signature, msg: &[u8]) -> Result<()> {
        verify_signature(&sig, &pubkey, &msg)?;
        self.nodes.write().unwrap().remove(&pubkey);
        Ok(())
    }
}

impl Default for LocalVoteSigner {
    fn default() -> Self {
        LocalVoteSigner {
            nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonrpc_core::{types::*, Response};
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use std::mem;

    fn start_rpc_handler() -> (MetaIoHandler<Meta>, Meta) {
        let request_processor = LocalVoteSigner::default();
        let mut io = MetaIoHandler::default();
        let rpc = VoteSignerRpcImpl;
        io.extend_with(rpc.to_delegate());
        let meta = Meta { request_processor };
        (io, meta)
    }

    #[test]
    fn test_rpc_register_node() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let sig = node_keypair.sign_context(msg.as_bytes());
        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "registerNode",
           "params": [node_pubkey, sig, msg.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Success(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
                assert_eq!(
                    succ.result.as_array().unwrap().len(),
                    mem::size_of::<BvmAddr>()
                );
                let _pk: BvmAddr = serde_json::from_value(succ.result).unwrap();
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_register_node_invalid_sig() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let msg1 = "This is a Test1";
        let sig = node_keypair.sign_context(msg.as_bytes());
        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "registerNode",
           "params": [node_pubkey, sig, msg1.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Failure(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_deregister_node() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let sig = node_keypair.sign_context(msg.as_bytes());
        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "deregisterNode",
           "params": [node_pubkey, sig, msg.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Success(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_deregister_node_invalid_sig() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let msg1 = "This is a Test1";
        let sig = node_keypair.sign_context(msg.as_bytes());
        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "deregisterNode",
           "params": [node_pubkey, sig, msg1.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Failure(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_sign_vote() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let sig = node_keypair.sign_context(msg.as_bytes());

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "registerNode",
           "params": [node_pubkey, sig, msg.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta.clone());
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let mut vote_pubkey = BvmAddr::new_rand();
        if let Response::Single(out) = result {
            if let Output::Success(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
                assert_eq!(
                    succ.result.as_array().unwrap().len(),
                    mem::size_of::<BvmAddr>()
                );
                vote_pubkey = serde_json::from_value(succ.result).unwrap();
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "signVote",
           "params": [node_pubkey, sig, msg.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Success(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
                assert_eq!(
                    succ.result.as_array().unwrap().len(),
                    mem::size_of::<Signature>()
                );
                let sig: Signature = serde_json::from_value(succ.result).unwrap();
                assert_eq!(verify_signature(&sig, &vote_pubkey, msg.as_bytes()), Ok(()));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_sign_vote_before_register() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let sig = node_keypair.sign_context(msg.as_bytes());
        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "signVote",
           "params": [node_pubkey, sig, msg.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Failure(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_sign_vote_after_deregister() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let sig = node_keypair.sign_context(msg.as_bytes());

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "registerNode",
           "params": [node_pubkey, sig, msg.as_bytes()],
        });
        let _res = io.handle_request_sync(&req.to_string(), meta.clone());

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "deregisterNode",
           "params": [node_pubkey, sig, msg.as_bytes()],
        });
        let _res = io.handle_request_sync(&req.to_string(), meta.clone());

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "signVote",
           "params": [node_pubkey, sig, msg.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Failure(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_sign_vote_invalid_sig() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let msg1 = "This is a Test";
        let sig = node_keypair.sign_context(msg.as_bytes());

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "registerNode",
           "params": [node_pubkey, sig, msg.as_bytes()],
        });
        let _res = io.handle_request_sync(&req.to_string(), meta.clone());

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "signVote",
           "params": [node_pubkey, sig, msg1.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Failure(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }
}
