//! The `rpc_service` module implements the Morgan JSON RPC service.

// use crate::treasury_forks::BankForks;
use crate::treasury_forks::BankForks;
use crate::node_group_info::NodeGroupInfo;
use crate::rpc::*;
use crate::service::Service;
use crate::storage_stage::StorageState;
use jsonrpc_core::MetaIoHandler;
use jsonrpc_http_server::{hyper, AccessControlAllowOrigin, DomainsValidation, ServerBuilder};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, sleep, Builder, JoinHandle};
use std::time::Duration;
use morgan_helper::logHelper::*;

pub struct JsonRpcService {
    thread_hdl: JoinHandle<()>,

    #[cfg(test)]
    pub request_processor: Arc<RwLock<JsonRpcRequestProcessor>>, // Used only by test_rpc_new()...
}

impl JsonRpcService {
    pub fn new(
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        rpc_addr: SocketAddr,
        storage_state: StorageState,
        config: JsonRpcConfig,
        treasury_forks: Arc<RwLock<BankForks>>,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        // info!("{}", Info(format!("rpc bound to {:?}", rpc_addr).to_string()));
        // info!("{}", Info(format!("rpc configuration: {:?}", config).to_string()));
        println!("{}",
            printLn(
                format!("rpc bound to {:?}", rpc_addr).to_string(),
                module_path!().to_string()
            )
        );
        println!("{}",
            printLn(
                format!("rpc configuration: {:?}", config).to_string(),
                module_path!().to_string()
            )
        );
        let request_processor = Arc::new(RwLock::new(JsonRpcRequestProcessor::new(
            storage_state,
            config,
            treasury_forks,
            exit,
        )));
        let request_processor_ = request_processor.clone();

        let node_group_info = node_group_info.clone();
        let exit_ = exit.clone();

        let thread_hdl = Builder::new()
            .name("morgan-jsonrpc".to_string())
            .spawn(move || {
                let mut io = MetaIoHandler::default();
                let rpc = RpcSolImpl;
                io.extend_with(rpc.to_delegate());

                let server =
                    ServerBuilder::with_meta_extractor(io, move |_req: &hyper::Request<hyper::Body>| Meta {
                        request_processor: request_processor_.clone(),
                        node_group_info: node_group_info.clone(),
                    }).threads(4)
                        .cors(DomainsValidation::AllowOnly(vec![
                            AccessControlAllowOrigin::Any,
                        ]))
                        .start_http(&rpc_addr);
                if let Err(e) = server {
                    // warn!("JSON RPC service unavailable error: {:?}. \nAlso, check that port {} is not already in use by another application", e, rpc_addr.port());
                    println!(
                        "{}",
                        Warn(
                            format!("JSON RPC service unavailable error: {:?}. \nAlso, check that port {} is not already in use by another application", e, rpc_addr.port()).to_string(),
                            module_path!().to_string()
                        )
                    );
                    return;
                }
                while !exit_.load(Ordering::Relaxed) {
                    sleep(Duration::from_millis(100));
                }
                server.unwrap().close();
            })
            .unwrap();
        Self {
            thread_hdl,
            #[cfg(test)]
            request_processor,
        }
    }
}

impl Service for JsonRpcService {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

use serde::{de, ser};
use std::{error, fmt};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    Eof,
    ExceededMaxLen(usize),
    ExpectedBoolean,
    ExpectedMapKey,
    ExpectedMapValue,
    ExpectedOption,
    Custom(String),
    MissingLen,
    NotSupported(&'static str),
    RemainingInput,
    Utf8,
}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Custom(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Custom(msg.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(std::error::Error::description(self))
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        use Error::*;

        match self {
            Eof => "unexpected end of input",
            ExceededMaxLen(_) => "exceeded max sequence length",
            ExpectedBoolean => "expected boolean",
            ExpectedMapKey => "expected map key",
            ExpectedMapValue => "expected map value",
            ExpectedOption => "expected option type",
            Custom(msg) => msg,
            MissingLen => "sequence missing length",
            NotSupported(_) => "not supported",
            RemainingInput => "remaining input",
            Utf8 => "malformed utf8",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_info::ContactInfo;
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use morgan_runtime::treasury::Treasury;
    use morgan_interface::signature::KeypairUtil;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn test_rpc_new() {
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(10_000);
        let exit = Arc::new(AtomicBool::new(false));
        let treasury = Treasury::new(&genesis_block);
        let node_group_info = Arc::new(RwLock::new(NodeGroupInfo::new_with_invalid_keypair(
            ContactInfo::default(),
        )));
        let rpc_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            morgan_netutil::find_available_port_in_range((10000, 65535)).unwrap(),
        );
        let treasury_forks = Arc::new(RwLock::new(BankForks::new(treasury.slot(), treasury)));
        let rpc_service = JsonRpcService::new(
            &node_group_info,
            rpc_addr,
            StorageState::default(),
            JsonRpcConfig::default(),
            treasury_forks,
            &exit,
        );
        let thread = rpc_service.thread_hdl.thread();
        assert_eq!(thread.name().unwrap(), "morgan-jsonrpc");

        assert_eq!(
            10_000,
            rpc_service
                .request_processor
                .read()
                .unwrap()
                .get_balance(&mint_keypair.pubkey())
        );
        exit.store(true, Ordering::Relaxed);
        rpc_service.join().unwrap();
    }
}
