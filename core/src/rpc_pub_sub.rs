//! The `pubsub` module implements a threaded subscription service on client RPC request

use crate::rpc_subscriptions::{Confirmations, RpcSubscriptions};
use jsonrpc_core::{Error, ErrorCode, Result};
use jsonrpc_derive::rpc;
use jsonrpc_pubsub::typed::Subscriber;
use jsonrpc_pubsub::{Session, SubscriptionId};
use morgan_interface::account::Account;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::Signature;
use morgan_interface::transaction;
use std::sync::{atomic, Arc};
use morgan_helper::logHelper::*;

#[rpc(server)]
pub trait RpcSolPubSub {
    type Metadata;

    // Get notification every time account data is changed
    // Accepts address parameter as base-58 encoded string
    #[pubsub(
        subscription = "accountNotification",
        subscribe,
        name = "accountSubscribe"
    )]
    fn account_subscribe(
        &self,
        _: Self::Metadata,
        _: Subscriber<Account>,
        _: String,
        _: Option<Confirmations>,
    );

    // Unsubscribe from account notification subscription.
    #[pubsub(
        subscription = "accountNotification",
        unsubscribe,
        name = "accountUnsubscribe"
    )]
    fn account_unsubscribe(&self, _: Option<Self::Metadata>, _: SubscriptionId) -> Result<bool>;

    // Get notification every time account data owned by a particular program is changed
    // Accepts address parameter as base-58 encoded string
    #[pubsub(
        subscription = "programNotification",
        subscribe,
        name = "programSubscribe"
    )]
    fn program_subscribe(
        &self,
        _: Self::Metadata,
        _: Subscriber<(String, Account)>,
        _: String,
        _: Option<Confirmations>,
    );

    // Unsubscribe from account notification subscription.
    #[pubsub(
        subscription = "programNotification",
        unsubscribe,
        name = "programUnsubscribe"
    )]
    fn program_unsubscribe(&self, _: Option<Self::Metadata>, _: SubscriptionId) -> Result<bool>;

    // Get notification when signature is verified
    // Accepts signature parameter as base-58 encoded string
    #[pubsub(
        subscription = "signatureNotification",
        subscribe,
        name = "signatureSubscribe"
    )]
    fn signature_subscribe(
        &self,
        _: Self::Metadata,
        _: Subscriber<transaction::Result<()>>,
        _: String,
        _: Option<Confirmations>,
    );

    // Unsubscribe from signature notification subscription.
    #[pubsub(
        subscription = "signatureNotification",
        unsubscribe,
        name = "signatureUnsubscribe"
    )]
    fn signature_unsubscribe(&self, _: Option<Self::Metadata>, _: SubscriptionId) -> Result<bool>;
}

#[derive(Default)]
pub struct RpcSolPubSubImpl {
    uid: Arc<atomic::AtomicUsize>,
    subscriptions: Arc<RpcSubscriptions>,
}

impl RpcSolPubSubImpl {
    pub fn new(subscriptions: Arc<RpcSubscriptions>) -> Self {
        let uid = Arc::new(atomic::AtomicUsize::default());
        Self { uid, subscriptions }
    }
}

use std::str::FromStr;

fn param<T: FromStr>(param_str: &str, thing: &str) -> Result<T> {
    param_str.parse::<T>().map_err(|_e| Error {
        code: ErrorCode::InvalidParams,
        message: format!("Invalid Request: Invalid {} provided", thing),
        data: None,
    })
}

impl RpcSolPubSub for RpcSolPubSubImpl {
    type Metadata = Arc<Session>;

    fn account_subscribe(
        &self,
        _meta: Self::Metadata,
        subscriber: Subscriber<Account>,
        address_str: String,
        confirmations: Option<Confirmations>,
    ) {
        match param::<BvmAddr>(&address_str, "address") {
            Ok(address) => {
                let id = self.uid.fetch_add(1, atomic::Ordering::SeqCst);
                let sub_id = SubscriptionId::Number(id as u64);
                // info!("{}", Info(format!("account_subscribe: account={:?} id={:?}", address, sub_id).to_string()));
                println!("{}",
                    printLn(
                        format!("account_subscribe: account={:?} id={:?}", address, sub_id).to_string(),
                        module_path!().to_string()
                    )
                );
                let sink = subscriber.assign_id(sub_id.clone()).unwrap();

                self.subscriptions
                    .add_account_subscription(&address, confirmations, &sub_id, &sink)
            }
            Err(e) => subscriber.reject(e).unwrap(),
        }
    }

    fn account_unsubscribe(
        &self,
        _meta: Option<Self::Metadata>,
        id: SubscriptionId,
    ) -> Result<bool> {
        // info!("{}", Info(format!("account_unsubscribe: id={:?}", id).to_string()));
        println!("{}",
            printLn(
                format!("account_unsubscribe: id={:?}", id).to_string(),
                module_path!().to_string()
            )
        );
        if self.subscriptions.remove_account_subscription(&id) {
            Ok(true)
        } else {
            Err(Error {
                code: ErrorCode::InvalidParams,
                message: "Invalid Request: Subscription id does not exist".into(),
                data: None,
            })
        }
    }

    fn program_subscribe(
        &self,
        _meta: Self::Metadata,
        subscriber: Subscriber<(String, Account)>,
        address_str: String,
        confirmations: Option<Confirmations>,
    ) {
        match param::<BvmAddr>(&address_str, "address") {
            Ok(address) => {
                let id = self.uid.fetch_add(1, atomic::Ordering::SeqCst);
                let sub_id = SubscriptionId::Number(id as u64);
                // info!("{}", Info(format!("program_subscribe: account={:?} id={:?}", address, sub_id).to_string()));
                println!("{}",
                    printLn(
                        format!("program_subscribe: account={:?} id={:?}", address, sub_id).to_string(),
                        module_path!().to_string()
                    )
                );
                let sink = subscriber.assign_id(sub_id.clone()).unwrap();

                self.subscriptions
                    .add_program_subscription(&address, confirmations, &sub_id, &sink)
            }
            Err(e) => subscriber.reject(e).unwrap(),
        }
    }

    fn program_unsubscribe(
        &self,
        _meta: Option<Self::Metadata>,
        id: SubscriptionId,
    ) -> Result<bool> {
        // info!("{}", Info(format!("program_unsubscribe: id={:?}", id).to_string()));
        println!("{}",
            printLn(
                format!("program_unsubscribe: id={:?}", id).to_string(),
                module_path!().to_string()
            )
        );
        if self.subscriptions.remove_program_subscription(&id) {
            Ok(true)
        } else {
            Err(Error {
                code: ErrorCode::InvalidParams,
                message: "Invalid Request: Subscription id does not exist".into(),
                data: None,
            })
        }
    }

    fn signature_subscribe(
        &self,
        _meta: Self::Metadata,
        subscriber: Subscriber<transaction::Result<()>>,
        signature_str: String,
        confirmations: Option<Confirmations>,
    ) {
        // info!("{}", Info(format!("signature_subscribe").to_string()));
        println!("{}",
            printLn(
                format!("signature_subscribe").to_string(),
                module_path!().to_string()
            )
        );
        match param::<Signature>(&signature_str, "signature") {
            Ok(signature) => {
                let id = self.uid.fetch_add(1, atomic::Ordering::SeqCst);
                let sub_id = SubscriptionId::Number(id as u64);
                // info!(
                //     "{}",
                //     Info(format!("signature_subscribe: signature={:?} id={:?}",
                //     signature, sub_id).to_string())
                // );
                println!("{}",
                    printLn(
                        format!("signature_subscribe: signature={:?} id={:?}",
                            signature, sub_id).to_string(),
                        module_path!().to_string()
                    )
                );
                let sink = subscriber.assign_id(sub_id.clone()).unwrap();

                self.subscriptions.add_signature_subscription(
                    &signature,
                    confirmations,
                    &sub_id,
                    &sink,
                );
            }
            Err(e) => subscriber.reject(e).unwrap(),
        }
    }

    fn signature_unsubscribe(
        &self,
        _meta: Option<Self::Metadata>,
        id: SubscriptionId,
    ) -> Result<bool> {
        // info!("{}", Info(format!("signature_unsubscribe").to_string()));
        println!("{}",
            printLn(
                format!("signature_unsubscribe").to_string(),
                module_path!().to_string()
            )
        );
        if self.subscriptions.remove_signature_subscription(&id) {
            Ok(true)
        } else {
            Err(Error {
                code: ErrorCode::InvalidParams,
                message: "Invalid Request: Subscription id does not exist".into(),
                data: None,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::treasury_forks::TreasuryForks;
    use crate::treasury_forks::TreasuryForks;
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use jsonrpc_core::futures::sync::mpsc;
    use jsonrpc_core::Response;
    use jsonrpc_pubsub::{PubSubHandler, Session};
    use morgan_bvm_script;
    use morgan_bvm_script::sc_opcode;
    use morgan_runtime::treasury::Treasury;
    use morgan_interface::bvm_address::BvmAddr;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::sys_controller;
    use morgan_interface::transaction::{self, Transaction};
    use std::sync::RwLock;
    use std::thread::sleep;
    use std::time::Duration;
    use tokio::prelude::{Async, Stream};

    fn process_transaction_and_notify(
        treasury_forks: &Arc<RwLock<TreasuryForks>>,
        tx: &Transaction,
        subscriptions: &RpcSubscriptions,
    ) -> transaction::Result<()> {
        treasury_forks
            .write()
            .unwrap()
            .get(0)
            .unwrap()
            .process_transaction(tx)?;
        subscriptions.notify_subscribers(0, &treasury_forks);
        Ok(())
    }

    fn create_session() -> Arc<Session> {
        Arc::new(Session::new(mpsc::channel(1).0))
    }

    #[test]
    fn test_signature_subscribe() {
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair: alice,
            ..
        } = create_genesis_block(10_000);
        let bob = Keypair::new();
        let bob_address = bob.address();
        let treasury = Treasury::new(&genesis_block);
        let transaction_seal = treasury.last_transaction_seal();
        let treasury_forks = Arc::new(RwLock::new(TreasuryForks::new(0, treasury)));

        let rpc = RpcSolPubSubImpl::default();

        // Test signature subscriptions
        let tx = sys_controller::transfer(&alice, &bob_address, 20, transaction_seal);

        let session = create_session();
        let (subscriber, _id_receiver, mut receiver) =
            Subscriber::new_test("signatureNotification");
        rpc.signature_subscribe(session, subscriber, tx.signatures[0].to_string(), None);

        process_transaction_and_notify(&treasury_forks, &tx, &rpc.subscriptions).unwrap();
        sleep(Duration::from_millis(200));

        // Test signature confirmation notification
        let string = receiver.poll();
        if let Async::Ready(Some(response)) = string.unwrap() {
            let expected_res: Option<transaction::Result<()>> = Some(Ok(()));
            let expected_res_str =
                serde_json::to_string(&serde_json::to_value(expected_res).unwrap()).unwrap();
            let expected = format!(r#"{{"jsonrpc":"2.0","method":"signatureNotification","params":{{"result":{},"subscription":0}}}}"#, expected_res_str);
            assert_eq!(expected, response);
        }
    }

    #[test]
    fn test_signature_unsubscribe() {
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair: alice,
            ..
        } = create_genesis_block(10_000);
        let bob_address = BvmAddr::new_rand();
        let treasury = Treasury::new(&genesis_block);
        let arc_treasury = Arc::new(treasury);
        let transaction_seal = arc_treasury.last_transaction_seal();

        let session = create_session();

        let mut io = PubSubHandler::default();
        let rpc = RpcSolPubSubImpl::default();
        io.extend_with(rpc.to_delegate());

        let tx = sys_controller::transfer(&alice, &bob_address, 20, transaction_seal);
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"signatureSubscribe","params":["{}"]}}"#,
            tx.signatures[0].to_string()
        );
        let _res = io.handle_request_sync(&req, session.clone());

        let req =
            format!(r#"{{"jsonrpc":"2.0","id":1,"method":"signatureUnsubscribe","params":[0]}}"#);
        let res = io.handle_request_sync(&req, session.clone());

        let expected = format!(r#"{{"jsonrpc":"2.0","result":true,"id":1}}"#);
        let expected: Response = serde_json::from_str(&expected).unwrap();

        let result: Response = serde_json::from_str(&res.unwrap()).unwrap();
        assert_eq!(expected, result);

        // Test bad parameter
        let req =
            format!(r#"{{"jsonrpc":"2.0","id":1,"method":"signatureUnsubscribe","params":[1]}}"#);
        let res = io.handle_request_sync(&req, session.clone());
        let expected = format!(r#"{{"jsonrpc":"2.0","error":{{"code":-32602,"message":"Invalid Request: Subscription id does not exist"}},"id":1}}"#);
        let expected: Response = serde_json::from_str(&expected).unwrap();

        let result: Response = serde_json::from_str(&res.unwrap()).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_account_subscribe() {
        let GenesisBlockInfo {
            mut genesis_block,
            mint_keypair: alice,
            ..
        } = create_genesis_block(10_000);

        // This test depends on the budget program
        genesis_block
            .builtin_opcode_handlers
            .push(morgan_budget_controller!());

        let bob_address = BvmAddr::new_rand();
        let witness = Keypair::new();
        let contract_funds = Keypair::new();
        let contract_state = Keypair::new();
        let budget_program_id = morgan_bvm_script::id();
        let executable = false; // TODO
        let treasury = Treasury::new(&genesis_block);
        let transaction_seal = treasury.last_transaction_seal();
        let treasury_forks = Arc::new(RwLock::new(TreasuryForks::new(0, treasury)));

        let rpc = RpcSolPubSubImpl::default();
        let session = create_session();
        let (subscriber, _id_receiver, mut receiver) = Subscriber::new_test("accountNotification");
        rpc.account_subscribe(
            session,
            subscriber,
            contract_state.address().to_string(),
            None,
        );

        let tx = sys_controller::create_user_account(
            &alice,
            &contract_funds.address(),
            51,
            transaction_seal,
        );
        process_transaction_and_notify(&treasury_forks, &tx, &rpc.subscriptions).unwrap();

        let ixs = sc_opcode::when_signed(
            &contract_funds.address(),
            &bob_address,
            &contract_state.address(),
            &witness.address(),
            None,
            51,
        );
        let tx = Transaction::new_s_opcodes(&[&contract_funds], ixs, transaction_seal);
        process_transaction_and_notify(&treasury_forks, &tx, &rpc.subscriptions).unwrap();
        sleep(Duration::from_millis(200));

        // Test signature confirmation notification #1
        let string = receiver.poll();
        let expected_data = treasury_forks
            .read()
            .unwrap()
            .get(0)
            .unwrap()
            .get_account(&contract_state.address())
            .unwrap()
            .data;
        let expected = json!({
           "jsonrpc": "2.0",
           "method": "accountNotification",
           "params": {
               "result": {
                   "owner": budget_program_id,
                   "difs": 51,
                   "reputations": 0,
                   "data": expected_data,
                    "executable": executable,
               },
               "subscription": 0,
           }
        });

        if let Async::Ready(Some(response)) = string.unwrap() {
            assert_eq!(serde_json::to_string(&expected).unwrap(), response);
        }

        let tx = sys_controller::create_user_account(&alice, &witness.address(), 1, transaction_seal);
        process_transaction_and_notify(&treasury_forks, &tx, &rpc.subscriptions).unwrap();
        sleep(Duration::from_millis(200));
        let ix = sc_opcode::apply_signature(
            &witness.address(),
            &contract_state.address(),
            &bob_address,
        );
        let tx = Transaction::new_s_opcodes(&[&witness], vec![ix], transaction_seal);
        process_transaction_and_notify(&treasury_forks, &tx, &rpc.subscriptions).unwrap();
        sleep(Duration::from_millis(200));

        assert_eq!(
            treasury_forks
                .read()
                .unwrap()
                .get(0)
                .unwrap()
                .get_account(&contract_state.address()),
            None
        );
    }

    #[test]
    fn test_account_unsubscribe() {
        let bob_address = BvmAddr::new_rand();
        let session = create_session();

        let mut io = PubSubHandler::default();
        let rpc = RpcSolPubSubImpl::default();

        io.extend_with(rpc.to_delegate());

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"accountSubscribe","params":["{}"]}}"#,
            bob_address.to_string()
        );
        let _res = io.handle_request_sync(&req, session.clone());

        let req =
            format!(r#"{{"jsonrpc":"2.0","id":1,"method":"accountUnsubscribe","params":[0]}}"#);
        let res = io.handle_request_sync(&req, session.clone());

        let expected = format!(r#"{{"jsonrpc":"2.0","result":true,"id":1}}"#);
        let expected: Response = serde_json::from_str(&expected).unwrap();

        let result: Response = serde_json::from_str(&res.unwrap()).unwrap();
        assert_eq!(expected, result);

        // Test bad parameter
        let req =
            format!(r#"{{"jsonrpc":"2.0","id":1,"method":"accountUnsubscribe","params":[1]}}"#);
        let res = io.handle_request_sync(&req, session.clone());
        let expected = format!(r#"{{"jsonrpc":"2.0","error":{{"code":-32602,"message":"Invalid Request: Subscription id does not exist"}},"id":1}}"#);
        let expected: Response = serde_json::from_str(&expected).unwrap();

        let result: Response = serde_json::from_str(&res.unwrap()).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    #[should_panic]
    fn test_account_confirmations_not_fulfilled() {
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair: alice,
            ..
        } = create_genesis_block(10_000);
        let treasury = Treasury::new(&genesis_block);
        let transaction_seal = treasury.last_transaction_seal();
        let treasury_forks = Arc::new(RwLock::new(TreasuryForks::new(0, treasury)));
        let bob = Keypair::new();

        let rpc = RpcSolPubSubImpl::default();
        let session = create_session();
        let (subscriber, _id_receiver, mut receiver) = Subscriber::new_test("accountNotification");
        rpc.account_subscribe(session, subscriber, bob.address().to_string(), Some(2));

        let tx = sys_controller::transfer(&alice, &bob.address(), 100, transaction_seal);
        treasury_forks
            .write()
            .unwrap()
            .get(0)
            .unwrap()
            .process_transaction(&tx)
            .unwrap();
        rpc.subscriptions.notify_subscribers(0, &treasury_forks);
        let _panic = receiver.poll();
    }

    #[test]
    fn test_account_confirmations() {
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair: alice,
            ..
        } = create_genesis_block(10_000);
        let treasury = Treasury::new(&genesis_block);
        let transaction_seal = treasury.last_transaction_seal();
        let treasury_forks = Arc::new(RwLock::new(TreasuryForks::new(0, treasury)));
        let bob = Keypair::new();

        let rpc = RpcSolPubSubImpl::default();
        let session = create_session();
        let (subscriber, _id_receiver, mut receiver) = Subscriber::new_test("accountNotification");
        rpc.account_subscribe(session, subscriber, bob.address().to_string(), Some(2));

        let tx = sys_controller::transfer(&alice, &bob.address(), 100, transaction_seal);
        treasury_forks
            .write()
            .unwrap()
            .get(0)
            .unwrap()
            .process_transaction(&tx)
            .unwrap();
        rpc.subscriptions.notify_subscribers(0, &treasury_forks);

        let treasury0 = treasury_forks.read().unwrap()[0].clone();
        let treasury1 = Treasury::new_from_parent(&treasury0, &BvmAddr::default(), 1);
        treasury_forks.write().unwrap().insert(treasury1);
        rpc.subscriptions.notify_subscribers(1, &treasury_forks);
        let treasury1 = treasury_forks.read().unwrap()[1].clone();
        let treasury2 = Treasury::new_from_parent(&treasury1, &BvmAddr::default(), 2);
        treasury_forks.write().unwrap().insert(treasury2);
        rpc.subscriptions.notify_subscribers(2, &treasury_forks);
        let string = receiver.poll();
        let expected = json!({
           "jsonrpc": "2.0",
           "method": "accountNotification",
           "params": {
               "result": {
                   "owner": sys_controller::id(),
                   "difs": 100,
                   "reputations": 0,
                   "data": [],
                   "executable": false,
               },
               "subscription": 0,
           }
        });
        if let Async::Ready(Some(response)) = string.unwrap() {
            assert_eq!(serde_json::to_string(&expected).unwrap(), response);
        }
    }
}
