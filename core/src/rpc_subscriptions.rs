//! The `pubsub` module implements a threaded subscription service on client RPC request

// use crate::treasury_forks::TreasuryForks;
use crate::treasury_forks::TreasuryForks;
use core::hash::Hash;
use jsonrpc_core::futures::Future;
use jsonrpc_pubsub::typed::Sink;
use jsonrpc_pubsub::SubscriptionId;
use serde::Serialize;
use morgan_runtime::treasury::Treasury;
use morgan_interface::account::Account;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::Signature;
use morgan_interface::transaction;
use morgan_vote_api::vote_state::MAX_LOCKOUT_HISTORY;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub type Confirmations = usize;

type RpcAccountSubscriptions =
    RwLock<HashMap<BvmAddr, HashMap<SubscriptionId, (Sink<Account>, Confirmations)>>>;
type RpcProgramSubscriptions =
    RwLock<HashMap<BvmAddr, HashMap<SubscriptionId, (Sink<(String, Account)>, Confirmations)>>>;
type RpcSignatureSubscriptions = RwLock<
    HashMap<Signature, HashMap<SubscriptionId, (Sink<transaction::Result<()>>, Confirmations)>>,
>;

fn add_subscription<K, S>(
    subscriptions: &mut HashMap<K, HashMap<SubscriptionId, (Sink<S>, Confirmations)>>,
    hashmap_key: &K,
    confirmations: Option<Confirmations>,
    sub_id: &SubscriptionId,
    sink: &Sink<S>,
) where
    K: Eq + Hash + Clone + Copy,
    S: Clone,
{
    let confirmations = confirmations.unwrap_or(0);
    let confirmations = if confirmations > MAX_LOCKOUT_HISTORY {
        MAX_LOCKOUT_HISTORY
    } else {
        confirmations
    };
    if let Some(current_hashmap) = subscriptions.get_mut(hashmap_key) {
        current_hashmap.insert(sub_id.clone(), (sink.clone(), confirmations));
        return;
    }
    let mut hashmap = HashMap::new();
    hashmap.insert(sub_id.clone(), (sink.clone(), confirmations));
    subscriptions.insert(*hashmap_key, hashmap);
}

fn remove_subscription<K, S>(
    subscriptions: &mut HashMap<K, HashMap<SubscriptionId, (Sink<S>, Confirmations)>>,
    sub_id: &SubscriptionId,
) -> bool
where
    K: Eq + Hash + Clone + Copy,
    S: Clone,
{
    let mut found = false;
    subscriptions.retain(|_, v| {
        v.retain(|k, _| {
            if *k == *sub_id {
                found = true;
            }
            !found
        });
        !v.is_empty()
    });
    found
}

fn check_confirmations_and_notify<K, S, F, N, X>(
    subscriptions: &HashMap<K, HashMap<SubscriptionId, (Sink<S>, Confirmations)>>,
    hashmap_key: &K,
    current_slot: u64,
    treasury_forks: &Arc<RwLock<TreasuryForks>>,
    treasury_method: F,
    notify: N,
) where
    K: Eq + Hash + Clone + Copy,
    S: Clone + Serialize,
    F: Fn(&Treasury, &K) -> X,
    N: Fn(X, &Sink<S>, u64),
    X: Clone + Serialize,
{
    let current_ancestors = treasury_forks
        .read()
        .unwrap()
        .get(current_slot)
        .unwrap()
        .ancestors
        .clone();
    if let Some(hashmap) = subscriptions.get(hashmap_key) {
        for (_treasury_sub_id, (sink, confirmations)) in hashmap.iter() {
            let desired_slot: Vec<u64> = current_ancestors
                .iter()
                .filter(|(_, &v)| v == *confirmations)
                .map(|(k, _)| k)
                .cloned()
                .collect();
            let root: Vec<u64> = current_ancestors
                .iter()
                .filter(|(_, &v)| v == 32)
                .map(|(k, _)| k)
                .cloned()
                .collect();
            let root = if root.len() == 1 { root[0] } else { 0 };
            if desired_slot.len() == 1 {
                let desired_treasury = treasury_forks
                    .read()
                    .unwrap()
                    .get(desired_slot[0])
                    .unwrap()
                    .clone();
                let result = treasury_method(&desired_treasury, hashmap_key);
                notify(result, &sink, root);
            }
        }
    }
}

fn notify_account<S>(result: Option<(S, u64)>, sink: &Sink<S>, root: u64)
where
    S: Clone + Serialize,
{
    if let Some((account, fork)) = result {
        if fork >= root {
            sink.notify(Ok(account)).wait().unwrap();
        }
    }
}

fn notify_signature<S>(result: Option<S>, sink: &Sink<S>, _root: u64)
where
    S: Clone + Serialize,
{
    if let Some(result) = result {
        sink.notify(Ok(result)).wait().unwrap();
    }
}

fn notify_program(accounts: Vec<(BvmAddr, Account)>, sink: &Sink<(String, Account)>, _root: u64) {
    for (address, account) in accounts.iter() {
        sink.notify(Ok((address.to_string(), account.clone())))
            .wait()
            .unwrap();
    }
}

pub struct RpcSubscriptions {
    account_subscriptions: RpcAccountSubscriptions,
    program_subscriptions: RpcProgramSubscriptions,
    signature_subscriptions: RpcSignatureSubscriptions,
}

impl Default for RpcSubscriptions {
    fn default() -> Self {
        RpcSubscriptions {
            account_subscriptions: RpcAccountSubscriptions::default(),
            program_subscriptions: RpcProgramSubscriptions::default(),
            signature_subscriptions: RpcSignatureSubscriptions::default(),
        }
    }
}

impl RpcSubscriptions {
    pub fn check_account(
        &self,
        address: &BvmAddr,
        current_slot: u64,
        treasury_forks: &Arc<RwLock<TreasuryForks>>,
    ) {
        let subscriptions = self.account_subscriptions.read().unwrap();
        check_confirmations_and_notify(
            &subscriptions,
            address,
            current_slot,
            treasury_forks,
            Treasury::get_account_modified_since_parent,
            notify_account,
        );
    }

    pub fn check_program(
        &self,
        program_id: &BvmAddr,
        current_slot: u64,
        treasury_forks: &Arc<RwLock<TreasuryForks>>,
    ) {
        let subscriptions = self.program_subscriptions.write().unwrap();
        check_confirmations_and_notify(
            &subscriptions,
            program_id,
            current_slot,
            treasury_forks,
            Treasury::get_program_accounts_modified_since_parent,
            notify_program,
        );
    }

    pub fn check_signature(
        &self,
        signature: &Signature,
        current_slot: u64,
        treasury_forks: &Arc<RwLock<TreasuryForks>>,
    ) {
        let mut subscriptions = self.signature_subscriptions.write().unwrap();
        check_confirmations_and_notify(
            &subscriptions,
            signature,
            current_slot,
            treasury_forks,
            Treasury::get_signature_status,
            notify_signature,
        );
        subscriptions.remove(&signature);
    }

    pub fn add_account_subscription(
        &self,
        address: &BvmAddr,
        confirmations: Option<Confirmations>,
        sub_id: &SubscriptionId,
        sink: &Sink<Account>,
    ) {
        let mut subscriptions = self.account_subscriptions.write().unwrap();
        add_subscription(&mut subscriptions, address, confirmations, sub_id, sink);
    }

    pub fn remove_account_subscription(&self, id: &SubscriptionId) -> bool {
        let mut subscriptions = self.account_subscriptions.write().unwrap();
        remove_subscription(&mut subscriptions, id)
    }

    pub fn add_program_subscription(
        &self,
        program_id: &BvmAddr,
        confirmations: Option<Confirmations>,
        sub_id: &SubscriptionId,
        sink: &Sink<(String, Account)>,
    ) {
        let mut subscriptions = self.program_subscriptions.write().unwrap();
        add_subscription(&mut subscriptions, program_id, confirmations, sub_id, sink);
    }

    pub fn remove_program_subscription(&self, id: &SubscriptionId) -> bool {
        let mut subscriptions = self.program_subscriptions.write().unwrap();
        remove_subscription(&mut subscriptions, id)
    }

    pub fn add_signature_subscription(
        &self,
        signature: &Signature,
        confirmations: Option<Confirmations>,
        sub_id: &SubscriptionId,
        sink: &Sink<transaction::Result<()>>,
    ) {
        let mut subscriptions = self.signature_subscriptions.write().unwrap();
        add_subscription(&mut subscriptions, signature, confirmations, sub_id, sink);
    }

    pub fn remove_signature_subscription(&self, id: &SubscriptionId) -> bool {
        let mut subscriptions = self.signature_subscriptions.write().unwrap();
        remove_subscription(&mut subscriptions, id)
    }

    /// Notify subscribers of changes to any accounts or new signatures since
    /// the treasury's last checkpoint.
    pub fn notify_subscribers(&self, current_slot: u64, treasury_forks: &Arc<RwLock<TreasuryForks>>) {
        let addresss: Vec<_> = {
            let subs = self.account_subscriptions.read().unwrap();
            subs.keys().cloned().collect()
        };
        for address in &addresss {
            self.check_account(address, current_slot, treasury_forks);
        }

        let programs: Vec<_> = {
            let subs = self.program_subscriptions.read().unwrap();
            subs.keys().cloned().collect()
        };
        for program_id in &programs {
            self.check_program(program_id, current_slot, treasury_forks);
        }

        let signatures: Vec<_> = {
            let subs = self.signature_subscriptions.read().unwrap();
            subs.keys().cloned().collect()
        };
        for signature in &signatures {
            self.check_signature(signature, current_slot, treasury_forks);
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use jsonrpc_pubsub::typed::Subscriber;
    use morgan_bvm_script;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::sys_controller;
    use tokio::prelude::{Async, Stream};

    #[test]
    fn test_check_account_subscribe() {
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(100);
        let treasury = Treasury::new(&genesis_block);
        let transaction_seal = treasury.last_transaction_seal();
        let treasury_forks = Arc::new(RwLock::new(TreasuryForks::new(0, treasury)));
        let alice = Keypair::new();
        let tx = sys_controller::create_account(
            &mint_keypair,
            &alice.address(),
            transaction_seal,
            1,
            16,
            &morgan_bvm_script::id(),
        );
        treasury_forks
            .write()
            .unwrap()
            .get(0)
            .unwrap()
            .process_transaction(&tx)
            .unwrap();

        let (subscriber, _id_receiver, mut transport_receiver) =
            Subscriber::new_test("accountNotification");
        let sub_id = SubscriptionId::Number(0 as u64);
        let sink = subscriber.assign_id(sub_id.clone()).unwrap();
        let subscriptions = RpcSubscriptions::default();
        subscriptions.add_account_subscription(&alice.address(), None, &sub_id, &sink);

        assert!(subscriptions
            .account_subscriptions
            .read()
            .unwrap()
            .contains_key(&alice.address()));

        subscriptions.check_account(&alice.address(), 0, &treasury_forks);
        let string = transport_receiver.poll();
        println!("response : {:?}", string);
        if let Async::Ready(Some(response)) = string.unwrap() {
            let expected = format!(r#"{{"jsonrpc":"2.0","method":"accountNotification","params":{{"result":{{"data":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"difs":1,"executable":false,"owner":[2,203,81,223,225,24,34,35,203,214,138,130,144,208,35,77,63,16,87,51,47,198,115,123,98,188,19,160,0,0,0,0],"reputations":0}},"subscription":0}}}}"#);
            assert_eq!(expected, response);
        }

        subscriptions.remove_account_subscription(&sub_id);
        assert!(!subscriptions
            .account_subscriptions
            .read()
            .unwrap()
            .contains_key(&alice.address()));
    }

    #[test]
    fn test_check_program_subscribe() {
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(100);
        let treasury = Treasury::new(&genesis_block);
        let transaction_seal = treasury.last_transaction_seal();
        let treasury_forks = Arc::new(RwLock::new(TreasuryForks::new(0, treasury)));
        let alice = Keypair::new();
        let tx = sys_controller::create_account(
            &mint_keypair,
            &alice.address(),
            transaction_seal,
            1,
            16,
            &morgan_bvm_script::id(),
        );
        treasury_forks
            .write()
            .unwrap()
            .get(0)
            .unwrap()
            .process_transaction(&tx)
            .unwrap();

        let (subscriber, _id_receiver, mut transport_receiver) =
            Subscriber::new_test("programNotification");
        let sub_id = SubscriptionId::Number(0 as u64);
        let sink = subscriber.assign_id(sub_id.clone()).unwrap();
        let subscriptions = RpcSubscriptions::default();
        subscriptions.add_program_subscription(&morgan_bvm_script::id(), None, &sub_id, &sink);

        assert!(subscriptions
            .program_subscriptions
            .read()
            .unwrap()
            .contains_key(&morgan_bvm_script::id()));

        subscriptions.check_program(&morgan_bvm_script::id(), 0, &treasury_forks);
        let string = transport_receiver.poll();
        if let Async::Ready(Some(response)) = string.unwrap() {
            let expected = format!(r#"{{"jsonrpc":"2.0","method":"programNotification","params":{{"result":["{:?}",{{"data":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"difs":1,"executable":false,"owner":[2,203,81,223,225,24,34,35,203,214,138,130,144,208,35,77,63,16,87,51,47,198,115,123,98,188,19,160,0,0,0,0],"reputations":0}}],"subscription":0}}}}"#, alice.address());
            assert_eq!(expected, response);
        }

        subscriptions.remove_program_subscription(&sub_id);
        assert!(!subscriptions
            .program_subscriptions
            .read()
            .unwrap()
            .contains_key(&morgan_bvm_script::id()));
    }
    #[test]
    fn test_check_signature_subscribe() {
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(100);
        let treasury = Treasury::new(&genesis_block);
        let transaction_seal = treasury.last_transaction_seal();
        let treasury_forks = Arc::new(RwLock::new(TreasuryForks::new(0, treasury)));
        let alice = Keypair::new();
        let tx = sys_controller::transfer(&mint_keypair, &alice.address(), 20, transaction_seal);
        let signature = tx.signatures[0];
        treasury_forks
            .write()
            .unwrap()
            .get(0)
            .unwrap()
            .process_transaction(&tx)
            .unwrap();

        let (subscriber, _id_receiver, mut transport_receiver) =
            Subscriber::new_test("signatureNotification");
        let sub_id = SubscriptionId::Number(0 as u64);
        let sink = subscriber.assign_id(sub_id.clone()).unwrap();
        let subscriptions = RpcSubscriptions::default();
        subscriptions.add_signature_subscription(&signature, None, &sub_id, &sink);

        assert!(subscriptions
            .signature_subscriptions
            .read()
            .unwrap()
            .contains_key(&signature));

        subscriptions.check_signature(&signature, 0, &treasury_forks);
        let string = transport_receiver.poll();
        if let Async::Ready(Some(response)) = string.unwrap() {
            let expected_res: Option<transaction::Result<()>> = Some(Ok(()));
            let expected_res_str =
                serde_json::to_string(&serde_json::to_value(expected_res).unwrap()).unwrap();
            let expected = format!(r#"{{"jsonrpc":"2.0","method":"signatureNotification","params":{{"result":{},"subscription":0}}}}"#, expected_res_str);
            assert_eq!(expected, response);
        }

        subscriptions.remove_signature_subscription(&sub_id);
        assert!(!subscriptions
            .signature_subscriptions
            .read()
            .unwrap()
            .contains_key(&signature));
    }
}
