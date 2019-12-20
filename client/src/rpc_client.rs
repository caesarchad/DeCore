use crate::account_host_err::ClientError;
use crate::rpc_client_request::GenericRpcClientRequest;
use crate::rpc_client_request::MockRpcClientRequest;
use crate::rpc_client_request::RpcClientRequest;
use crate::rpc_request::RpcRequest;
use bincode::serialize;
use log::*;
use serde_json::{json, Value};
use morgan_interface::account::Account;
use morgan_interface::fee_calculator::FeeCalculator;
use morgan_interface::hash::Hash;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::signature::{KeypairUtil, Signature};
use morgan_interface::timing::{DEFAULT_NUM_DROPS_PER_SECOND, DEFAULT_DROPS_PER_SLOT};
use morgan_interface::transaction::{self, Transaction, TransactionError};
use std::error;
use std::io;
use std::net::SocketAddr;
use std::thread::sleep;
use std::time::{Duration, Instant};
use ansi_term::Color::{Green};
use morgan_helper::logHelper::*;

pub struct RpcClient {
    client: Box<GenericRpcClientRequest + Send + Sync>,
}

impl RpcClient {
    pub fn new(url: String) -> Self {
        Self {
            client: Box::new(RpcClientRequest::new(url)),
        }
    }

    pub fn new_mock(url: String) -> Self {
        Self {
            client: Box::new(MockRpcClientRequest::new(url)),
        }
    }

    pub fn new_socket(addr: SocketAddr) -> Self {
        Self::new(get_rpc_request_str(addr, false))
    }

    pub fn new_socket_with_timeout(addr: SocketAddr, timeout: Duration) -> Self {
        let url = get_rpc_request_str(addr, false);
        Self {
            client: Box::new(RpcClientRequest::new_with_timeout(url, timeout)),
        }
    }

    pub fn send_transaction(&self, transaction: &Transaction) -> Result<String, ClientError> {
        let serialized = serialize(transaction).unwrap();
        let params = json!([serialized]);
        let signature = self
            .client
            .send(&RpcRequest::SendTransaction, Some(params), 5)?;
        if signature.as_str().is_none() {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Received result of an unexpected type",
            ))?;
        }
        Ok(signature.as_str().unwrap().to_string())
    }

    pub fn get_signature_status(
        &self,
        signature: &str,
    ) -> Result<Option<transaction::Result<()>>, ClientError> {
        let params = json!([signature.to_string()]);
        let signature_status =
            self.client
                .send(&RpcRequest::GetSignatureStatus, Some(params), 5)?;
        let result: Option<transaction::Result<()>> =
            serde_json::from_value(signature_status).unwrap();
        Ok(result)
    }

    pub fn send_and_confirm_transaction<T: KeypairUtil>(
        &self,
        transaction: &mut Transaction,
        signer_keys: &[&T],
    ) -> Result<String, ClientError> {
        let mut send_retries = 5;
        loop {
            let mut status_retries = 4;
            let signature_str = self.send_transaction(transaction)?;
            let status = loop {
                let status = self.get_signature_status(&signature_str)?;
                if status.is_none() {
                    status_retries -= 1;
                    if status_retries == 0 {
                        break status;
                    }
                } else {
                    break status;
                }
                if cfg!(not(test)) {
                    // Retry ~twice during a slot
                    sleep(Duration::from_millis(
                        500 * DEFAULT_DROPS_PER_SLOT / DEFAULT_NUM_DROPS_PER_SECOND,
                    ));
                }
            };
            send_retries = if let Some(result) = status.clone() {
                match result {
                    Ok(_) => return Ok(signature_str),
                    Err(TransactionError::AccountInUse) => {
                        // Fetch a new transaction_seal and re-sign the transaction before sending it again
                        self.resign_transaction(transaction, signer_keys)?;
                        send_retries - 1
                    }
                    Err(_) => 0,
                }
            } else {
                send_retries - 1
            };
            if send_retries == 0 {
                if status.is_some() {
                    status.unwrap()?
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Transaction {:?} failed: {:?}", signature_str, status),
                    ))?;
                }
            }
        }
    }

    pub fn send_and_confirm_transactions<T: KeypairUtil>(
        &self,
        mut transactions: Vec<Transaction>,
        signer_keys: &[&T],
    ) -> Result<(), Box<dyn error::Error>> {
        let mut send_retries = 5;
        loop {
            let mut status_retries = 4;

            // Send all transactions
            let mut transactions_signatures = vec![];
            for transaction in transactions {
                if cfg!(not(test)) {
                    // Delay ~1 _drop between write transactions in an attempt to reduce AccountInUse errors
                    // when all the write transactions modify the same program account (eg, deploying a
                    // new program)
                    sleep(Duration::from_millis(1000 / DEFAULT_NUM_DROPS_PER_SECOND));
                }

                let signature = self.send_transaction(&transaction).ok();
                transactions_signatures.push((transaction, signature))
            }

            // Collect statuses for all the transactions, drop those that are confirmed
            while status_retries > 0 {
                status_retries -= 1;

                if cfg!(not(test)) {
                    // Retry ~twice during a slot
                    sleep(Duration::from_millis(
                        500 * DEFAULT_DROPS_PER_SLOT / DEFAULT_NUM_DROPS_PER_SECOND,
                    ));
                }

                transactions_signatures = transactions_signatures
                    .into_iter()
                    .filter(|(_transaction, signature)| {
                        if let Some(signature) = signature {
                            if let Ok(status) = self.get_signature_status(&signature) {
                                if status.is_none() {
                                    return false;
                                }
                                return status.unwrap().is_err();
                            }
                        }
                        true
                    })
                    .collect();

                if transactions_signatures.is_empty() {
                    return Ok(());
                }
            }

            if send_retries == 0 {
                Err(io::Error::new(io::ErrorKind::Other, "Transactions failed"))?;
            }
            send_retries -= 1;

            // Re-sign any failed transactions with a new transaction_seal and retry
            let (transaction_seal, _fee_calculator) =
                self.get_new_transaction_seal(&transactions_signatures[0].0.message().recent_transaction_seal)?;
            transactions = transactions_signatures
                .into_iter()
                .map(|(mut transaction, _)| {
                    transaction.sign(signer_keys, transaction_seal);
                    transaction
                })
                .collect();
        }
    }

    pub fn resign_transaction<T: KeypairUtil>(
        &self,
        tx: &mut Transaction,
        signer_keys: &[&T],
    ) -> Result<(), ClientError> {
        let (transaction_seal, _fee_calculator) =
            self.get_new_transaction_seal(&tx.message().recent_transaction_seal)?;
        tx.sign(signer_keys, transaction_seal);
        Ok(())
    }

    pub fn retry_get_balance(
        &self,
        pubkey: &Pubkey,
        retries: usize,
    ) -> Result<Option<u64>, Box<dyn error::Error>> {
        let params = json!([format!("{}", pubkey)]);
        let res = self
            .client
            .send(&RpcRequest::GetBalance, Some(params), retries)?
            .as_u64();
        Ok(res)
    }

    pub fn get_account(&self, pubkey: &Pubkey) -> io::Result<Account> {
        let params = json!([format!("{}", pubkey)]);
        let response = self
            .client
            .send(&RpcRequest::GetAccountInfo, Some(params), 0);

        response
            .and_then(|account_json| {
                let account: Account =
                    serde_json::from_value(account_json).expect("deserialize account");
                trace!("Response account {:?} {:?}", pubkey, account);
                Ok(account)
            })
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("AccountNotFound: pubkey={}: {}", pubkey, err),
                )
            })
    }

    pub fn get_account_data(&self, pubkey: &Pubkey) -> io::Result<Vec<u8>> {
        self.get_account(pubkey).map(|account| account.data)
    }

    /// Request the balance of the user holding `pubkey`. This method blocks
    /// until the server sends a response. If the response packet is dropped
    /// by the network, this method will hang indefinitely.
    pub fn get_balance(&self, pubkey: &Pubkey) -> io::Result<u64> {
        self.get_account(pubkey).map(|account| account.difs)
    }

    /// Request the transaction count.  If the response packet is dropped by the network,
    /// this method will try again 5 times.
    pub fn get_transaction_count(&self) -> io::Result<u64> {
        let response = self
            .client
            .send(&RpcRequest::GetTransactionCount, None, 0)
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("GetTransactionCount request failure: {:?}", err),
                )
            })?;

        serde_json::from_value(response).map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("GetTransactionCount parse failure: {}", err),
            )
        })
    }

    pub fn get_recent_transaction_seal(&self) -> io::Result<(Hash, FeeCalculator)> {
        let response = self
            .client
            .send(&RpcRequest::GetRecentTransactionSeal, None, 0)
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("GetRecentTransactionSeal request failure: {:?}", err),
                )
            })?;

        let (transaction_seal, fee_calculator) =
            serde_json::from_value::<(String, FeeCalculator)>(response).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("GetRecentTransactionSeal parse failure: {:?}", err),
                )
            })?;

        let transaction_seal = transaction_seal.parse().map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("GetRecentTransactionSeal parse failure: {:?}", err),
            )
        })?;
        Ok((transaction_seal, fee_calculator))
    }

    pub fn get_new_transaction_seal(&self, transaction_seal: &Hash) -> io::Result<(Hash, FeeCalculator)> {
        let mut num_retries = 10;
        while num_retries > 0 {
            if let Ok((new_transaction_seal, fee_calculator)) = self.get_recent_transaction_seal() {
                if new_transaction_seal != *transaction_seal {
                    return Ok((new_transaction_seal, fee_calculator));
                }
            }
            debug!("Got same transaction_seal ({:?}), will retry...", transaction_seal);

            // Retry ~twice during a slot
            sleep(Duration::from_millis(
                500 * DEFAULT_DROPS_PER_SLOT / DEFAULT_NUM_DROPS_PER_SECOND,
            ));
            num_retries -= 1;
        }
        Err(io::Error::new(
            io::ErrorKind::Other,
            "Unable to get new transaction_seal, too many retries",
        ))
    }

    pub fn poll_balance_with_timeout(
        &self,
        pubkey: &Pubkey,
        polling_frequency: &Duration,
        timeout: &Duration,
    ) -> io::Result<u64> {
        let now = Instant::now();
        loop {
            match self.get_balance(&pubkey) {
                Ok(bal) => {
                    return Ok(bal);
                }
                Err(e) => {
                    sleep(*polling_frequency);
                    if now.elapsed() > *timeout {
                        return Err(e);
                    }
                }
            };
        }
    }

    pub fn poll_get_balance(&self, pubkey: &Pubkey) -> io::Result<u64> {
        self.poll_balance_with_timeout(pubkey, &Duration::from_millis(100), &Duration::from_secs(1))
    }

    pub fn wait_for_balance(&self, pubkey: &Pubkey, expected_balance: Option<u64>) -> Option<u64> {
        const LAST: usize = 30;
        for run in 0..LAST {
            let balance_result = self.poll_get_balance(pubkey);
            if expected_balance.is_none() {
                return balance_result.ok();
            }
            trace!(
                "retry_get_balance[{}] {:?} {:?}",
                run,
                balance_result,
                expected_balance
            );
            if let (Some(expected_balance), Ok(balance_result)) = (expected_balance, balance_result)
            {
                if expected_balance == balance_result {
                    return Some(balance_result);
                }
            }
        }
        None
    }

    /// Poll the server to confirm a transaction.
    pub fn poll_for_signature(&self, signature: &Signature) -> io::Result<()> {
        let now = Instant::now();
        while !self.check_signature(signature) {
            if now.elapsed().as_secs() > 15 {
                // TODO: Return a better error.
                return Err(io::Error::new(io::ErrorKind::Other, "signature not found"));
            }
            sleep(Duration::from_millis(250));
        }
        Ok(())
    }

    /// Check a signature in the treasury.
    pub fn check_signature(&self, signature: &Signature) -> bool {
        trace!("check_signature: {:?}", signature);
        let params = json!([format!("{}", signature)]);

        for _ in 0..30 {
            let response =
                self.client
                    .send(&RpcRequest::ConfirmTransaction, Some(params.clone()), 0);

            match response {
                Ok(confirmation) => {
                    let signature_status = confirmation.as_bool().unwrap();
                    if signature_status {
                        trace!("Response found signature");
                    } else {
                        trace!("Response signature not found");
                    }

                    return signature_status;
                }
                Err(err) => {
                    debug!("check_signature request failed: {:?}", err);
                }
            };
            sleep(Duration::from_millis(250));
        }

        panic!("Couldn't check signature of {}", signature);
    }

    /// Poll the server to confirm a transaction.
    pub fn poll_for_signature_confirmation(
        &self,
        signature: &Signature,
        min_confirmed_blocks: usize,
    ) -> io::Result<()> {
        let mut now = Instant::now();
        let mut confirmed_blocks = 0;
        loop {
            let response = self.get_num_blocks_since_signature_confirmation(signature);
            match response {
                Ok(count) => {
                    if confirmed_blocks != count {
                        // info!(
                        //     "{}",
                        //     Info(format!(
                        //         "signature {} confirmed {} out of {}",
                        //         signature, count, min_confirmed_blocks).to_string()
                        //     )
                        // );
                        let info:String = format!(
                            "signature {} confirmed {} out of {}",
                            signature, count, min_confirmed_blocks).to_string();
                        println!("{}",
                            printLn(
                                info,
                                module_path!().to_string()
                            )
                        );

                        now = Instant::now();
                        confirmed_blocks = count;
                    }
                    if count >= min_confirmed_blocks {
                        break;
                    }
                }
                Err(err) => {
                    debug!("check_confirmations request failed: {:?}", err);
                }
            };
            if now.elapsed().as_secs() > 15 {
                // TODO: Return a better error.
                return Err(io::Error::new(io::ErrorKind::Other, "signature not found"));
            }
            sleep(Duration::from_millis(250));
        }
        Ok(())
    }

    pub fn get_num_blocks_since_signature_confirmation(
        &self,
        sig: &Signature,
    ) -> io::Result<usize> {
        let params = json!([format!("{}", sig)]);
        let response = self
            .client
            .send(
                &RpcRequest::GetNumBlocksSinceSignatureConfirmation,
                Some(params.clone()),
                1,
            )
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "GetNumBlocksSinceSignatureConfirmation request failure: {}",
                        err
                    ),
                )
            })?;
        serde_json::from_value(response).map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "GetNumBlocksSinceSignatureConfirmation parse failure: {}",
                    err
                ),
            )
        })
    }

    pub fn fullnode_exit(&self) -> io::Result<bool> {
        let response = self
            .client
            .send(&RpcRequest::FullnodeExit, None, 0)
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("FullnodeExit request failure: {:?}", err),
                )
            })?;
        serde_json::from_value(response).map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("FullnodeExit parse failure: {:?}", err),
            )
        })
    }

    // TODO: Remove
    pub fn retry_make_rpc_request(
        &self,
        request: &RpcRequest,
        params: Option<Value>,
        retries: usize,
    ) -> Result<Value, ClientError> {
        self.client.send(request, params, retries)
    }
}

pub fn get_rpc_request_str(rpc_addr: SocketAddr, tls: bool) -> String {
    if tls {
        format!("https://{}", rpc_addr)
    } else {
        format!("http://{}", rpc_addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_client_request::{PUBKEY, SIGNATURE};
    use jsonrpc_core::{Error, IoHandler, Params};
    use jsonrpc_http_server::{AccessControlAllowOrigin, DomainsValidation, ServerBuilder};
    use serde_json::Number;
    use morgan_logger;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::system_transaction;
    use morgan_interface::transaction::TransactionError;
    use std::sync::mpsc::channel;
    use std::thread;

    #[test]
    fn test_make_rpc_request() {
        let (sender, receiver) = channel();
        thread::spawn(move || {
            let rpc_addr = "0.0.0.0:0".parse().unwrap();
            let mut io = IoHandler::default();
            // Successful request
            io.add_method("getDif", |_params: Params| {
                Ok(Value::Number(Number::from(50)))
            });
            // Failed request
            io.add_method("getLatestTransactionSeal", |params: Params| {
                if params != Params::None {
                    Err(Error::invalid_request())
                } else {
                    Ok(Value::String(
                        "deadbeefXjn8o3yroDHxUtKsZZgoy4GPkPPXfouKNHhx".to_string(),
                    ))
                }
            });

            let server = ServerBuilder::new(io)
                .threads(1)
                .cors(DomainsValidation::AllowOnly(vec![
                    AccessControlAllowOrigin::Any,
                ]))
                .start_http(&rpc_addr)
                .expect("Unable to start RPC server");
            sender.send(*server.address()).unwrap();
            server.wait();
        });

        let rpc_addr = receiver.recv().unwrap();
        let rpc_client = RpcClient::new_socket(rpc_addr);

        let balance = rpc_client.retry_make_rpc_request(
            &RpcRequest::GetBalance,
            Some(json!(["deadbeefXjn8o3yroDHxUtKsZZgoy4GPkPPXfouKNHhx"])),
            0,
        );
        assert_eq!(balance.unwrap().as_u64().unwrap(), 50);

        let transaction_seal = rpc_client.retry_make_rpc_request(&RpcRequest::GetRecentTransactionSeal, None, 0);
        assert_eq!(
            transaction_seal.unwrap().as_str().unwrap(),
            "deadbeefXjn8o3yroDHxUtKsZZgoy4GPkPPXfouKNHhx"
        );

        // Send erroneous parameter
        let transaction_seal = rpc_client.retry_make_rpc_request(
            &RpcRequest::GetRecentTransactionSeal,
            Some(json!("parameter")),
            0,
        );
        assert_eq!(transaction_seal.is_err(), true);
    }

    #[test]
    fn test_retry_make_rpc_request() {
        morgan_logger::setup();
        let (sender, receiver) = channel();
        thread::spawn(move || {
            // 1. Pick a random port
            // 2. Tell the client to start using it
            // 3. Delay for 1.5 seconds before starting the server to ensure the client will fail
            //    and need to retry
            let rpc_addr: SocketAddr = "0.0.0.0:4242".parse().unwrap();
            sender.send(rpc_addr.clone()).unwrap();
            sleep(Duration::from_millis(1500));

            let mut io = IoHandler::default();
            io.add_method("getDif", move |_params: Params| {
                Ok(Value::Number(Number::from(5)))
            });
            let server = ServerBuilder::new(io)
                .threads(1)
                .cors(DomainsValidation::AllowOnly(vec![
                    AccessControlAllowOrigin::Any,
                ]))
                .start_http(&rpc_addr)
                .expect("Unable to start RPC server");
            server.wait();
        });

        let rpc_addr = receiver.recv().unwrap();
        let rpc_client = RpcClient::new_socket(rpc_addr);

        let balance = rpc_client.retry_make_rpc_request(
            &RpcRequest::GetBalance,
            Some(json!(["deadbeefXjn8o3yroDHxUtKsZZgoy4GPkPPXfouKNHhw"])),
            10,
        );
        assert_eq!(balance.unwrap().as_u64().unwrap(), 5);
    }

    #[test]
    fn test_send_transaction() {
        let rpc_client = RpcClient::new_mock("succeeds".to_string());

        let key = Keypair::new();
        let to = Pubkey::new_rand();
        let transaction_seal = Hash::default();
        let tx = system_transaction::create_user_account(&key, &to, 50, transaction_seal);

        let signature = rpc_client.send_transaction(&tx);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        let rpc_client = RpcClient::new_mock("fails".to_string());

        let signature = rpc_client.send_transaction(&tx);
        assert!(signature.is_err());
    }
    #[test]
    fn test_get_recent_transaction_seal() {
        let rpc_client = RpcClient::new_mock("succeeds".to_string());

        let expected_transaction_seal: Hash = PUBKEY.parse().unwrap();

        let (transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal().expect("transaction_seal ok");
        assert_eq!(transaction_seal, expected_transaction_seal);

        let rpc_client = RpcClient::new_mock("fails".to_string());

        assert!(rpc_client.get_recent_transaction_seal().is_err());
    }

    #[test]
    fn test_get_signature_status() {
        let rpc_client = RpcClient::new_mock("succeeds".to_string());
        let signature = "good_signature";
        let status = rpc_client.get_signature_status(&signature).unwrap();
        assert_eq!(status, Some(Ok(())));

        let rpc_client = RpcClient::new_mock("sig_not_found".to_string());
        let signature = "sig_not_found";
        let status = rpc_client.get_signature_status(&signature).unwrap();
        assert_eq!(status, None);

        let rpc_client = RpcClient::new_mock("account_in_use".to_string());
        let signature = "account_in_use";
        let status = rpc_client.get_signature_status(&signature).unwrap();
        assert_eq!(status, Some(Err(TransactionError::AccountInUse)));
    }

    #[test]
    fn test_send_and_confirm_transaction() {
        let rpc_client = RpcClient::new_mock("succeeds".to_string());

        let key = Keypair::new();
        let to = Pubkey::new_rand();
        let transaction_seal = Hash::default();
        let mut tx = system_transaction::create_user_account(&key, &to, 50, transaction_seal);

        let result = rpc_client.send_and_confirm_transaction(&mut tx, &[&key]);
        result.unwrap();

        let rpc_client = RpcClient::new_mock("account_in_use".to_string());
        let result = rpc_client.send_and_confirm_transaction(&mut tx, &[&key]);
        assert!(result.is_err());

        let rpc_client = RpcClient::new_mock("fails".to_string());
        let result = rpc_client.send_and_confirm_transaction(&mut tx, &[&key]);
        assert!(result.is_err());
    }

    #[test]
    fn test_resign_transaction() {
        let rpc_client = RpcClient::new_mock("succeeds".to_string());

        let key = Keypair::new();
        let to = Pubkey::new_rand();
        let transaction_seal: Hash = "HUu3LwEzGRsUkuJS121jzkPJW39Kq62pXCTmTa1F9jDL"
            .parse()
            .unwrap();
        let prev_tx = system_transaction::create_user_account(&key, &to, 50, transaction_seal);
        let mut tx = system_transaction::create_user_account(&key, &to, 50, transaction_seal);

        rpc_client.resign_transaction(&mut tx, &[&key]).unwrap();

        assert_ne!(prev_tx, tx);
        assert_ne!(prev_tx.signatures, tx.signatures);
        assert_ne!(
            prev_tx.message().recent_transaction_seal,
            tx.message().recent_transaction_seal
        );
    }

    #[test]
    fn test_rpc_client_thread() {
        let rpc_client = RpcClient::new_mock("succeeds".to_string());
        thread::spawn(move || rpc_client);
    }
}
