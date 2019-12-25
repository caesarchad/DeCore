//! The `nodesyncflow` module provides a method for streaming entries out via a
//! local unix socket, to provide client services such as a block explorer with
//! real-time access to entries.

use crate::fiscal_statement_info::FsclStmt;
use crate::result::Result;
use bincode::serialize;
use chrono::{SecondsFormat, Utc};
use serde_json::json;
use morgan_interface::hash::Hash;
use morgan_interface::pubkey::Pubkey;
use std::cell::RefCell;
use std::io::prelude::*;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::Path;
use log::*;
use morgan_helper::logHelper::*;
use std::{ops::Range, thread, time::Duration};

pub trait FiscSatementWriter: std::fmt::Debug {
    fn write(&self, payload: String) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct FiscStmtVector {
    values: RefCell<Vec<String>>,
}

impl FiscSatementWriter for FiscStmtVector {
    fn write(&self, payload: String) -> Result<()> {
        self.values.borrow_mut().push(payload);
        Ok(())
    }
}

impl FiscStmtVector {
    pub fn new() -> Self {
        FiscStmtVector {
            values: RefCell::new(Vec::new()),
        }
    }

    pub fn statements(&self) -> Vec<String> {
        self.values.borrow().clone()
    }
}

#[derive(Debug)]
pub struct FiscStmtCandidate {
    socket: String,
}

const MESSAGE_TERMINATOR: &str = "\n";

impl FiscSatementWriter for FiscStmtCandidate {
    fn write(&self, payload: String) -> Result<()> {
        let mut socket = UnixStream::connect(Path::new(&self.socket))?;
        socket.write_all(payload.as_bytes())?;
        socket.write_all(MESSAGE_TERMINATOR.as_bytes())?;
        socket.shutdown(Shutdown::Write)?;
        Ok(())
    }
}

pub trait NodeSyncEvents {
    fn emit_fscl_stmt_event(
        &self,
        slot: u64,
        drop_height: u64,
        leader_pubkey: &Pubkey,
        statements: &FsclStmt,
    ) -> Result<()>;
    fn emit_block_event(
        &self,
        slot: u64,
        drop_height: u64,
        leader_pubkey: &Pubkey,
        transaction_seal: Hash,
    ) -> Result<()>;
}

#[derive(Debug)]
pub struct NodeSyncFlow<T: FiscSatementWriter> {
    pub output: T,
}

pub const INTERFACE_CONNECT_ATTEMPTS_MAX: usize = 30;
pub const INTERFACE_CONNECT_INTERVAL: Duration = Duration::from_secs(1);

pub fn try_with_port<T, F: FnOnce() -> T>(port: u16, f: F) -> T {
    let mut attempts = 0;
    while attempts <= INTERFACE_CONNECT_ATTEMPTS_MAX {
        if port_is_available(port) {
            return f();
        }
        warn!(
            "Waiting for port {} to be available, sleeping (attempt #{})",
            port, attempts
        );
        thread::sleep(INTERFACE_CONNECT_INTERVAL);
        attempts += 1;
    }
    f()
}

pub fn port_is_available(port: u16) -> bool {
    use std::net::TcpListener;
    TcpListener::bind(format!("0.0.0.0:{}", port)).is_ok()
}

pub fn get_free_port(range: Range<u16>) -> Option<u16> {
    for i in range {
        if port_is_available(i) {
            return Some(i);
        }
    }
    None
}

impl<T> NodeSyncEvents for NodeSyncFlow<T>
where
    T: FiscSatementWriter,
{
    fn emit_fscl_stmt_event(
        &self,
        slot: u64,
        drop_height: u64,
        leader_pubkey: &Pubkey,
        stmt: &FsclStmt,
    ) -> Result<()> {
        let transactions: Vec<Vec<u8>> = serialize_transactions(stmt);
        let mut be_vote_tx = false;
        // ignore transaction's length entry
        if transactions.len() > 0 {
            // ignore entry.transaction.message's account_keys contain Vote111111111111111111111111111111111111111
            for tx in &stmt.transactions {
                for key in &tx.message.account_keys {
                    if key.to_string() == "Vote111111111111111111111111111111111111111" {
                        be_vote_tx = true;
                        break;
                    } 
                }
            }
            if be_vote_tx == false {
                let stmt_flow = json!({
                    "num_hashes": stmt.num_hashes,
                    "hash": stmt.hash,
                    "transactions": transactions
                });
                let json_entry = serde_json::to_string(&stmt_flow)?;
                let payload = format!(
                    r#"{{"dt":"{}","t":"entry","s":{},"h":{},"l":"{:?}","entry":{}}}"#,
                    Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true),
                    slot,
                    drop_height,
                    leader_pubkey,
                    json_entry,
                );
                // error!("{}", Error(format!("fscl stmt event: {:?}", entry).to_string()));
                println!(
                    "{}",
                    Error(
                        format!("fscl stmt event: {:?}", stmt).to_string(),
                        module_path!().to_string()
                    )
                );
                self.output.write(payload)?;
            }

        }
        Ok(())
    }

    fn emit_block_event(
        &self,
        slot: u64,
        drop_height: u64,
        leader_pubkey: &Pubkey,
        transaction_seal: Hash,
    ) -> Result<()> {
        let payload = format!(
            r#"{{"dt":"{}","t":"block","s":{},"h":{},"l":"{:?}","hash":"{:?}"}}"#,
            Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true),
            slot,
            drop_height,
            leader_pubkey,
            transaction_seal,
        );
        // error!("{}", Error(format!("block_event: {:?}", payload).to_string()));
        println!(
            "{}",
            Error(
                format!("block_event: {:?}", payload).to_string(),
                module_path!().to_string()
            )
        );
        self.output.write(payload)?;
        Ok(())
    }
}

pub type SocketNodeSync = NodeSyncFlow<FiscStmtCandidate>;

impl SocketNodeSync {
    pub fn new(socket: String) -> Self {
        NodeSyncFlow {
            output: FiscStmtCandidate { socket },
        }
    }
}

pub type FakeNodeSync = NodeSyncFlow<FiscStmtVector>;

impl FakeNodeSync {
    pub fn new(_: String) -> Self {
        NodeSyncFlow {
            output: FiscStmtVector::new(),
        }
    }

    pub fn statements(&self) -> Vec<String> {
        self.output.statements()
    }
}

fn serialize_transactions(entry: &FsclStmt) -> Vec<Vec<u8>> {
    entry
        .transactions
        .iter()
        .map(|tx| serialize(&tx).unwrap())
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::fiscal_statement_info::FsclStmt;
    use chrono::{DateTime, FixedOffset};
    use serde_json::Value;
    use morgan_interface::hash::Hash;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::system_transaction;
    use std::collections::HashSet;

    #[test]
    fn test_serialize_transactions() {
        let entry = FsclStmt::new(&Hash::default(), 1, vec![]);
        let empty_vec: Vec<Vec<u8>> = vec![];
        assert_eq!(serialize_transactions(&entry), empty_vec);

        let keypair0 = Keypair::new();
        let keypair1 = Keypair::new();
        let tx0 = system_transaction::transfer(&keypair0, &keypair1.pubkey(), 1, Hash::default());
        let tx1 = system_transaction::transfer(&keypair1, &keypair0.pubkey(), 2, Hash::default());
        let serialized_tx0 = serialize(&tx0).unwrap();
        let serialized_tx1 = serialize(&tx1).unwrap();
        let entry = FsclStmt::new(&Hash::default(), 1, vec![tx0, tx1]);
        assert_eq!(
            serialize_transactions(&entry),
            vec![serialized_tx0, serialized_tx1]
        );
    }

    #[test]
    fn test_blockstream() -> () {
        let nodesyncflow = FakeNodeSync::new("test_stream".to_string());
        let drops_per_slot = 5;

        let mut transaction_seal = Hash::default();
        let mut statements = Vec::new();
        let mut expected_entries = Vec::new();

        let drop_height_initial = 0;
        let drop_height_final = drop_height_initial + drops_per_slot + 2;
        let mut curr_slot = 0;
        let leader_pubkey = Pubkey::new_rand();

        for drop_height in drop_height_initial..=drop_height_final {
            if drop_height == 5 {
                nodesyncflow
                    .emit_block_event(curr_slot, drop_height - 1, &leader_pubkey, transaction_seal)
                    .unwrap();
                curr_slot += 1;
            }
            let entry = FsclStmt::new(&mut transaction_seal, 1, vec![]); // just drops
            transaction_seal = entry.hash;
            nodesyncflow
                .emit_fscl_stmt_event(curr_slot, drop_height, &leader_pubkey, &entry)
                .unwrap();
            expected_entries.push(entry.clone());
            statements.push(entry);
        }

        assert_eq!(
            nodesyncflow.statements().len() as u64,
            // one entry per drop (0..=N+2) is +3, plus one block
            drops_per_slot + 3 + 1
        );

        let mut j = 0;
        let mut matched_entries = 0;
        let mut matched_slots = HashSet::new();
        let mut matched_blocks = HashSet::new();

        for item in nodesyncflow.statements() {
            let json: Value = serde_json::from_str(&item).unwrap();
            let dt_str = json["dt"].as_str().unwrap();

            // Ensure `ts` field parses as valid DateTime
            let _dt: DateTime<FixedOffset> = DateTime::parse_from_rfc3339(dt_str).unwrap();

            let item_type = json["t"].as_str().unwrap();
            match item_type {
                "block" => {
                    let hash = json["hash"].to_string();
                    matched_blocks.insert(hash);
                }

                "entry" => {
                    let slot = json["s"].as_u64().unwrap();
                    matched_slots.insert(slot);
                    let entry_obj = json["entry"].clone();
                    let entry: FsclStmt = serde_json::from_value(entry_obj).unwrap();

                    assert_eq!(entry, expected_entries[j]);
                    matched_entries += 1;
                    j += 1;
                }

                _ => {
                    assert!(false, "unknown item type {}", item);
                }
            }
        }

        assert_eq!(matched_entries, expected_entries.len());
        assert_eq!(matched_slots.len(), 2);
        assert_eq!(matched_blocks.len(), 1);
    }
}
