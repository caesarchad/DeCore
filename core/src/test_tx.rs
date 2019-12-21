use morgan_interface::hash::Hash;
use morgan_interface::opcodes::EncodedOpCodes;
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_interface::sys_opcode::SysOpCode;
use morgan_interface::system_program;
use morgan_interface::system_transaction;
use morgan_interface::transaction::Transaction;

pub fn test_tx() -> Transaction {
    let keypair1 = Keypair::new();
    let pubkey1 = keypair1.pubkey();
    let zero = Hash::default();
    system_transaction::create_user_account(&keypair1, &pubkey1, 42, zero)
}

pub fn test_multisig_tx() -> Transaction {
    let keypair0 = Keypair::new();
    let keypair1 = Keypair::new();
    let keypairs = vec![&keypair0, &keypair1];
    let difs = 5;
    let transaction_seal = Hash::default();

    let transfer_instruction = SysOpCode::Transfer { difs };

    let program_ids = vec![system_program::id(), morgan_budget_api::id()];

    let instructions = vec![EncodedOpCodes::new(
        0,
        &transfer_instruction,
        vec![0, 1],
    )];

    Transaction::new_with_encoded_opcodes(
        &keypairs,
        &[],
        transaction_seal,
        program_ids,
        instructions,
    )
}

use std::{thread, time::Duration};

/// Given an operation retries it successfully sleeping everytime it fails
/// If the operation succeeds before the iterator runs out, it returns success
pub fn retry<I, O, T, E>(iterable: I, mut operation: O) -> Result<T, E>
where
    I: IntoIterator<Item = Duration>,
    O: FnMut() -> Result<T, E>,
{
    let mut iterator = iterable.into_iter();
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(err) => {
                if let Some(delay) = iterator.next() {
                    thread::sleep(delay);
                } else {
                    return Err(err);
                }
            }
        }
    }
}

pub fn fixed_retry_strategy(delay_ms: u64, tries: usize) -> impl Iterator<Item = Duration> {
    FixedDelay::new(delay_ms).take(tries)
}

/// An iterator which uses a fixed delay
pub struct FixedDelay {
    duration: Duration,
}

impl FixedDelay {
    /// Create a new `FixedDelay` using the given duration in milliseconds.
    fn new(millis: u64) -> Self {
        FixedDelay {
            duration: Duration::from_millis(millis),
        }
    }
}

impl Iterator for FixedDelay {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        Some(self.duration)
    }
}
