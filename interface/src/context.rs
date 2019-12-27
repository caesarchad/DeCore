//! A library for generating a message from a sequence of instructions

use crate::hash::Hash;
use crate::opcodes::{AccountMeta, EncodedOpCodes, OpCode};
use crate::bvm_address::BvmAddr;
use crate::short_vec;
use itertools::Itertools;

fn position(keys: &[BvmAddr], key: &BvmAddr) -> u8 {
    keys.iter().position(|k| k == key).unwrap() as u8
}

fn encode_opcode(ix:OpCode,keys: &[BvmAddr]) -> EncodedOpCodes {
    let accounts: Vec<_> = ix
        .accounts
        .iter()
        .map(|account_meta| position(keys, &account_meta.pubkey))
        .collect();

    EncodedOpCodes {
        program_ids_index: position(keys, &ix.program_ids_index),
        data: ix.data.clone(),
        accounts,
    }
}

fn encode_multiple_opcodes(ixs: Vec<OpCode>, keys: &[BvmAddr]) -> Vec<EncodedOpCodes> {
    ixs.into_iter()
        .map(|ix| encode_opcode(ix, keys))
        .collect()
}

/// A helper struct to collect pubkeys referenced by a set of instructions and credit-only counts
#[derive(Debug, PartialEq, Eq)]
struct OpCodeKeys {
    pub signed_keys: Vec<BvmAddr>,
    pub unsigned_keys: Vec<BvmAddr>,
    pub num_credit_only_signed_accounts: u8,
    pub num_credit_only_unsigned_accounts: u8,
}

impl OpCodeKeys {
    fn new(
        signed_keys: Vec<BvmAddr>,
        unsigned_keys: Vec<BvmAddr>,
        num_credit_only_signed_accounts: u8,
        num_credit_only_unsigned_accounts: u8,
    ) -> Self {
        Self {
            signed_keys,
            unsigned_keys,
            num_credit_only_signed_accounts,
            num_credit_only_unsigned_accounts,
        }
    }
}

/// Return pubkeys referenced by all instructions, with the ones needing signatures first. If the
/// payer key is provided, it is always placed first in the list of signed keys. Credit-only signed
/// accounts are placed last in the set of signed accounts. Credit-only unsigned accounts,
/// including program ids, are placed last in the set. No duplicates and order is preserved.
fn get_keys(instructions: &[OpCode], payer: Option<&BvmAddr>) -> OpCodeKeys {
    let programs: Vec<_> = get_program_ids(instructions)
        .iter()
        .map(|program_id| AccountMeta {
            pubkey: *program_id,
            is_signer: false,
            is_debitable: false,
        })
        .collect();
    let mut keys_and_signed: Vec<_> = instructions
        .iter()
        .flat_map(|ix| ix.accounts.iter())
        .collect();
    keys_and_signed.extend(&programs);
    keys_and_signed.sort_by(|x, y| {
        y.is_signer
            .cmp(&x.is_signer)
            .then(y.is_debitable.cmp(&x.is_debitable))
    });

    let payer_account_meta;
    if let Some(payer) = payer {
        payer_account_meta = AccountMeta {
            pubkey: *payer,
            is_signer: true,
            is_debitable: true,
        };
        keys_and_signed.insert(0, &payer_account_meta);
    }

    let mut signed_keys = vec![];
    let mut unsigned_keys = vec![];
    let mut num_credit_only_signed_accounts = 0;
    let mut num_credit_only_unsigned_accounts = 0;
    for account_meta in keys_and_signed.into_iter().unique_by(|x| x.pubkey) {
        if account_meta.is_signer {
            signed_keys.push(account_meta.pubkey);
            if !account_meta.is_debitable {
                num_credit_only_signed_accounts += 1;
            }
        } else {
            unsigned_keys.push(account_meta.pubkey);
            if !account_meta.is_debitable {
                num_credit_only_unsigned_accounts += 1;
            }
        }
    }
    OpCodeKeys::new(
        signed_keys,
        unsigned_keys,
        num_credit_only_signed_accounts,
        num_credit_only_unsigned_accounts,
    )
}

/// Return program ids referenced by all instructions.  No duplicates and order is preserved.
fn get_program_ids(instructions: &[OpCode]) -> Vec<BvmAddr> {
    instructions
        .iter()
        .map(|ix| ix.program_ids_index)
        .unique()
        .collect()
}

#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq, Clone)]
pub struct MessageHeader {
    /// The number of signatures required for this message to be considered valid. The
    /// signatures must match the first `num_required_signatures` of `account_keys`.
    pub num_required_signatures: u8,

    /// The last num_credit_only_signed_accounts of the signed keys are credit-only accounts.
    /// Programs may process multiple transactions that add difs to the same credit-only
    /// account within a single Water Clock entry, but are not permitted to debit difs or modify
    /// account data. Transactions targeting the same debit account are evaluated sequentially.
    pub num_credit_only_signed_accounts: u8,

    /// The last num_credit_only_unsigned_accounts of the unsigned keys are credit-only accounts.
    pub num_credit_only_unsigned_accounts: u8,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Context {
    /// The message header, identifying signed and credit-only `account_keys`
    pub header: MessageHeader,

    /// All the account keys used by this transaction
    #[serde(with = "short_vec")]
    pub account_keys: Vec<BvmAddr>,

    /// The id of a recent ledger entry.
    pub recent_transaction_seal: Hash,

    /// Programs that will be executed in sequence and committed in one atomic transaction if all
    /// succeed.
    #[serde(with = "short_vec")]
    pub instructions: Vec<EncodedOpCodes>,
}

impl Context {
    pub fn new_with_encoded_opcodes(
        num_required_signatures: u8,
        num_credit_only_signed_accounts: u8,
        num_credit_only_unsigned_accounts: u8,
        account_keys: Vec<BvmAddr>,
        recent_transaction_seal: Hash,
        instructions: Vec<EncodedOpCodes>,
    ) -> Self {
        Self {
            header: MessageHeader {
                num_required_signatures,
                num_credit_only_signed_accounts,
                num_credit_only_unsigned_accounts,
            },
            account_keys,
            recent_transaction_seal,
            instructions,
        }
    }

    pub fn new(instructions: Vec<OpCode>) -> Self {
        Self::new_with_payer(instructions, None)
    }

    pub fn new_with_payer(instructions: Vec<OpCode>, payer: Option<&BvmAddr>) -> Self {
        let OpCodeKeys {
            mut signed_keys,
            unsigned_keys,
            num_credit_only_signed_accounts,
            num_credit_only_unsigned_accounts,
        } = get_keys(&instructions, payer);
        let num_required_signatures = signed_keys.len() as u8;
        signed_keys.extend(&unsigned_keys);
        let instructions = encode_multiple_opcodes(instructions, &signed_keys);
        Self::new_with_encoded_opcodes(
            num_required_signatures,
            num_credit_only_signed_accounts,
            num_credit_only_unsigned_accounts,
            signed_keys,
            Hash::default(),
            instructions,
        )
    }

    pub fn program_ids(&self) -> Vec<&BvmAddr> {
        self.instructions
            .iter()
            .map(|ix| &self.account_keys[ix.program_ids_index as usize])
            .collect()
    }

    pub fn program_position(&self, index: usize) -> Option<usize> {
        let program_ids = self.program_ids();
        program_ids
            .iter()
            .position(|&&pubkey| pubkey == self.account_keys[index])
    }

    fn is_credit_debit(&self, i: usize) -> bool {
        i < (self.header.num_required_signatures - self.header.num_credit_only_signed_accounts)
            as usize
            || (i >= self.header.num_required_signatures as usize
                && i < self.account_keys.len()
                    - self.header.num_credit_only_unsigned_accounts as usize)
    }

    pub fn get_account_keys_by_lock_type(&self) -> (Vec<&BvmAddr>, Vec<&BvmAddr>) {
        let mut credit_debit_keys = vec![];
        let mut credit_only_keys = vec![];
        for (i, key) in self.account_keys.iter().enumerate() {
            if self.is_credit_debit(i) {
                credit_debit_keys.push(key);
            } else {
                credit_only_keys.push(key);
            }
        }
        (credit_debit_keys, credit_only_keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::opcodes::AccountMeta;
    use crate::signature::{Keypair, KeypairUtil};

    #[test]
    fn test_message_unique_program_ids() {
        let program_id0 = BvmAddr::default();
        let program_ids = get_program_ids(&[
            OpCode::new(program_id0, &0, vec![]),
            OpCode::new(program_id0, &0, vec![]),
        ]);
        assert_eq!(program_ids, vec![program_id0]);
    }

    #[test]
    fn test_message_unique_program_ids_not_adjacent() {
        let program_id0 = BvmAddr::default();
        let program_id1 = BvmAddr::new_rand();
        let program_ids = get_program_ids(&[
            OpCode::new(program_id0, &0, vec![]),
            OpCode::new(program_id1, &0, vec![]),
            OpCode::new(program_id0, &0, vec![]),
        ]);
        assert_eq!(program_ids, vec![program_id0, program_id1]);
    }

    #[test]
    fn test_message_unique_program_ids_order_preserved() {
        let program_id0 = BvmAddr::new_rand();
        let program_id1 = BvmAddr::default(); // Key less than program_id0
        let program_ids = get_program_ids(&[
            OpCode::new(program_id0, &0, vec![]),
            OpCode::new(program_id1, &0, vec![]),
            OpCode::new(program_id0, &0, vec![]),
        ]);
        assert_eq!(program_ids, vec![program_id0, program_id1]);
    }

    #[test]
    fn test_message_unique_keys_both_signed() {
        let program_id = BvmAddr::default();
        let id0 = BvmAddr::default();
        let keys = get_keys(
            &[
                OpCode::new(program_id, &0, vec![AccountMeta::new(id0, true)]),
                OpCode::new(program_id, &0, vec![AccountMeta::new(id0, true)]),
            ],
            None,
        );
        assert_eq!(keys, OpCodeKeys::new(vec![id0], vec![], 0, 0));
    }

    #[test]
    fn test_message_unique_keys_signed_and_payer() {
        let program_id = BvmAddr::default();
        let id0 = BvmAddr::default();
        let keys = get_keys(
            &[OpCode::new(
                program_id,
                &0,
                vec![AccountMeta::new(id0, true)],
            )],
            Some(&id0),
        );
        assert_eq!(keys, OpCodeKeys::new(vec![id0], vec![], 0, 0));
    }

    #[test]
    fn test_message_unique_keys_unsigned_and_payer() {
        let program_id = BvmAddr::default();
        let id0 = BvmAddr::default();
        let keys = get_keys(
            &[OpCode::new(
                program_id,
                &0,
                vec![AccountMeta::new(id0, false)],
            )],
            Some(&id0),
        );
        assert_eq!(keys, OpCodeKeys::new(vec![id0], vec![], 0, 0));
    }

    #[test]
    fn test_message_unique_keys_one_signed() {
        let program_id = BvmAddr::default();
        let id0 = BvmAddr::default();
        let keys = get_keys(
            &[
                OpCode::new(program_id, &0, vec![AccountMeta::new(id0, false)]),
                OpCode::new(program_id, &0, vec![AccountMeta::new(id0, true)]),
            ],
            None,
        );
        assert_eq!(keys, OpCodeKeys::new(vec![id0], vec![], 0, 0));
    }

    #[test]
    fn test_message_unique_keys_order_preserved() {
        let program_id = BvmAddr::default();
        let id0 = BvmAddr::new_rand();
        let id1 = BvmAddr::default(); // Key less than id0
        let keys = get_keys(
            &[
                OpCode::new(program_id, &0, vec![AccountMeta::new(id0, false)]),
                OpCode::new(program_id, &0, vec![AccountMeta::new(id1, false)]),
            ],
            None,
        );
        assert_eq!(keys, OpCodeKeys::new(vec![], vec![id0, id1], 0, 0));
    }

    #[test]
    fn test_message_unique_keys_not_adjacent() {
        let program_id = BvmAddr::default();
        let id0 = BvmAddr::default();
        let id1 = BvmAddr::new_rand();
        let keys = get_keys(
            &[
                OpCode::new(program_id, &0, vec![AccountMeta::new(id0, false)]),
                OpCode::new(program_id, &0, vec![AccountMeta::new(id1, false)]),
                OpCode::new(program_id, &0, vec![AccountMeta::new(id0, true)]),
            ],
            None,
        );
        assert_eq!(keys, OpCodeKeys::new(vec![id0], vec![id1], 0, 0));
    }

    #[test]
    fn test_message_signed_keys_first() {
        let program_id = BvmAddr::default();
        let id0 = BvmAddr::default();
        let id1 = BvmAddr::new_rand();
        let keys = get_keys(
            &[
                OpCode::new(program_id, &0, vec![AccountMeta::new(id0, false)]),
                OpCode::new(program_id, &0, vec![AccountMeta::new(id1, true)]),
            ],
            None,
        );
        assert_eq!(keys, OpCodeKeys::new(vec![id1], vec![id0], 0, 0));
    }

    #[test]
    // Ensure there's a way to calculate the number of required signatures.
    fn test_message_signed_keys_len() {
        let program_id = BvmAddr::default();
        let id0 = BvmAddr::default();
        let ix = OpCode::new(program_id, &0, vec![AccountMeta::new(id0, false)]);
        let context = Context::new(vec![ix]);
        assert_eq!(context.header.num_required_signatures, 0);

        let ix = OpCode::new(program_id, &0, vec![AccountMeta::new(id0, true)]);
        let context = Context::new(vec![ix]);
        assert_eq!(context.header.num_required_signatures, 1);
    }

    #[test]
    fn test_message_credit_only_keys_last() {
        let program_id = BvmAddr::default();
        let id0 = BvmAddr::default(); // Identical key/program_id should be de-duped
        let id1 = BvmAddr::new_rand();
        let id2 = BvmAddr::new_rand();
        let id3 = BvmAddr::new_rand();
        let keys = get_keys(
            &[
                OpCode::new(
                    program_id,
                    &0,
                    vec![AccountMeta::new_credit_only(id0, false)],
                ),
                OpCode::new(
                    program_id,
                    &0,
                    vec![AccountMeta::new_credit_only(id1, true)],
                ),
                OpCode::new(program_id, &0, vec![AccountMeta::new(id2, false)]),
                OpCode::new(program_id, &0, vec![AccountMeta::new(id3, true)]),
            ],
            None,
        );
        assert_eq!(
            keys,
            OpCodeKeys::new(vec![id3, id1], vec![id2, id0], 1, 1)
        );
    }

    #[test]
    fn test_message_kitchen_sink() {
        let program_id0 = BvmAddr::new_rand();
        let program_id1 = BvmAddr::new_rand();
        let id0 = BvmAddr::default();
        let keypair1 = Keypair::new();
        let id1 = keypair1.pubkey();
        let context = Context::new(vec![
            OpCode::new(program_id0, &0, vec![AccountMeta::new(id0, false)]),
            OpCode::new(program_id1, &0, vec![AccountMeta::new(id1, true)]),
            OpCode::new(program_id0, &0, vec![AccountMeta::new(id1, false)]),
        ]);
        assert_eq!(
            context.instructions[0],
            EncodedOpCodes::new(2, &0, vec![1])
        );
        assert_eq!(
            context.instructions[1],
            EncodedOpCodes::new(3, &0, vec![0])
        );
        assert_eq!(
            context.instructions[2],
            EncodedOpCodes::new(2, &0, vec![0])
        );
    }

    #[test]
    fn test_message_payer_first() {
        let program_id = BvmAddr::default();
        let payer = BvmAddr::new_rand();
        let id0 = BvmAddr::default();

        let ix = OpCode::new(program_id, &0, vec![AccountMeta::new(id0, false)]);
        let context = Context::new_with_payer(vec![ix], Some(&payer));
        assert_eq!(context.header.num_required_signatures, 1);

        let ix = OpCode::new(program_id, &0, vec![AccountMeta::new(id0, true)]);
        let context = Context::new_with_payer(vec![ix], Some(&payer));
        assert_eq!(context.header.num_required_signatures, 2);

        let ix = OpCode::new(
            program_id,
            &0,
            vec![AccountMeta::new(payer, true), AccountMeta::new(id0, true)],
        );
        let context = Context::new_with_payer(vec![ix], Some(&payer));
        assert_eq!(context.header.num_required_signatures, 2);
    }

    #[test]
    fn test_message_program_last() {
        let program_id = BvmAddr::default();
        let id0 = BvmAddr::new_rand();
        let id1 = BvmAddr::new_rand();
        let keys = get_keys(
            &[
                OpCode::new(
                    program_id,
                    &0,
                    vec![AccountMeta::new_credit_only(id0, false)],
                ),
                OpCode::new(
                    program_id,
                    &0,
                    vec![AccountMeta::new_credit_only(id1, true)],
                ),
            ],
            None,
        );
        assert_eq!(
            keys,
            OpCodeKeys::new(vec![id1], vec![id0, program_id], 1, 2)
        );
    }

    #[test]
    fn test_program_position() {
        let program_id0 = BvmAddr::default();
        let program_id1 = BvmAddr::new_rand();
        let id = BvmAddr::new_rand();
        let context = Context::new(vec![
            OpCode::new(program_id0, &0, vec![AccountMeta::new(id, false)]),
            OpCode::new(program_id1, &0, vec![AccountMeta::new(id, true)]),
        ]);
        assert_eq!(context.program_position(0), None);
        assert_eq!(context.program_position(1), Some(0));
        assert_eq!(context.program_position(2), Some(1));
    }

    #[test]
    fn test_is_credit_debit() {
        let key0 = BvmAddr::new_rand();
        let key1 = BvmAddr::new_rand();
        let key2 = BvmAddr::new_rand();
        let key3 = BvmAddr::new_rand();
        let key4 = BvmAddr::new_rand();
        let key5 = BvmAddr::new_rand();

        let context = Context {
            header: MessageHeader {
                num_required_signatures: 3,
                num_credit_only_signed_accounts: 2,
                num_credit_only_unsigned_accounts: 1,
            },
            account_keys: vec![key0, key1, key2, key3, key4, key5],
            recent_transaction_seal: Hash::default(),
            instructions: vec![],
        };
        assert_eq!(context.is_credit_debit(0), true);
        assert_eq!(context.is_credit_debit(1), false);
        assert_eq!(context.is_credit_debit(2), false);
        assert_eq!(context.is_credit_debit(3), true);
        assert_eq!(context.is_credit_debit(4), true);
        assert_eq!(context.is_credit_debit(5), false);
    }

    #[test]
    fn test_get_account_keys_by_lock_type() {
        let program_id = BvmAddr::default();
        let id0 = BvmAddr::new_rand();
        let id1 = BvmAddr::new_rand();
        let id2 = BvmAddr::new_rand();
        let id3 = BvmAddr::new_rand();
        let context = Context::new(vec![
            OpCode::new(program_id, &0, vec![AccountMeta::new(id0, false)]),
            OpCode::new(program_id, &0, vec![AccountMeta::new(id1, true)]),
            OpCode::new(
                program_id,
                &0,
                vec![AccountMeta::new_credit_only(id2, false)],
            ),
            OpCode::new(
                program_id,
                &0,
                vec![AccountMeta::new_credit_only(id3, true)],
            ),
        ]);
        assert_eq!(
            context.get_account_keys_by_lock_type(),
            (vec![&id1, &id0], vec![&id3, &id2, &program_id])
        );
    }
}
