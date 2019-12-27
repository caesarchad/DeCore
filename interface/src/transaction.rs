//! Defines a Transaction type to package an atomic sequence of instructions.

use crate::hash::Hash;
use crate::opcodes::{EncodedOpCodes, OpCode, OpCodeErr};
use crate::context::Context;
use crate::bvm_address::BvmAddr;
use crate::short_vec;
use crate::signature::{KeypairUtil, Signature};
use bincode::serialize;
use std::result;

/// Reasons a transaction might be rejected.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum TransactionError {
    
    AccountInUse,

    AccountLoadedTwice,

    AccountNotFound,

    ProgramAccountNotFound,

    InsufficientFundsForFee,

    InvalidAccountForFee,

    DuplicateSignature,

    TransactionSealNotFound,

    OpCodeErr(u8, OpCodeErr),

    CallChainTooDeep,

    MissingSignatureForFee,

    InvalidAccountIndex,
}

pub type Result<T> = result::Result<T, TransactionError>;

/// An atomic transaction
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// A set of digital signatures of `account_keys`, `program_ids`, `recent_transaction_seal`, and `instructions`, signed by the first
    /// signatures.len() keys of account_keys
    #[serde(with = "short_vec")]
    pub signatures: Vec<Signature>,

    /// The message to sign.
    pub context: Context,
}

impl Transaction {
    pub fn new_unsigned(context: Context) -> Self {
        Self {
            signatures: vec![Signature::default(); context.header.num_required_signatures as usize],
            context,
        }
    }

    pub fn new_u_opcodes(instructions: Vec<OpCode>) -> Self {
        let context = Context::new(instructions);
        Self::new_unsigned(context)
    }

    pub fn new<T: KeypairUtil>(
        from_keypairs: &[&T],
        context: Context,
        recent_transaction_seal: Hash,
    ) -> Transaction {
        let mut tx = Self::new_unsigned(context);
        tx.sign(from_keypairs, recent_transaction_seal);
        tx
    }

    pub fn new_s_opcodes<T: KeypairUtil>(
        from_keypairs: &[&T],
        instructions: Vec<OpCode>,
        recent_transaction_seal: Hash,
    ) -> Transaction {
        let context = Context::new(instructions);
        Self::new(from_keypairs, context, recent_transaction_seal)
    }

    
    pub fn new_with_encoded_opcodes<T: KeypairUtil>(
        from_keypairs: &[&T],
        keys: &[BvmAddr],
        recent_transaction_seal: Hash,
        program_ids: Vec<BvmAddr>,
        instructions: Vec<EncodedOpCodes>,
    ) -> Self {
        let mut account_keys: Vec<_> = from_keypairs
            .iter()
            .map(|keypair| (*keypair).pubkey())
            .collect();
        account_keys.extend_from_slice(keys);
        account_keys.extend(&program_ids);
        let context = Context::new_with_encoded_opcodes(
            from_keypairs.len() as u8,
            0,
            program_ids.len() as u8,
            account_keys,
            Hash::default(),
            instructions,
        );
        Transaction::new(from_keypairs, context, recent_transaction_seal)
    }

    pub fn data(&self, instruction_index: usize) -> &[u8] {
        &self.context.instructions[instruction_index].data
    }

    fn key_index(&self, instruction_index: usize, accounts_index: usize) -> Option<usize> {
        self.context
            .instructions
            .get(instruction_index)
            .and_then(|instruction| instruction.accounts.get(accounts_index))
            .map(|&account_keys_index| account_keys_index as usize)
    }
    pub fn key(&self, instruction_index: usize, accounts_index: usize) -> Option<&BvmAddr> {
        self.key_index(instruction_index, accounts_index)
            .and_then(|account_keys_index| self.context.account_keys.get(account_keys_index))
    }
    pub fn signer_key(&self, instruction_index: usize, accounts_index: usize) -> Option<&BvmAddr> {
        match self.key_index(instruction_index, accounts_index) {
            None => None,
            Some(signature_index) => {
                if signature_index >= self.signatures.len() {
                    return None;
                }
                self.context.account_keys.get(signature_index)
            }
        }
    }

    /// Return a message containing all data that should be signed.
    pub fn self_context(&self) -> &Context {
        &self.context
    }

    /// Return the serialized message data to sign.
    pub fn context_data(&self) -> Vec<u8> {
        serialize(&self.self_context()).unwrap()
    }

    /// Sign this transaction.
    pub fn sign_unchecked<T: KeypairUtil>(&mut self, keypairs: &[&T], recent_transaction_seal: Hash) {
        self.context.recent_transaction_seal = recent_transaction_seal;
        let context_data = self.context_data();
        self.signatures = keypairs
            .iter()
            .map(|keypair| keypair.sign_context(&context_data))
            .collect();
    }

    /// Check keys and keypair lengths, then sign this transaction.
    pub fn sign<T: KeypairUtil>(&mut self, keypairs: &[&T], recent_transaction_seal: Hash) {
        let signed_keys =
            &self.context.account_keys[0..self.context.header.num_required_signatures as usize];
        for (i, keypair) in keypairs.iter().enumerate() {
            assert_eq!(keypair.pubkey(), signed_keys[i], "keypair-pubkey mismatch");
        }
        assert_eq!(keypairs.len(), signed_keys.len(), "not enough keypairs");
        self.sign_unchecked(keypairs, recent_transaction_seal);
    }

    /// Sign using some subset of required keys
    ///  if recent_transaction_seal is not the same as currently in the transaction,
    ///  clear any prior signatures and update recent_transaction_seal
    pub fn partial_sign<T: KeypairUtil>(&mut self, keypairs: &[&T], recent_transaction_seal: Hash) {
        let signed_keys =
            &self.context.account_keys[0..self.context.header.num_required_signatures as usize];

        // if you change the transaction_seal, you're re-signing...
        if recent_transaction_seal != self.context.recent_transaction_seal {
            self.context.recent_transaction_seal = recent_transaction_seal;
            self.signatures
                .iter_mut()
                .for_each(|signature| *signature = Signature::default());
        }

        for keypair in keypairs {
            let i = signed_keys
                .iter()
                .position(|pubkey| pubkey == &keypair.pubkey())
                .expect("keypair-pubkey mismatch");

            self.signatures[i] = keypair.sign_context(&self.context_data())
        }
    }

    pub fn is_signed(&self) -> bool {
        self.signatures
            .iter()
            .all(|signature| *signature != Signature::default())
    }

    /// Verify that references in the instructions are valid
    pub fn verify_refs(&self) -> bool {
        let context = self.self_context();
        for instruction in &context.instructions {
            if (instruction.program_ids_index as usize) >= context.account_keys.len() {
                return false;
            }
            for account_index in &instruction.accounts {
                if (*account_index as usize) >= context.account_keys.len() {
                    return false;
                }
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash;
    use crate::opcodes::AccountMeta;
    use crate::signature::Keypair;
    use crate::sys_opcode;
    use bincode::{deserialize, serialize, serialized_size};
    use std::mem::size_of;

    fn get_program_id(tx: &Transaction, instruction_index: usize) -> &BvmAddr {
        let context = tx.self_context();
        let instruction = &context.instructions[instruction_index];
        instruction.program_id(&context.account_keys)
    }

    #[test]
    fn test_refs() {
        let key = Keypair::new();
        let key1 = BvmAddr::new_rand();
        let key2 = BvmAddr::new_rand();
        let prog1 = BvmAddr::new_rand();
        let prog2 = BvmAddr::new_rand();
        let instructions = vec![
            EncodedOpCodes::new(3, &(), vec![0, 1]),
            EncodedOpCodes::new(4, &(), vec![0, 2]),
        ];
        let tx = Transaction::new_with_encoded_opcodes(
            &[&key],
            &[key1, key2],
            Hash::default(),
            vec![prog1, prog2],
            instructions,
        );
        assert!(tx.verify_refs());

        assert_eq!(tx.key(0, 0), Some(&key.pubkey()));
        assert_eq!(tx.signer_key(0, 0), Some(&key.pubkey()));

        assert_eq!(tx.key(1, 0), Some(&key.pubkey()));
        assert_eq!(tx.signer_key(1, 0), Some(&key.pubkey()));

        assert_eq!(tx.key(0, 1), Some(&key1));
        assert_eq!(tx.signer_key(0, 1), None);

        assert_eq!(tx.key(1, 1), Some(&key2));
        assert_eq!(tx.signer_key(1, 1), None);

        assert_eq!(tx.key(2, 0), None);
        assert_eq!(tx.signer_key(2, 0), None);

        assert_eq!(tx.key(0, 2), None);
        assert_eq!(tx.signer_key(0, 2), None);

        assert_eq!(*get_program_id(&tx, 0), prog1);
        assert_eq!(*get_program_id(&tx, 1), prog2);
    }
    #[test]
    fn test_refs_invalid_program_id() {
        let key = Keypair::new();
        let instructions = vec![EncodedOpCodes::new(1, &(), vec![])];
        let tx = Transaction::new_with_encoded_opcodes(
            &[&key],
            &[],
            Hash::default(),
            vec![],
            instructions,
        );
        assert!(!tx.verify_refs());
    }
    #[test]
    fn test_refs_invalid_account() {
        let key = Keypair::new();
        let instructions = vec![EncodedOpCodes::new(1, &(), vec![2])];
        let tx = Transaction::new_with_encoded_opcodes(
            &[&key],
            &[],
            Hash::default(),
            vec![BvmAddr::default()],
            instructions,
        );
        assert_eq!(*get_program_id(&tx, 0), BvmAddr::default());
        assert!(!tx.verify_refs());
    }

    fn create_sample_transaction() -> Transaction {
        let keypair = Keypair::from_bytes(&[
            48, 83, 2, 1, 1, 48, 5, 6, 3, 43, 101, 112, 4, 34, 4, 32, 255, 101, 36, 24, 124, 23,
            167, 21, 132, 204, 155, 5, 185, 58, 121, 75, 156, 227, 116, 193, 215, 38, 142, 22, 8,
            14, 229, 239, 119, 93, 5, 218, 161, 35, 3, 33, 0, 36, 100, 158, 252, 33, 161, 97, 185,
            62, 89, 99,
        ])
        .unwrap();
        let to = BvmAddr::new(&[
            1, 1, 1, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 8, 7, 6, 5, 4,
            1, 1, 1,
        ]);

        let program_id = BvmAddr::new(&[
            2, 2, 2, 4, 5, 6, 7, 8, 9, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 9, 8, 7, 6, 5, 4,
            2, 2, 2,
        ]);
        let account_metas = vec![
            AccountMeta::new(keypair.pubkey(), true),
            AccountMeta::new(to, false),
        ];
        let instruction = OpCode::new(program_id, &(1u8, 2u8, 3u8), account_metas);
        let context = Context::new(vec![instruction]);
        Transaction::new(&[&keypair], context, Hash::default())
    }

    #[test]
    fn test_transaction_serialize() {
        let tx = create_sample_transaction();
        let ser = serialize(&tx).unwrap();
        let deser = deserialize(&ser).unwrap();
        assert_eq!(tx, deser);
    }

    /// Detect changes to the serialized size of payment transactions, which affects TPS.
    #[test]
    fn test_transaction_minimum_serialized_size() {
        let alice_keypair = Keypair::new();
        let alice_pubkey = alice_keypair.pubkey();
        let bob_pubkey = BvmAddr::new_rand();
        let ix = sys_opcode::transfer(&alice_pubkey, &bob_pubkey, 42);

        let expected_data_size = size_of::<u32>() + size_of::<u64>();
        assert_eq!(expected_data_size, 12);
        assert_eq!(
            ix.data.len(),
            expected_data_size,
            "unexpected system instruction size"
        );

        let expected_instruction_size = 1 + 1 + ix.accounts.len() + 1 + expected_data_size;
        assert_eq!(expected_instruction_size, 17);

        let context = Context::new(vec![ix]);
        assert_eq!(
            serialized_size(&context.instructions[0]).unwrap() as usize,
            expected_instruction_size,
            "unexpected Instruction::serialized_size"
        );

        let tx = Transaction::new(&[&alice_keypair], context, Hash::default());

        let len_size = 1;
        let num_required_sigs_size = 1;
        let num_credit_only_accounts_size = 2;
        let transaction_seal_size = size_of::<Hash>();
        let expected_transaction_size = len_size
            + (tx.signatures.len() * size_of::<Signature>())
            + num_required_sigs_size
            + num_credit_only_accounts_size
            + len_size
            + (tx.context.account_keys.len() * size_of::<BvmAddr>())
            + transaction_seal_size
            + len_size
            + expected_instruction_size;
        assert_eq!(expected_transaction_size, 215);

        assert_eq!(
            serialized_size(&tx).unwrap() as usize,
            expected_transaction_size,
            "unexpected serialized transaction size"
        );
    }

    /// Detect binary changes in the serialized transaction data, which could have a downstream
    /// affect on SDKs and DApps
    #[test]
    fn test_sdk_serialize() {
        assert_eq!(
            serialize(&create_sample_transaction()).unwrap(),
            vec![
                1, 71, 59, 9, 187, 190, 129, 150, 165, 21, 33, 158, 72, 87, 110, 144, 120, 79, 238,
                132, 134, 105, 39, 102, 116, 209, 29, 229, 154, 36, 105, 44, 172, 118, 131, 22,
                124, 131, 179, 142, 176, 27, 117, 160, 89, 102, 224, 204, 1, 252, 141, 2, 136, 0,
                37, 218, 225, 129, 92, 154, 250, 59, 97, 178, 10, 1, 0, 1, 3, 156, 227, 116, 193,
                215, 38, 142, 22, 8, 14, 229, 239, 119, 93, 5, 218, 161, 35, 3, 33, 0, 36, 100,
                158, 252, 33, 161, 97, 185, 62, 89, 99, 1, 1, 1, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 9,
                9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 8, 7, 6, 5, 4, 1, 1, 1, 2, 2, 2, 4, 5, 6, 7, 8, 9, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 9, 8, 7, 6, 5, 4, 2, 2, 2, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2,
                2, 0, 1, 3, 1, 2, 3
            ]
        );
    }

    #[test]
    #[should_panic]
    fn test_transaction_missing_key() {
        let keypair = Keypair::new();
        Transaction::new_u_opcodes(vec![]).sign(&[&keypair], Hash::default());
    }

    #[test]
    #[should_panic]
    fn test_partial_sign_mismatched_key() {
        let keypair = Keypair::new();
        Transaction::new_u_opcodes(vec![OpCode::new(
            BvmAddr::default(),
            &0,
            vec![AccountMeta::new(BvmAddr::new_rand(), true)],
        )])
        .partial_sign(&[&keypair], Hash::default());
    }

    #[test]
    fn test_partial_sign() {
        let keypair0 = Keypair::new();
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let mut tx = Transaction::new_u_opcodes(vec![OpCode::new(
            BvmAddr::default(),
            &0,
            vec![
                AccountMeta::new(keypair0.pubkey(), true),
                AccountMeta::new(keypair1.pubkey(), true),
                AccountMeta::new(keypair2.pubkey(), true),
            ],
        )]);

        tx.partial_sign(&[&keypair0, &keypair2], Hash::default());
        assert!(!tx.is_signed());
        tx.partial_sign(&[&keypair1], Hash::default());
        assert!(tx.is_signed());

        let hash = hash(&[1]);
        tx.partial_sign(&[&keypair1], hash);
        assert!(!tx.is_signed());
        tx.partial_sign(&[&keypair0, &keypair2], hash);
        assert!(tx.is_signed());
    }

    #[test]
    #[should_panic]
    fn test_transaction_missing_keypair() {
        let program_id = BvmAddr::default();
        let keypair0 = Keypair::new();
        let id0 = keypair0.pubkey();
        let ix = OpCode::new(program_id, &0, vec![AccountMeta::new(id0, true)]);
        Transaction::new_u_opcodes(vec![ix])
            .sign(&Vec::<&Keypair>::new(), Hash::default());
    }

    #[test]
    #[should_panic]
    fn test_transaction_wrong_key() {
        let program_id = BvmAddr::default();
        let keypair0 = Keypair::new();
        let wrong_id = BvmAddr::default();
        let ix = OpCode::new(program_id, &0, vec![AccountMeta::new(wrong_id, true)]);
        Transaction::new_u_opcodes(vec![ix]).sign(&[&keypair0], Hash::default());
    }

    #[test]
    fn test_transaction_correct_key() {
        let program_id = BvmAddr::default();
        let keypair0 = Keypair::new();
        let id0 = keypair0.pubkey();
        let ix = OpCode::new(program_id, &0, vec![AccountMeta::new(id0, true)]);
        let mut tx = Transaction::new_u_opcodes(vec![ix]);
        tx.sign(&[&keypair0], Hash::default());
        assert_eq!(
            tx.context.instructions[0],
            EncodedOpCodes::new(1, &0, vec![0])
        );
        assert!(tx.is_signed());
    }
}
