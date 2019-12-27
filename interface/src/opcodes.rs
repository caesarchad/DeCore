//! Defines a composable Instruction type and a memory-efficient EncodedOpCodes.

use crate::bvm_address::BvmAddr;
use crate::short_vec;
use crate::sys_opcode::SystemError;
use bincode::serialize;
use serde::Serialize;

/// Reasons the runtime might have rejected an instruction.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum OpCodeErr {
    /// Deprecated! Use CustomError instead!
    /// The program instruction returned an error
    GenericError,

    /// The arguments provided to a program instruction where invalid
    InvalidArgument,

    /// An instruction's data contents was invalid
    BadOpCodeContext,

    /// An account's data contents was invalid
    InvalidAccountData,

    /// An account's data was too small
    AccountDataTooSmall,

    /// The account did not have the expected program id
    IncorrectProgramId,

    /// A signature was required but not found
    MissingRequiredSignature,

    /// An initialize instruction was sent to an account that has already been initialized.
    AccountAlreadyInitialized,

    /// An attempt to operate on an account that hasn't been initialized.
    UninitializedAccount,

    /// Program's instruction dif balance does not equal the balance after the instruction
    DeficitOpCode,

    /// Program modified an account's program id
    ModifiedProgramId,

    /// Program spent the difs of an account that doesn't belong to it
    ExternalAccountDifSpend,

    /// Program modified the data of an account that doesn't belong to it
    ExternalAccountDataModified,

    /// An account was referenced more than once in a single instruction
    DuplicateAccountIndex,

    /// CustomError allows on-chain programs to implement program-specific error types and see
    /// them returned by the Morgan runtime. A CustomError may be any type that is represented
    /// as or serialized to a u32 integer.
    CustomError(u32),
}

impl OpCodeErr {
    pub fn new_result_with_negative_difs() -> Self {
        OpCodeErr::CustomError(SystemError::ResultWithNegativeDifs as u32)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct OpCode {
    /// BvmAddr of the instruction processor that executes this instruction
    pub program_ids_index: BvmAddr,
    /// Metadata for what accounts should be passed to the instruction processor
    pub accounts: Vec<AccountMeta>,
    /// Opaque data passed to the instruction processor
    pub data: Vec<u8>,
}

impl OpCode {
    pub fn new<T: Serialize>(
        program_ids_index: BvmAddr,
        data: &T,
        accounts: Vec<AccountMeta>,
    ) -> Self {
        let data = serialize(data).unwrap();
        Self {
            program_ids_index,
            data,
            accounts,
        }
    }
}

/// Account metadata used to define Instructions
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct AccountMeta {
    /// An account's public key
    pub address: BvmAddr,
    /// True if an Instruciton requires a Transaction signature matching `address`.
    pub is_signer: bool,
    /// True if the `address` can be loaded as a credit-debit account.
    pub is_debitable: bool,
}

impl AccountMeta {
    pub fn new(address: BvmAddr, is_signer: bool) -> Self {
        Self {
            address,
            is_signer,
            is_debitable: true,
        }
    }

    pub fn new_credit_only(address: BvmAddr, is_signer: bool) -> Self {
        Self {
            address,
            is_signer,
            is_debitable: false,
        }
    }
}

/// An instruction to execute a program
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct EncodedOpCodes {
    /// Index into the transaction keys array indicating the program account that executes this instruction
    pub program_ids_index: u8,
    /// Ordered indices into the transaction keys array indicating which accounts to pass to the program
    #[serde(with = "short_vec")]
    pub accounts: Vec<u8>,
    /// The program input data
    #[serde(with = "short_vec")]
    pub data: Vec<u8>,
}

impl EncodedOpCodes {
    pub fn new<T: Serialize>(program_ids_index: u8, data: &T, accounts: Vec<u8>) -> Self {
        let data = serialize(data).unwrap();
        Self {
            program_ids_index,
            data,
            accounts,
        }
    }

    pub fn program_id<'a>(&self, program_ids: &'a [BvmAddr]) -> &'a BvmAddr {
        &program_ids[self.program_ids_index as usize]
    }
}
