use log::*;
use crate::opcodes::{AccountMeta, OpCode};
use crate::opcodes_utils::DecodeError;
use crate::bvm_address::BvmAddr;
use crate::sys_controller;
use num_derive::FromPrimitive;
use morgan_helper::logHelper::*;

#[derive(Serialize, Debug, Clone, PartialEq, FromPrimitive)]
pub enum SystemError {
    AccountAlreadyInUse,
    ResultWithNegativeDifs,
    SourceNotSystemAccount,
    ResultWithNegativeReputations,
}

impl<T> DecodeError<T> for SystemError {
    fn type_of(&self) -> &'static str {
        "SystemError"
    }
}

impl std::fmt::Display for SystemError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "error")
    }
}
impl std::error::Error for SystemError {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum SysOpCode {
    /// Create a new account
    /// * Transaction::keys[0] - genesis
    /// * Transaction::keys[1] - new account key
    /// * difs - number of difs to transfer to the new account
    /// * space - memory to allocate if greater then zero
    /// * program_id - the program id of the new account
    CreateAccount {
        difs: u64,
        reputations: u64,
        space: u64,
        program_id: BvmAddr,
    },
    /// Assign account to a program
    /// * Transaction::keys[0] - account to assign
    Assign { program_id: BvmAddr },
    /// Transfer difs
    /// * Transaction::keys[0] - genesis
    /// * Transaction::keys[1] - destination
    Transfer { difs: u64 },
    /// Create a new account
    /// * Transaction::keys[0] - genesis
    /// * Transaction::keys[1] - new account key
    /// * reputations - number of reputations to transfer to the new account
    /// * space - memory to allocate if greater then zero
    /// * program_id - the program id of the new account
    CreateAccountWithReputation {
        reputations: u64,
        space: u64,
        program_id: BvmAddr,
    },
    /// Transfer reputations
    /// * Transaction::keys[0] - genesis
    /// * Transaction::keys[1] - destination
    TransferReputations { reputations: u64 },
}

pub fn create_account(
    from_address: &BvmAddr,
    to_address: &BvmAddr,
    difs: u64,
    space: u64,
    program_id: &BvmAddr,
) -> OpCode {
    let account_metas = vec![
        AccountMeta::new(*from_address, true),
        AccountMeta::new(*to_address, false),
    ];
    let reputations = 0;
    OpCode::new(
        sys_controller::id(),
        &SysOpCode::CreateAccount {
            difs,
            reputations,
            space,
            program_id: *program_id,
        },
        account_metas,
    )
}

pub fn create_account_with_reputation(
    from_address: &BvmAddr,
    to_address: &BvmAddr,
    reputations: u64,
    space: u64,
    program_id: &BvmAddr,
) -> OpCode {
    let account_metas = vec![
        AccountMeta::new(*from_address, true),
        AccountMeta::new(*to_address, false),
    ];
    // info!("{}", Info(format!("create_account_with_reputation: {:?}", account_metas).to_string()));
    println!("{}",
        printLn(
            format!("create_account_with_reputation: {:?}", account_metas).to_string(),
            module_path!().to_string()
        )
    );
    OpCode::new(
        sys_controller::id(),
        &SysOpCode::CreateAccountWithReputation {
            reputations,
            space,
            program_id: *program_id,
        },
        account_metas,
    )
}

/// Create and sign a transaction to create a system account
pub fn create_user_account(from_address: &BvmAddr, to_address: &BvmAddr, difs: u64) -> OpCode {
    let program_id = sys_controller::id();
    create_account(from_address, to_address, difs, 0, &program_id)
}

/// Create and sign a transaction to create a system account with reputation
pub fn create_user_account_with_reputation(from_address: &BvmAddr, to_address: &BvmAddr, reputations: u64) -> OpCode {
    let program_id = sys_controller::id();
    // info!("{}", Info(format!("create_user_account_with_reputation to : {:?}", to_address).to_string()));
    println!("{}",
        printLn(
            format!("create_user_account_with_reputation to : {:?}", to_address).to_string(),
            module_path!().to_string()
        )
    );
    create_account_with_reputation(from_address, to_address, reputations, 0, &program_id)
}

pub fn assign(from_address: &BvmAddr, program_id: &BvmAddr) -> OpCode {
    let account_metas = vec![AccountMeta::new(*from_address, true)];
    OpCode::new(
        sys_controller::id(),
        &SysOpCode::Assign {
            program_id: *program_id,
        },
        account_metas,
    )
}

pub fn transfer(from_address: &BvmAddr, to_address: &BvmAddr, difs: u64) -> OpCode {
    let account_metas = vec![
        AccountMeta::new(*from_address, true),
        AccountMeta::new(*to_address, false),
    ];
    OpCode::new(
        sys_controller::id(),
        &SysOpCode::Transfer { difs },
        account_metas,
    )
}

pub fn transfer_reputations(from_address: &BvmAddr, to_address: &BvmAddr, reputations: u64) -> OpCode {
    let account_metas = vec![
        AccountMeta::new(*from_address, true),
        AccountMeta::new(*to_address, false),
    ];
    OpCode::new(
        sys_controller::id(),
        &SysOpCode::TransferReputations { reputations },
        account_metas,
    )
}

/// Create and sign new SysOpCode::Transfer transaction to many destinations
pub fn transfer_many(from_address: &BvmAddr, to_difs: &[(BvmAddr, u64)]) -> Vec<OpCode> {
    to_difs
        .iter()
        .map(|(to_address, difs)| transfer(from_address, to_address, *difs))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_keys(instruction: &OpCode) -> Vec<BvmAddr> {
        instruction.accounts.iter().map(|x| x.address).collect()
    }

    #[test]
    fn test_move_many() {
        let alice_address = BvmAddr::new_rand();
        let bob_address = BvmAddr::new_rand();
        let carol_address = BvmAddr::new_rand();
        let to_difs = vec![(bob_address, 1), (carol_address, 2)];

        let instructions = transfer_many(&alice_address, &to_difs);
        assert_eq!(instructions.len(), 2);
        assert_eq!(get_keys(&instructions[0]), vec![alice_address, bob_address]);
        assert_eq!(get_keys(&instructions[1]), vec![alice_address, carol_address]);
    }
}
