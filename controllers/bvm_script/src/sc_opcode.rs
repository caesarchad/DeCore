use crate::bvm_script::BvmScript;
use crate::script_state::BudgetState;
use crate::id;
use bincode::serialized_size;
use chrono::prelude::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use morgan_interface::opcodes::{AccountMeta, OpCode};
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::sys_opcode;

/// A smart contract.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Contract {
    /// The number of difs allocated to the `BvmScript` and any transaction fees.
    pub difs: u64,
    pub bvm_script: BvmScript,
}

/// An instruction to progress the smart contract.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum SmarContractOpCode {
    /// Declare and instantiate `BvmScript`.
    InitializeAccount(BvmScript),

    /// Tell a payment plan acknowledge the given `DateTime` has past.
    ApplyTimestamp(DateTime<Utc>),

    /// Tell the budget that the `InitializeAccount` with `Signature` has been
    /// signed by the containing transaction's `BvmAddr`.
    ApplySignature,
}

fn initialize_account(contract: &BvmAddr, expr: BvmScript) -> OpCode {
    let mut keys = vec![];
    if let BvmScript::Pay(payment) = &expr {
        keys.push(AccountMeta::new(payment.to, false));
    }
    keys.push(AccountMeta::new(*contract, false));
    OpCode::new(id(), &SmarContractOpCode::InitializeAccount(expr), keys)
}

pub fn create_account(
    from: &BvmAddr,
    contract: &BvmAddr,
    difs: u64,
    expr: BvmScript,
) -> Vec<OpCode> {
    if !expr.verify(difs) {
        panic!("invalid budget expression");
    }
    let space = serialized_size(&BudgetState::new(expr.clone())).unwrap();
    vec![
        sys_opcode::create_account(&from, contract, difs, space, &id()),
        initialize_account(contract, expr),
    ]
}

/// Create a new payment script.
pub fn payment(from: &BvmAddr, to: &BvmAddr, difs: u64) -> Vec<OpCode> {
    let contract = BvmAddr::new_rand();
    let expr = BvmScript::new_payment(difs, to);
    create_account(from, &contract, difs, expr)
}

/// Create a future payment script.
pub fn on_date(
    from: &BvmAddr,
    to: &BvmAddr,
    contract: &BvmAddr,
    dt: DateTime<Utc>,
    dt_address: &BvmAddr,
    cancelable: Option<BvmAddr>,
    difs: u64,
) -> Vec<OpCode> {
    let expr = BvmScript::pending_planned_pay(dt, dt_address, difs, to, cancelable);
    create_account(from, contract, difs, expr)
}

/// Create a multisig payment script.
pub fn when_signed(
    from: &BvmAddr,
    to: &BvmAddr,
    contract: &BvmAddr,
    witness: &BvmAddr,
    cancelable: Option<BvmAddr>,
    difs: u64,
) -> Vec<OpCode> {
    let expr = BvmScript::pending_appr_pay(witness, difs, to, cancelable);
    create_account(from, contract, difs, expr)
}

pub fn apply_timestamp(
    from: &BvmAddr,
    contract: &BvmAddr,
    to: &BvmAddr,
    dt: DateTime<Utc>,
) -> OpCode {
    let mut account_metas = vec![
        AccountMeta::new(*from, true),
        AccountMeta::new(*contract, false),
    ];
    if from != to {
        account_metas.push(AccountMeta::new(*to, false));
    }
    OpCode::new(id(), &SmarContractOpCode::ApplyTimestamp(dt), account_metas)
}

pub fn apply_signature(from: &BvmAddr, contract: &BvmAddr, to: &BvmAddr) -> OpCode {
    let mut account_metas = vec![
        AccountMeta::new(*from, true),
        AccountMeta::new(*contract, false),
    ];
    if from != to {
        account_metas.push(AccountMeta::new(*to, false));
    }
    OpCode::new(id(), &SmarContractOpCode::ApplySignature, account_metas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bvm_script::BvmScript;

    #[test]
    fn test_budget_instruction_verify() {
        let alice_address = BvmAddr::new_rand();
        let bob_address = BvmAddr::new_rand();
        payment(&alice_address, &bob_address, 1); // No panic! indicates success.
    }

    #[test]
    #[should_panic]
    fn test_budget_instruction_overspend() {
        let alice_address = BvmAddr::new_rand();
        let bob_address = BvmAddr::new_rand();
        let budget_address = BvmAddr::new_rand();
        let expr = BvmScript::new_payment(2, &bob_address);
        create_account(&alice_address, &budget_address, 1, expr);
    }

    #[test]
    #[should_panic]
    fn test_budget_instruction_underspend() {
        let alice_address = BvmAddr::new_rand();
        let bob_address = BvmAddr::new_rand();
        let budget_address = BvmAddr::new_rand();
        let expr = BvmScript::new_payment(1, &bob_address);
        create_account(&alice_address, &budget_address, 2, expr);
    }
}
