use crate::bvm_script::BvmScript;
use crate::script_state::BudgetState;
use crate::id;
use bincode::serialized_size;
use chrono::prelude::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use morgan_interface::opcodes::{AccountMeta, OpCode};
use morgan_interface::pubkey::Pubkey;
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
    /// signed by the containing transaction's `Pubkey`.
    ApplySignature,
}

fn initialize_account(contract: &Pubkey, expr: BvmScript) -> OpCode {
    let mut keys = vec![];
    if let BvmScript::Pay(payment) = &expr {
        keys.push(AccountMeta::new(payment.to, false));
    }
    keys.push(AccountMeta::new(*contract, false));
    OpCode::new(id(), &SmarContractOpCode::InitializeAccount(expr), keys)
}

pub fn create_account(
    from: &Pubkey,
    contract: &Pubkey,
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
pub fn payment(from: &Pubkey, to: &Pubkey, difs: u64) -> Vec<OpCode> {
    let contract = Pubkey::new_rand();
    let expr = BvmScript::new_payment(difs, to);
    create_account(from, &contract, difs, expr)
}

/// Create a future payment script.
pub fn on_date(
    from: &Pubkey,
    to: &Pubkey,
    contract: &Pubkey,
    dt: DateTime<Utc>,
    dt_pubkey: &Pubkey,
    cancelable: Option<Pubkey>,
    difs: u64,
) -> Vec<OpCode> {
    let expr = BvmScript::new_cancelable_future_payment(dt, dt_pubkey, difs, to, cancelable);
    create_account(from, contract, difs, expr)
}

/// Create a multisig payment script.
pub fn when_signed(
    from: &Pubkey,
    to: &Pubkey,
    contract: &Pubkey,
    witness: &Pubkey,
    cancelable: Option<Pubkey>,
    difs: u64,
) -> Vec<OpCode> {
    let expr = BvmScript::new_cancelable_authorized_payment(witness, difs, to, cancelable);
    create_account(from, contract, difs, expr)
}

pub fn apply_timestamp(
    from: &Pubkey,
    contract: &Pubkey,
    to: &Pubkey,
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

pub fn apply_signature(from: &Pubkey, contract: &Pubkey, to: &Pubkey) -> OpCode {
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
        let alice_pubkey = Pubkey::new_rand();
        let bob_pubkey = Pubkey::new_rand();
        payment(&alice_pubkey, &bob_pubkey, 1); // No panic! indicates success.
    }

    #[test]
    #[should_panic]
    fn test_budget_instruction_overspend() {
        let alice_pubkey = Pubkey::new_rand();
        let bob_pubkey = Pubkey::new_rand();
        let budget_pubkey = Pubkey::new_rand();
        let expr = BvmScript::new_payment(2, &bob_pubkey);
        create_account(&alice_pubkey, &budget_pubkey, 1, expr);
    }

    #[test]
    #[should_panic]
    fn test_budget_instruction_underspend() {
        let alice_pubkey = Pubkey::new_rand();
        let bob_pubkey = Pubkey::new_rand();
        let budget_pubkey = Pubkey::new_rand();
        let expr = BvmScript::new_payment(1, &bob_pubkey);
        create_account(&alice_pubkey, &budget_pubkey, 2, expr);
    }
}
