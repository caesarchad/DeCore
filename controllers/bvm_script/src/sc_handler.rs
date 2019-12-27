//! budget program
use crate::bvm_script::Endorsement;
use crate::sc_opcode::SmarContractOpCode;
use crate::script_state::{BudgetError, BudgetState};
use bincode::deserialize;
use chrono::prelude::{DateTime, Utc};
use log::*;
use morgan_interface::account::KeyedAccount;
use morgan_interface::opcodes::OpCodeErr;
use morgan_interface::bvm_address::BvmAddr;
use morgan_helper::logHelper::*;

/// Process a Endorsement Signature. Any payment plans waiting on this signature
/// will progress one step.
fn apply_signature(
    script_state: &mut BudgetState,
    keyed_accounts: &mut [KeyedAccount],
) -> Result<(), BudgetError> {
    let mut commit_pay = None;
    if let Some(ref mut expr) = script_state.pending_budget {
        let key = keyed_accounts[0].signer_key().unwrap();
        expr.apply_witness(&Endorsement::Signature, key);
        commit_pay = expr.commit_pay();
    }

    if let Some(payment) = commit_pay {
        if let Some(key) = keyed_accounts[0].signer_key() {
            if &payment.to == key {
                script_state.pending_budget = None;
                keyed_accounts[1].account.difs -= payment.difs;
                keyed_accounts[0].account.difs += payment.difs;
                return Ok(());
            }
        }
        if &payment.to != keyed_accounts[2].unsigned_key() {
            trace!("destination missing");
            return Err(BudgetError::DestinationMissing);
        }
        script_state.pending_budget = None;
        keyed_accounts[1].account.difs -= payment.difs;
        keyed_accounts[2].account.difs += payment.difs;
    }
    Ok(())
}

/// Process a Endorsement Timestamp. Any payment plans waiting on this timestamp
/// will progress one step.
fn apply_timestamp(
    script_state: &mut BudgetState,
    keyed_accounts: &mut [KeyedAccount],
    dt: DateTime<Utc>,
) -> Result<(), BudgetError> {
    // Check to see if any timelocked transactions can be completed.
    let mut commit_pay = None;

    if let Some(ref mut expr) = script_state.pending_budget {
        let key = keyed_accounts[0].signer_key().unwrap();
        expr.apply_witness(&Endorsement::Timestamp(dt), key);
        commit_pay = expr.commit_pay();
    }

    if let Some(payment) = commit_pay {
        if &payment.to != keyed_accounts[2].unsigned_key() {
            trace!("destination missing");
            return Err(BudgetError::DestinationMissing);
        }
        script_state.pending_budget = None;
        keyed_accounts[1].account.difs -= payment.difs;
        keyed_accounts[2].account.difs += payment.difs;
    }
    Ok(())
}

pub fn handle_opcode(
    _program_id: &BvmAddr,
    keyed_accounts: &mut [KeyedAccount],
    data: &[u8],
    _drop_height: u64,
) -> Result<(), OpCodeErr> {
    let instruction = deserialize(data).map_err(|err| {
        // info!("{}", Info(format!("Invalid transaction data: {:?} {:?}", data, err).to_string()));
        let info:String = format!("Invalid transaction data: {:?} {:?}", data, err).to_string();
        println!("{}",
            printLn(
                info,
                module_path!().to_string()
            )
        );
        OpCodeErr::BadOpCodeContext
    })?;

    trace!("handle_opcode: {:?}", instruction);

    match instruction {
        SmarContractOpCode::InitializeAccount(expr) => {
            let expr = expr.clone();
            if let Some(payment) = expr.commit_pay() {
                keyed_accounts[1].account.difs = 0;
                keyed_accounts[0].account.difs += payment.difs;
                return Ok(());
            }
            let existing = BudgetState::deserialize(&keyed_accounts[0].account.data).ok();
            if Some(true) == existing.map(|x| x.initialized) {
                trace!("contract already exists");
                return Err(OpCodeErr::AccountAlreadyInitialized);
            }
            let mut script_state = BudgetState::default();
            script_state.pending_budget = Some(expr);
            script_state.initialized = true;
            script_state.serialize(&mut keyed_accounts[0].account.data)
        }
        SmarContractOpCode::ApplyTimestamp(dt) => {
            let mut script_state = BudgetState::deserialize(&keyed_accounts[1].account.data)?;
            if !script_state.is_pending() {
                return Ok(()); // Nothing to do here.
            }
            if !script_state.initialized {
                trace!("contract is uninitialized");
                return Err(OpCodeErr::UninitializedAccount);
            }
            if keyed_accounts[0].signer_key().is_none() {
                return Err(OpCodeErr::MissingRequiredSignature);
            }
            trace!("apply timestamp");
            apply_timestamp(&mut script_state, keyed_accounts, dt)
                .map_err(|e| OpCodeErr::CustomError(e as u32))?;
            trace!("apply timestamp committed");
            script_state.serialize(&mut keyed_accounts[1].account.data)
        }
        SmarContractOpCode::ApplySignature => {
            let mut script_state = BudgetState::deserialize(&keyed_accounts[1].account.data)?;
            if !script_state.is_pending() {
                return Ok(()); // Nothing to do here.
            }
            if !script_state.initialized {
                trace!("contract is uninitialized");
                return Err(OpCodeErr::UninitializedAccount);
            }
            if keyed_accounts[0].signer_key().is_none() {
                return Err(OpCodeErr::MissingRequiredSignature);
            }
            trace!("apply signature");
            apply_signature(&mut script_state, keyed_accounts)
                .map_err(|e| OpCodeErr::CustomError(e as u32))?;
            trace!("apply signature committed");
            script_state.serialize(&mut keyed_accounts[1].account.data)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sc_opcode;
    use crate::id;
    use morgan_runtime::treasury::Treasury;
    use morgan_runtime::treasury_client::TreasuryClient;
    use morgan_interface::account_host::OnlineAccount;
    use morgan_interface::genesis_block::create_genesis_block;
    use morgan_interface::opcodes::OpCodeErr;
    use morgan_interface::context::Context;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::transaction::TransactionError;

    fn create_treasury(difs: u64) -> (Treasury, Keypair) {
        let (genesis_block, mint_keypair) = create_genesis_block(difs);
        let mut treasury = Treasury::new(&genesis_block);
        treasury.add_opcode_handler(id(), handle_opcode);
        (treasury, mint_keypair)
    }

    #[test]
    fn test_budget_payment() {
        let (treasury, alice_keypair) = create_treasury(10_000);
        let treasury_client = TreasuryClient::new(treasury);
        let alice = alice_keypair.address();
        let bob = BvmAddr::new_rand();
        let instructions = sc_opcode::payment(&alice, &bob, 100);
        let context = Context::new(instructions);
        treasury_client
            .snd_online_context(&[&alice_keypair], context)
            .unwrap();
        assert_eq!(treasury_client.get_balance(&bob).unwrap(), 100);
    }

    #[test]
    fn test_unsigned_witness_key() {
        let (treasury, alice_keypair) = create_treasury(10_000);
        let treasury_client = TreasuryClient::new(treasury);
        let alice = alice_keypair.address();

        // Initialize BudgetState
        let budget_address = BvmAddr::new_rand();
        let bob = BvmAddr::new_rand();
        let witness = BvmAddr::new_rand();
        let instructions = sc_opcode::when_signed(
            &alice,
            &bob,
            &budget_address,
            &witness,
            None,
            1,
        );
        let context = Context::new(instructions);
        treasury_client
            .snd_online_context(&[&alice_keypair], context)
            .unwrap();

        // Attack! Part 1: Sign a witness transaction with a random key.
        let mallory_keypair = Keypair::new();
        let mallory = mallory_keypair.address();
        treasury_client
            .online_transfer(1, &alice_keypair, &mallory)
            .unwrap();
        let instruction =
            sc_opcode::apply_signature(&mallory, &budget_address, &bob);
        let mut context = Context::new(vec![instruction]);

        // Attack! Part 2: Point the instruction to the expected, but unsigned, key.
        context.account_keys.insert(3, alice);
        context.instructions[0].accounts[0] = 3;
        context.instructions[0].program_ids_index = 4;

        // Ensure the transaction fails because of the unsigned key.
        assert_eq!(
            treasury_client
                .snd_online_context(&[&mallory_keypair], context)
                .unwrap_err()
                .unwrap(),
            TransactionError::OpCodeErr(0, OpCodeErr::MissingRequiredSignature)
        );
    }

    #[test]
    fn test_unsigned_timestamp() {
        let (treasury, alice_keypair) = create_treasury(10_000);
        let treasury_client = TreasuryClient::new(treasury);
        let alice_address = alice_keypair.address();

        // Initialize BudgetState
        let budget_address = BvmAddr::new_rand();
        let bob_address = BvmAddr::new_rand();
        let dt = Utc::now();
        let instructions = sc_opcode::on_date(
            &alice_address,
            &bob_address,
            &budget_address,
            dt,
            &alice_address,
            None,
            1,
        );
        let context = Context::new(instructions);
        treasury_client
            .snd_online_context(&[&alice_keypair], context)
            .unwrap();

        // Attack! Part 1: Sign a timestamp transaction with a random key.
        let mallory_keypair = Keypair::new();
        let mallory_address = mallory_keypair.address();
        treasury_client
            .online_transfer(1, &alice_keypair, &mallory_address)
            .unwrap();
        let instruction =
            sc_opcode::apply_timestamp(&mallory_address, &budget_address, &bob_address, dt);
        let mut context = Context::new(vec![instruction]);

        // Attack! Part 2: Point the instruction to the expected, but unsigned, key.
        context.account_keys.insert(3, alice_address);
        context.instructions[0].accounts[0] = 3;
        context.instructions[0].program_ids_index = 4;

        // Ensure the transaction fails because of the unsigned key.
        assert_eq!(
            treasury_client
                .snd_online_context(&[&mallory_keypair], context)
                .unwrap_err()
                .unwrap(),
            TransactionError::OpCodeErr(0, OpCodeErr::MissingRequiredSignature)
        );
    }

    #[test]
    fn test_pay_on_date() {
        let (treasury, alice_keypair) = create_treasury(2);
        let treasury_client = TreasuryClient::new(treasury);
        let alice_address = alice_keypair.address();
        let budget_address = BvmAddr::new_rand();
        let bob_address = BvmAddr::new_rand();
        let mallory_address = BvmAddr::new_rand();
        let dt = Utc::now();
        let instructions = sc_opcode::on_date(
            &alice_address,
            &bob_address,
            &budget_address,
            dt,
            &alice_address,
            None,
            1,
        );
        let context = Context::new(instructions);
        treasury_client
            .snd_online_context(&[&alice_keypair], context)
            .unwrap();
        assert_eq!(treasury_client.get_balance(&alice_address).unwrap(), 1);
        assert_eq!(treasury_client.get_balance(&budget_address).unwrap(), 1);

        let contract_account = treasury_client
            .get_account_data(&budget_address)
            .unwrap()
            .unwrap();
        let script_state = BudgetState::deserialize(&contract_account).unwrap();
        assert!(script_state.is_pending());

        // Attack! Try to payout to mallory_address
        let instruction =
            sc_opcode::apply_timestamp(&alice_address, &budget_address, &mallory_address, dt);
        assert_eq!(
            treasury_client
                .snd_online_instruction(&alice_keypair, instruction)
                .unwrap_err()
                .unwrap(),
            TransactionError::OpCodeErr(
                0,
                OpCodeErr::CustomError(BudgetError::DestinationMissing as u32)
            )
        );
        assert_eq!(treasury_client.get_balance(&alice_address).unwrap(), 1);
        assert_eq!(treasury_client.get_balance(&budget_address).unwrap(), 1);
        assert_eq!(treasury_client.get_balance(&bob_address).unwrap(), 0);

        let contract_account = treasury_client
            .get_account_data(&budget_address)
            .unwrap()
            .unwrap();
        let script_state = BudgetState::deserialize(&contract_account).unwrap();
        assert!(script_state.is_pending());

        // Now, acknowledge the time in the condition occurred and
        // that address's funds are now available.
        let instruction =
            sc_opcode::apply_timestamp(&alice_address, &budget_address, &bob_address, dt);
        treasury_client
            .snd_online_instruction(&alice_keypair, instruction)
            .unwrap();
        assert_eq!(treasury_client.get_balance(&alice_address).unwrap(), 1);
        assert_eq!(treasury_client.get_balance(&budget_address).unwrap(), 0);
        assert_eq!(treasury_client.get_balance(&bob_address).unwrap(), 1);
        assert_eq!(treasury_client.get_account_data(&budget_address).unwrap(), None);
    }

    #[test]
    fn test_cancel_payment() {
        let (treasury, alice_keypair) = create_treasury(3);
        let treasury_client = TreasuryClient::new(treasury);
        let alice_address = alice_keypair.address();
        let budget_address = BvmAddr::new_rand();
        let bob_address = BvmAddr::new_rand();
        let dt = Utc::now();

        let instructions = sc_opcode::on_date(
            &alice_address,
            &bob_address,
            &budget_address,
            dt,
            &alice_address,
            Some(alice_address),
            1,
        );
        let context = Context::new(instructions);
        treasury_client
            .snd_online_context(&[&alice_keypair], context)
            .unwrap();
        assert_eq!(treasury_client.get_balance(&alice_address).unwrap(), 2);
        assert_eq!(treasury_client.get_balance(&budget_address).unwrap(), 1);

        let contract_account = treasury_client
            .get_account_data(&budget_address)
            .unwrap()
            .unwrap();
        let script_state = BudgetState::deserialize(&contract_account).unwrap();
        assert!(script_state.is_pending());

        // Attack! try to put the difs into the wrong account with cancel
        let mallory_keypair = Keypair::new();
        let mallory_address = mallory_keypair.address();
        treasury_client
            .online_transfer(1, &alice_keypair, &mallory_address)
            .unwrap();
        assert_eq!(treasury_client.get_balance(&alice_address).unwrap(), 1);

        let instruction =
            sc_opcode::apply_signature(&mallory_address, &budget_address, &bob_address);
        treasury_client
            .snd_online_instruction(&mallory_keypair, instruction)
            .unwrap();
        // nothing should be changed because apply witness didn't finalize a payment
        assert_eq!(treasury_client.get_balance(&alice_address).unwrap(), 1);
        assert_eq!(treasury_client.get_balance(&budget_address).unwrap(), 1);
        assert_eq!(treasury_client.get_account_data(&bob_address).unwrap(), None);

        // Now, cancel the transaction. mint gets her funds back
        let instruction =
            sc_opcode::apply_signature(&alice_address, &budget_address, &alice_address);
        treasury_client
            .snd_online_instruction(&alice_keypair, instruction)
            .unwrap();
        assert_eq!(treasury_client.get_balance(&alice_address).unwrap(), 2);
        assert_eq!(treasury_client.get_account_data(&budget_address).unwrap(), None);
        assert_eq!(treasury_client.get_account_data(&bob_address).unwrap(), None);
    }
}
