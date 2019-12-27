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
    let mut final_payment = None;
    if let Some(ref mut expr) = script_state.pending_budget {
        let key = keyed_accounts[0].signer_key().unwrap();
        expr.apply_witness(&Endorsement::Signature, key);
        final_payment = expr.final_payment();
    }

    if let Some(payment) = final_payment {
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
    let mut final_payment = None;

    if let Some(ref mut expr) = script_state.pending_budget {
        let key = keyed_accounts[0].signer_key().unwrap();
        expr.apply_witness(&Endorsement::Timestamp(dt), key);
        final_payment = expr.final_payment();
    }

    if let Some(payment) = final_payment {
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
            if let Some(payment) = expr.final_payment() {
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
        let alice_pubkey = alice_keypair.pubkey();
        let bob_pubkey = BvmAddr::new_rand();
        let instructions = sc_opcode::payment(&alice_pubkey, &bob_pubkey, 100);
        let context = Context::new(instructions);
        treasury_client
            .snd_online_context(&[&alice_keypair], context)
            .unwrap();
        assert_eq!(treasury_client.get_balance(&bob_pubkey).unwrap(), 100);
    }

    #[test]
    fn test_unsigned_witness_key() {
        let (treasury, alice_keypair) = create_treasury(10_000);
        let treasury_client = TreasuryClient::new(treasury);
        let alice_pubkey = alice_keypair.pubkey();

        // Initialize BudgetState
        let budget_pubkey = BvmAddr::new_rand();
        let bob_pubkey = BvmAddr::new_rand();
        let witness = BvmAddr::new_rand();
        let instructions = sc_opcode::when_signed(
            &alice_pubkey,
            &bob_pubkey,
            &budget_pubkey,
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
        let mallory_pubkey = mallory_keypair.pubkey();
        treasury_client
            .online_transfer(1, &alice_keypair, &mallory_pubkey)
            .unwrap();
        let instruction =
            sc_opcode::apply_signature(&mallory_pubkey, &budget_pubkey, &bob_pubkey);
        let mut context = Context::new(vec![instruction]);

        // Attack! Part 2: Point the instruction to the expected, but unsigned, key.
        context.account_keys.insert(3, alice_pubkey);
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
        let alice_pubkey = alice_keypair.pubkey();

        // Initialize BudgetState
        let budget_pubkey = BvmAddr::new_rand();
        let bob_pubkey = BvmAddr::new_rand();
        let dt = Utc::now();
        let instructions = sc_opcode::on_date(
            &alice_pubkey,
            &bob_pubkey,
            &budget_pubkey,
            dt,
            &alice_pubkey,
            None,
            1,
        );
        let context = Context::new(instructions);
        treasury_client
            .snd_online_context(&[&alice_keypair], context)
            .unwrap();

        // Attack! Part 1: Sign a timestamp transaction with a random key.
        let mallory_keypair = Keypair::new();
        let mallory_pubkey = mallory_keypair.pubkey();
        treasury_client
            .online_transfer(1, &alice_keypair, &mallory_pubkey)
            .unwrap();
        let instruction =
            sc_opcode::apply_timestamp(&mallory_pubkey, &budget_pubkey, &bob_pubkey, dt);
        let mut context = Context::new(vec![instruction]);

        // Attack! Part 2: Point the instruction to the expected, but unsigned, key.
        context.account_keys.insert(3, alice_pubkey);
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
        let alice_pubkey = alice_keypair.pubkey();
        let budget_pubkey = BvmAddr::new_rand();
        let bob_pubkey = BvmAddr::new_rand();
        let mallory_pubkey = BvmAddr::new_rand();
        let dt = Utc::now();
        let instructions = sc_opcode::on_date(
            &alice_pubkey,
            &bob_pubkey,
            &budget_pubkey,
            dt,
            &alice_pubkey,
            None,
            1,
        );
        let context = Context::new(instructions);
        treasury_client
            .snd_online_context(&[&alice_keypair], context)
            .unwrap();
        assert_eq!(treasury_client.get_balance(&alice_pubkey).unwrap(), 1);
        assert_eq!(treasury_client.get_balance(&budget_pubkey).unwrap(), 1);

        let contract_account = treasury_client
            .get_account_data(&budget_pubkey)
            .unwrap()
            .unwrap();
        let script_state = BudgetState::deserialize(&contract_account).unwrap();
        assert!(script_state.is_pending());

        // Attack! Try to payout to mallory_pubkey
        let instruction =
            sc_opcode::apply_timestamp(&alice_pubkey, &budget_pubkey, &mallory_pubkey, dt);
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
        assert_eq!(treasury_client.get_balance(&alice_pubkey).unwrap(), 1);
        assert_eq!(treasury_client.get_balance(&budget_pubkey).unwrap(), 1);
        assert_eq!(treasury_client.get_balance(&bob_pubkey).unwrap(), 0);

        let contract_account = treasury_client
            .get_account_data(&budget_pubkey)
            .unwrap()
            .unwrap();
        let script_state = BudgetState::deserialize(&contract_account).unwrap();
        assert!(script_state.is_pending());

        // Now, acknowledge the time in the condition occurred and
        // that pubkey's funds are now available.
        let instruction =
            sc_opcode::apply_timestamp(&alice_pubkey, &budget_pubkey, &bob_pubkey, dt);
        treasury_client
            .snd_online_instruction(&alice_keypair, instruction)
            .unwrap();
        assert_eq!(treasury_client.get_balance(&alice_pubkey).unwrap(), 1);
        assert_eq!(treasury_client.get_balance(&budget_pubkey).unwrap(), 0);
        assert_eq!(treasury_client.get_balance(&bob_pubkey).unwrap(), 1);
        assert_eq!(treasury_client.get_account_data(&budget_pubkey).unwrap(), None);
    }

    #[test]
    fn test_cancel_payment() {
        let (treasury, alice_keypair) = create_treasury(3);
        let treasury_client = TreasuryClient::new(treasury);
        let alice_pubkey = alice_keypair.pubkey();
        let budget_pubkey = BvmAddr::new_rand();
        let bob_pubkey = BvmAddr::new_rand();
        let dt = Utc::now();

        let instructions = sc_opcode::on_date(
            &alice_pubkey,
            &bob_pubkey,
            &budget_pubkey,
            dt,
            &alice_pubkey,
            Some(alice_pubkey),
            1,
        );
        let context = Context::new(instructions);
        treasury_client
            .snd_online_context(&[&alice_keypair], context)
            .unwrap();
        assert_eq!(treasury_client.get_balance(&alice_pubkey).unwrap(), 2);
        assert_eq!(treasury_client.get_balance(&budget_pubkey).unwrap(), 1);

        let contract_account = treasury_client
            .get_account_data(&budget_pubkey)
            .unwrap()
            .unwrap();
        let script_state = BudgetState::deserialize(&contract_account).unwrap();
        assert!(script_state.is_pending());

        // Attack! try to put the difs into the wrong account with cancel
        let mallory_keypair = Keypair::new();
        let mallory_pubkey = mallory_keypair.pubkey();
        treasury_client
            .online_transfer(1, &alice_keypair, &mallory_pubkey)
            .unwrap();
        assert_eq!(treasury_client.get_balance(&alice_pubkey).unwrap(), 1);

        let instruction =
            sc_opcode::apply_signature(&mallory_pubkey, &budget_pubkey, &bob_pubkey);
        treasury_client
            .snd_online_instruction(&mallory_keypair, instruction)
            .unwrap();
        // nothing should be changed because apply witness didn't finalize a payment
        assert_eq!(treasury_client.get_balance(&alice_pubkey).unwrap(), 1);
        assert_eq!(treasury_client.get_balance(&budget_pubkey).unwrap(), 1);
        assert_eq!(treasury_client.get_account_data(&bob_pubkey).unwrap(), None);

        // Now, cancel the transaction. mint gets her funds back
        let instruction =
            sc_opcode::apply_signature(&alice_pubkey, &budget_pubkey, &alice_pubkey);
        treasury_client
            .snd_online_instruction(&alice_keypair, instruction)
            .unwrap();
        assert_eq!(treasury_client.get_balance(&alice_pubkey).unwrap(), 2);
        assert_eq!(treasury_client.get_account_data(&budget_pubkey).unwrap(), None);
        assert_eq!(treasury_client.get_account_data(&bob_pubkey).unwrap(), None);
    }
}
