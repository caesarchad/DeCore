use crate::bultin_mounter;
use crate::sys_opcode_handler;
use morgan_interface::account::{create_keyed_accounts, Account, KeyedAccount};
use morgan_interface::opcodes::{EncodedOpCodes, OpCodeErr};
use morgan_interface::opcodes_utils;
use morgan_interface::context::Context;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::sys_controller;
use morgan_interface::transaction::TransactionError;
use std::collections::HashMap;
use std::sync::RwLock;
use log::*;

#[cfg(unix)]
use libloading::os::unix::*;
#[cfg(windows)]
use libloading::os::windows::*;

/// Return true if the slice has any duplicate elements
pub fn has_duplicates<T: PartialEq>(xs: &[T]) -> bool {
    // Note: This is an O(n^2) algorithm, but requires no heap allocations. The benchmark
    // `bench_has_duplicates` in benches/context_handler.rs shows that this implementation is
    // ~50 times faster than using HashSet for very short slices.
    for i in 1..xs.len() {
        if xs[i..].contains(&xs[i - 1]) {
            return true;
        }
    }
    false
}

/// Get mut references to a subset of elements.
fn get_subset_unchecked_mut<'a, T>(
    xs: &'a mut [T],
    indexes: &[u8],
) -> Result<Vec<&'a mut T>, OpCodeErr> {
    // Since the compiler doesn't know the indexes are unique, dereferencing
    // multiple mut elements is assumed to be unsafe. If, however, all
    // indexes are unique, it's perfectly safe. The returned elements will share
    // the liftime of the input slice.

    // Make certain there are no duplicate indexes. If there are, return an error
    // because we can't return multiple mut references to the same element.
    if has_duplicates(indexes) {
        return Err(OpCodeErr::DuplicateAccountIndex);
    }

    Ok(indexes
        .iter()
        .map(|i| {
            let ptr = &mut xs[*i as usize] as *mut T;
            unsafe { &mut *ptr }
        })
        .collect())
}

fn check_opcode(
    program_id: &BvmAddr,
    pre_program_id: &BvmAddr,
    pre_difs: u64,
    pre_data: &[u8],
    account: &Account,
) -> Result<(), OpCodeErr> {
    // Verify the transaction

    // Make sure that program_id is still the same or this was just assigned by the system program
    if *pre_program_id != account.owner && !sys_controller::check_id(&program_id) {
        return Err(OpCodeErr::ModifiedProgramId);
    }
    // For accounts unassigned to the program, the individual balance of each accounts cannot decrease.
    if *program_id != account.owner && pre_difs > account.difs {
        return Err(OpCodeErr::ExternalAccountDifSpend);
    }
    // For accounts unassigned to the program, the data may not change.
    if *program_id != account.owner
        && !sys_controller::check_id(&program_id)
        && pre_data != &account.data[..]
    {
        return Err(OpCodeErr::ExternalAccountDataModified);
    }
    Ok(())
}

pub type HandleOpCode =
    fn(&BvmAddr, &mut [KeyedAccount], &[u8], u64) -> Result<(), OpCodeErr>;

pub type SymbolCache = RwLock<HashMap<Vec<u8>, Symbol<opcodes_utils::Entrypoint>>>;

pub struct ContextHandler {
    opcode_handler: Vec<(BvmAddr, HandleOpCode)>,
    symbol_cache: SymbolCache,
}

impl Default for ContextHandler {
    fn default() -> Self {
        let opcode_handler: Vec<(BvmAddr, HandleOpCode)> = vec![(
            sys_controller::id(),
            sys_opcode_handler::handle_opcode,
        )];

        Self {
            opcode_handler,
            symbol_cache: RwLock::new(HashMap::new()),
        }
    }
}

impl ContextHandler {
    /// Add a static entrypoint to intercept intructions before the dynamic loader.
    pub fn add_opcode_handler(
        &mut self,
        program_id: BvmAddr,
        handle_opcode: HandleOpCode,
    ) {
        self.opcode_handler
            .push((program_id, handle_opcode));
    }

    /// Process an instruction
    /// This method calls the instruction's program entrypoint method
    fn handle_opcode(
        &self,
        context: &Context,
        instruction: &EncodedOpCodes,
        executable_accounts: &mut [(BvmAddr, Account)],
        program_accounts: &mut [&mut Account],
        drop_height: u64,
    ) -> Result<(), OpCodeErr> {
        let program_id = instruction.program_id(&context.account_keys);
        let mut keyed_accounts = create_keyed_accounts(executable_accounts);
        let mut keyed_accounts2: Vec<_> = instruction
            .accounts
            .iter()
            .map(|&index| {
                let index = index as usize;
                let key = &context.account_keys[index];
                (key, index < context.header.num_required_signatures as usize)
            })
            .zip(program_accounts.iter_mut())
            .map(|((key, is_signer), account)| KeyedAccount::new(key, is_signer, account))
            .collect();
        keyed_accounts.append(&mut keyed_accounts2);

        for (id, handle_opcode) in &self.opcode_handler {
            if id == program_id {
                return handle_opcode(
                    &program_id,
                    &mut keyed_accounts[1..],
                    &instruction.data,
                    drop_height,
                );
            }
        }

        bultin_mounter::entrypoint(
            &program_id,
            &mut keyed_accounts,
            &instruction.data,
            drop_height,
            &self.symbol_cache,
        )
    }

    /// Execute an instruction
    /// This method calls the instruction's program entrypoint method and verifies that the result of
    /// the call does not violate the treasury's accounting rules.
    /// The accounts are committed back to the treasury only if this function returns Ok(_).
    fn execute_instruction(
        &self,
        context: &Context,
        instruction: &EncodedOpCodes,
        executable_accounts: &mut [(BvmAddr, Account)],
        program_accounts: &mut [&mut Account],
        drop_height: u64,
    ) -> Result<(), OpCodeErr> {
        let program_id = instruction.program_id(&context.account_keys);
        // TODO: the runtime should be checking read/write access to memory
        // we are trusting the hard-coded programs not to clobber or allocate
        let pre_total: u64 = program_accounts.iter().map(|a| a.difs).sum();
        let pre_data: Vec<_> = program_accounts
            .iter_mut()
            .map(|a| (a.owner, a.difs, a.data.clone()))
            .collect();

        self.handle_opcode(
            context,
            instruction,
            executable_accounts,
            program_accounts,
            drop_height,
        )?;

        // Verify the instruction
        for ((pre_program_id, pre_difs, pre_data), post_account) in
            pre_data.iter().zip(program_accounts.iter())
        {
            check_opcode(
                &program_id,
                pre_program_id,
                *pre_difs,
                pre_data,
                post_account,
            )?;
        }
        // The total sum of all the difs in all the accounts cannot change.
        let post_total: u64 = program_accounts.iter().map(|a| a.difs).sum();
        if pre_total != post_total {
            return Err(OpCodeErr::DeficitOpCode);
        }
        Ok(())
    }

    /// Process a message.
    /// This method calls each instruction in the message over the set of loaded Accounts
    /// The accounts are committed back to the treasury only if every instruction succeeds
    pub fn process_message(
        &self,
        context: &Context,
        loaders: &mut [Vec<(BvmAddr, Account)>],
        accounts: &mut [Account],
        drop_height: u64,
    ) -> Result<(), TransactionError> {
        for (instruction_index, instruction) in context.instructions.iter().enumerate() {
            let executable_index = context
                .program_position(instruction.program_ids_index as usize)
                .ok_or(TransactionError::InvalidAccountIndex)?;
            let executable_accounts = &mut loaders[executable_index];
            let mut program_accounts = get_subset_unchecked_mut(accounts, &instruction.accounts)
                .map_err(|err| TransactionError::OpCodeErr(instruction_index as u8, err))?;
            // TODO: `get_subset_unchecked_mut` panics on an index out of bounds if an executable
            // account is also included as a regular account for an instruction, because the
            // executable account is not passed in as part of the accounts slice
            self.execute_instruction(
                context,
                instruction,
                executable_accounts,
                &mut program_accounts,
                drop_height,
            )
            .map_err(|err| TransactionError::OpCodeErr(instruction_index as u8, err))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_duplicates() {
        assert!(!has_duplicates(&[1, 2]));
        assert!(has_duplicates(&[1, 2, 1]));
    }

    #[test]
    fn test_get_subset_unchecked_mut() {
        assert_eq!(
            get_subset_unchecked_mut(&mut [7, 8], &[0]).unwrap(),
            vec![&mut 7]
        );
        assert_eq!(
            get_subset_unchecked_mut(&mut [7, 8], &[0, 1]).unwrap(),
            vec![&mut 7, &mut 8]
        );
    }

    #[test]
    fn test_get_subset_unchecked_mut_duplicate_index() {
        // This panics, because it assumes duplicate detection is done elsewhere.
        assert_eq!(
            get_subset_unchecked_mut(&mut [7, 8], &[0, 0]).unwrap_err(),
            OpCodeErr::DuplicateAccountIndex
        );
    }

    #[test]
    #[should_panic]
    fn test_get_subset_unchecked_mut_out_of_bounds() {
        // This panics, because it assumes bounds validation is done elsewhere.
        get_subset_unchecked_mut(&mut [7, 8], &[2]).unwrap();
    }

    #[test]
    fn test_verify_instruction_change_program_id() {
        fn change_program_id(
            ix: &BvmAddr,
            pre: &BvmAddr,
            post: &BvmAddr,
        ) -> Result<(), OpCodeErr> {
            check_opcode(&ix, &pre, 0, &[], &Account::new(0, 0, 0, post))
        }

        let system_program_id = sys_controller::id();
        let alice_program_id = BvmAddr::new_rand();
        let mallory_program_id = BvmAddr::new_rand();

        assert_eq!(
            change_program_id(&system_program_id, &system_program_id, &alice_program_id),
            Ok(()),
            "system program should be able to change the account owner"
        );
        assert_eq!(
            change_program_id(&mallory_program_id, &system_program_id, &alice_program_id),
            Err(OpCodeErr::ModifiedProgramId),
            "malicious Mallory should not be able to change the account owner"
        );
    }

    #[test]
    fn test_verify_instruction_change_data() {
        fn change_data(program_id: &BvmAddr) -> Result<(), OpCodeErr> {
            let alice_program_id = BvmAddr::new_rand();
            let account = Account::new(0, 0, 0, &alice_program_id);
            check_opcode(&program_id, &alice_program_id, 0, &[42], &account)
        }

        let system_program_id = sys_controller::id();
        let mallory_program_id = BvmAddr::new_rand();

        assert_eq!(
            change_data(&system_program_id),
            Ok(()),
            "system program should be able to change the data"
        );
        assert_eq!(
            change_data(&mallory_program_id),
            Err(OpCodeErr::ExternalAccountDataModified),
            "malicious Mallory should not be able to change the account data"
        );
    }
}
