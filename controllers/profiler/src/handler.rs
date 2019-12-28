//! Config program

use log::*;
use morgan_interface::account::KeyedAccount;
use morgan_interface::opcodes::OpCodeErr;
use morgan_interface::bvm_address::BvmAddr;
use morgan_helper::logHelper::*;

pub fn handle_opcode(
    _program_id: &BvmAddr,
    keyed_accounts: &mut [KeyedAccount],
    data: &[u8],
    _drop_height: u64,
) -> Result<(), OpCodeErr> {
    if keyed_accounts[0].signer_key().is_none() {
        // error!("{}", Error(format!("account[0].signer_key().is_none()").to_string()));
        println!(
            "{}",
            Error(
                format!("account[0].signer_key().is_none()").to_string(),
                module_path!().to_string()
            )
        );
        Err(OpCodeErr::MissingRequiredSignature)?;
    }

    if keyed_accounts[0].account.data.len() < data.len() {
        // error!("{}", Error(format!("instruction data too large").to_string()));
        println!(
            "{}",
            Error(
                format!("instruction data too large").to_string(),
                module_path!().to_string()
            )
        );
        Err(OpCodeErr::BadOpCodeContext)?;
    }

    keyed_accounts[0].account.data[0..data.len()].copy_from_slice(data);
    Ok(())
}



#[cfg(test)]
mod tests {
    use super::*;
    use crate::pub_func::*;
    use crate::pgm_id::{id, ConfigState,};
    use bincode::{deserialize, serialized_size};
    use serde_derive::{Deserialize, Serialize};
    use morgan_runtime::treasury::Treasury;
    use morgan_runtime::treasury_client::TreasuryClient;
    use morgan_interface::account_host::OnlineAccount;
    use morgan_interface::genesis_block::create_genesis_block;
    use morgan_interface::context::Context;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::sys_opcode;

    #[derive(Serialize, Deserialize, Default, Debug, PartialEq)]
    struct MyConfig {
        pub item: u64,
    }
    impl MyConfig {
        pub fn new(item: u64) -> Self {
            Self { item }
        }
        pub fn deserialize(input: &[u8]) -> Option<Self> {
            deserialize(input).ok()
        }
    }

    impl ConfigState for MyConfig {
        fn max_space() -> u64 {
            serialized_size(&Self::default()).unwrap()
        }
    }

    fn create_treasury(difs: u64) -> (Treasury, Keypair) {
        let (genesis_block, mint_keypair) = create_genesis_block(difs);
        let mut treasury = Treasury::new(&genesis_block);
        treasury.add_opcode_handler(id(), handle_opcode);
        (treasury, mint_keypair)
    }

    fn create_config_account(treasury: Treasury, mint_keypair: &Keypair) -> (TreasuryClient, Keypair) {
        let config_keypair = Keypair::new();
        let config_address = config_keypair.address();

        let treasury_client = TreasuryClient::new(treasury);
        treasury_client
            .snd_online_instruction(
                mint_keypair,
                profile_account::<MyConfig>(
                    &mint_keypair.address(),
                    &config_address,
                    1,
                ),
            )
            .expect("new_account");

        (treasury_client, config_keypair)
    }

    #[test]
    fn test_process_create_ok() {
        morgan_logger::setup();
        let (treasury, mint_keypair) = create_treasury(10_000);
        let (treasury_client, config_keypair) = create_config_account(treasury, &mint_keypair);
        let config_account_data = treasury_client
            .get_account_data(&config_keypair.address())
            .unwrap()
            .unwrap();
        assert_eq!(
            MyConfig::default(),
            MyConfig::deserialize(&config_account_data).unwrap()
        );
    }

    #[test]
    fn test_process_store_ok() {
        morgan_logger::setup();
        let (treasury, mint_keypair) = create_treasury(10_000);
        let (treasury_client, config_keypair) = create_config_account(treasury, &mint_keypair);
        let config_address = config_keypair.address();

        let my_config = MyConfig::new(42);

        let instruction = populate_with(&config_address, &my_config);
        let context = Context::new_with_payer(vec![instruction], Some(&mint_keypair.address()));
        treasury_client
            .snd_online_context(&[&mint_keypair, &config_keypair], context)
            .unwrap();

        let config_account_data = treasury_client
            .get_account_data(&config_address)
            .unwrap()
            .unwrap();
        assert_eq!(
            my_config,
            MyConfig::deserialize(&config_account_data).unwrap()
        );
    }

    #[test]
    fn test_process_store_fail_instruction_data_too_large() {
        morgan_logger::setup();
        let (treasury, mint_keypair) = create_treasury(10_000);
        let (treasury_client, config_keypair) = create_config_account(treasury, &mint_keypair);
        let config_address = config_keypair.address();

        let my_config = MyConfig::new(42);

        let mut instruction = populate_with(&config_address, &my_config);
        instruction.data = vec![0; 123]; // <-- Replace data with a vector that's too large
        let context = Context::new(vec![instruction]);
        treasury_client
            .snd_online_context(&[&config_keypair], context)
            .unwrap_err();
    }

    #[test]
    fn test_process_store_fail_account0_not_signer() {
        morgan_logger::setup();
        let (treasury, mint_keypair) = create_treasury(10_000);
        let system_keypair = Keypair::new();
        let system_address = system_keypair.address();

        treasury.transfer(42, &mint_keypair, &system_address).unwrap();
        let (treasury_client, config_keypair) = create_config_account(treasury, &mint_keypair);
        let config_address = config_keypair.address();

        let transfer_instruction =
            sys_opcode::transfer(&system_address, &BvmAddr::new_rand(), 42);
        let my_config = MyConfig::new(42);
        let mut store_instruction = populate_with(&config_address, &my_config);
        store_instruction.accounts[0].is_signer = false; // <----- not a signer

        let context = Context::new(vec![transfer_instruction, store_instruction]);
        treasury_client
            .snd_online_context(&[&system_keypair], context)
            .unwrap_err();
    }
}
