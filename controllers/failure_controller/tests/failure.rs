use morgan_runtime::treasury::Treasury;
use morgan_runtime::treasury_client::TreasuryClient;
use morgan_runtime::loader_utils::{create_invoke_instruction, load_program};
use morgan_interface::account_host::OnlineAccount;
use morgan_interface::genesis_block::create_genesis_block;
use morgan_interface::instruction::InstructionError;
use morgan_interface::native_loader;
use morgan_interface::signature::KeypairUtil;
use morgan_interface::transaction::TransactionError;

#[test]
fn test_program_native_failure() {
    let (genesis_block, alice_keypair) = create_genesis_block(50);
    let treasury = Treasury::new(&genesis_block);
    let treasury_client = TreasuryClient::new(treasury);

    let program = "morgan_failure_program".as_bytes().to_vec();
    let program_id = load_program(&treasury_client, &alice_keypair, &native_loader::id(), program);

    // Call user program
    let instruction = create_invoke_instruction(alice_keypair.pubkey(), program_id, &1u8);
    assert_eq!(
        treasury_client
            .snd_online_instruction(&alice_keypair, instruction)
            .unwrap_err()
            .unwrap(),
        TransactionError::InstructionError(0, InstructionError::GenericError)
    );
}
