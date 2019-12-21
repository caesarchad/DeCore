use morgan_runtime::treasury::Treasury;
use morgan_runtime::treasury_client::TreasuryClient;
use morgan_runtime::loader_utils::{compose_call_opcode, load_program};
use morgan_interface::account_host::OnlineAccount;
use morgan_interface::genesis_block::create_genesis_block;
use morgan_interface::bultin_mounter;
use morgan_interface::signature::KeypairUtil;

#[test]
fn test_program_native_noop() {
    morgan_logger::setup();

    let (genesis_block, alice_keypair) = create_genesis_block(50);
    let treasury = Treasury::new(&genesis_block);
    let treasury_client = TreasuryClient::new(treasury);

    let program = "morgan_noop_controller".as_bytes().to_vec();
    let program_id = load_program(&treasury_client, &alice_keypair, &bultin_mounter::id(), program);

    // Call user program
    let instruction = compose_call_opcode(alice_keypair.pubkey(), program_id, &1u8);
    treasury_client
        .snd_online_instruction(&alice_keypair, instruction)
        .unwrap();
}
