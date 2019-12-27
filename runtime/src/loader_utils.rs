use crate::treasury_client::TreasuryClient;
use serde::Serialize;
use morgan_interface::account_host::OnlineAccount;
use morgan_interface::opcodes::{AccountMeta, OpCode};
use morgan_interface::mounter_opcode;
use morgan_interface::message::Context;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_interface::sys_opcode;

pub fn load_program(
    treasury_client: &TreasuryClient,
    from_keypair: &Keypair,
    loader_pubkey: &BvmAddr,
    program: Vec<u8>,
) -> BvmAddr {
    let program_keypair = Keypair::new();
    let program_pubkey = program_keypair.pubkey();

    let instruction = sys_opcode::create_account(
        &from_keypair.pubkey(),
        &program_pubkey,
        1,
        program.len() as u64,
        loader_pubkey,
    );
    treasury_client
        .snd_online_instruction(&from_keypair, instruction)
        .unwrap();

    let chunk_size = 256; // Size of chunk just needs to fit into tx
    let mut offset = 0;
    for chunk in program.chunks(chunk_size) {
        let instruction =
            morgan_interface::mounter_opcode::write(&program_pubkey, loader_pubkey, offset, chunk.to_vec());
        let message = Context::new_with_payer(vec![instruction], Some(&from_keypair.pubkey()));
        treasury_client
            .send_online_msg(&[from_keypair, &program_keypair], message)
            .unwrap();
        offset += chunk_size as u32;
    }

    let instruction = morgan_interface::mounter_opcode::finalize(&program_pubkey, loader_pubkey);
    let message = Context::new_with_payer(vec![instruction], Some(&from_keypair.pubkey()));
    treasury_client
        .send_online_msg(&[from_keypair, &program_keypair], message)
        .unwrap();

    program_pubkey
}

// Return an Instruction that invokes `program_id` with `data` and required
// a signature from `from_pubkey`.
pub fn compose_call_opcode<T: Serialize>(
    from_pubkey: BvmAddr,
    program_id: BvmAddr,
    data: &T,
) -> OpCode {
    let account_metas = vec![AccountMeta::new(from_pubkey, true)];
    OpCode::new(program_id, data, account_metas)
}
