use crate::treasury_client::TreasuryClient;
use serde::Serialize;
use morgan_interface::account_host::OnlineAccount;
use morgan_interface::opcodes::{AccountMeta, OpCode};
use morgan_interface::mounter_opcode;
use morgan_interface::context::Context;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_interface::sys_opcode;

pub fn load_program(
    treasury_client: &TreasuryClient,
    from_acct: &Keypair,
    ldr_addr: &BvmAddr,
    program: Vec<u8>,
) -> BvmAddr {
    let program_keypair = Keypair::new();
    let pgm_addr = program_keypair.address();

    let instruction = sys_opcode::create_account(
        &from_acct.address(),
        &pgm_addr,
        1,
        program.len() as u64,
        ldr_addr,
    );
    treasury_client
        .snd_online_instruction(&from_acct, instruction)
        .unwrap();

    let chunk_size = 256; // Size of chunk just needs to fit into tx
    let mut offset = 0;
    for chunk in program.chunks(chunk_size) {
        let instruction =
            morgan_interface::mounter_opcode::write(&pgm_addr, ldr_addr, offset, chunk.to_vec());
        let context = Context::new_with_payer(vec![instruction], Some(&from_acct.address()));
        treasury_client
            .snd_online_context(&[from_acct, &program_keypair], context)
            .unwrap();
        offset += chunk_size as u32;
    }

    let instruction = morgan_interface::mounter_opcode::finalize(&pgm_addr, ldr_addr);
    let context = Context::new_with_payer(vec![instruction], Some(&from_acct.address()));
    treasury_client
        .snd_online_context(&[from_acct, &program_keypair], context)
        .unwrap();

    pgm_addr
}

// Return an Instruction that invokes `program_id` with `data` and required
// a signature from `from_address`.
pub fn compose_call_opcode<T: Serialize>(
    from_address: BvmAddr,
    program_id: BvmAddr,
    data: &T,
) -> OpCode {
    let account_metas = vec![AccountMeta::new(from_address, true)];
    OpCode::new(program_id, data, account_metas)
}
