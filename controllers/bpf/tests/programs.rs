#[cfg(any(feature = "bpf_c", feature = "bpf_rust"))]
mod bpf {
    use morgan_runtime::treasury::Treasury;
    use morgan_runtime::treasury_client::TreasuryClient;
    use morgan_runtime::loader_utils::load_program;
    use morgan_interface::genesis_block::create_genesis_block;
    use morgan_interface::bultin_mounter;
    use std::env;
    use std::fs::File;
    use std::path::PathBuf;

    /// BPF program file extension
    const PLATFORM_FILE_EXTENSION_BPF: &str = "so";

    /// Create a BPF program file name
    fn create_bpf_path(name: &str) -> PathBuf {
        let mut pathbuf = {
            let current_exe = env::current_exe().unwrap();
            PathBuf::from(current_exe.parent().unwrap().parent().unwrap())
        };
        pathbuf.push("bpf/");
        pathbuf.push(name);
        pathbuf.set_extension(PLATFORM_FILE_EXTENSION_BPF);
        pathbuf
    }

    #[cfg(feature = "bpf_c")]
    mod bpf_c {
        use super::*;
        use morgan_runtime::loader_utils::compose_call_opcode;
        use morgan_interface::bvm_loader;
        use morgan_interface::account_host::OnlineAccount;
        use morgan_interface::signature::KeypairUtil;
        use std::io::Read;

        #[test]
        fn test_program_bpf_c_noop() {
            morgan_logger::setup();

            let mut file = File::open(create_bpf_path("noop")).expect("file open failed");
            let mut elf = Vec::new();
            file.read_to_end(&mut elf).unwrap();

            let (genesis_block, alice_keypair) = create_genesis_block(50);
            let treasury = Treasury::new(&genesis_block);
            let treasury_client = TreasuryClient::new(treasury);

            // Call user program
            let program_id = load_program(&treasury_client, &alice_keypair, &bvm_loader::id(), elf);
            let instruction = compose_call_opcode(alice_keypair.address(), program_id, &1u8);
            treasury_client
                .snd_online_instruction(&alice_keypair, instruction)
                .unwrap();
        }

        #[test]
        fn test_program_bpf_c() {
            morgan_logger::setup();

            let programs = [
                "bpf_to_bpf",
                "multiple_static",
                "noop",
                "noop++",
                "relative_call",
                "struct_pass",
                "struct_ret",
            ];
            for program in programs.iter() {
                println!("Test program: {:?}", program);
                let mut file = File::open(create_bpf_path(program)).expect("file open failed");
                let mut elf = Vec::new();
                file.read_to_end(&mut elf).unwrap();

                let (genesis_block, alice_keypair) = create_genesis_block(50);
                let treasury = Treasury::new(&genesis_block);
                let treasury_client = TreasuryClient::new(treasury);

                let ldr_addr = load_program(
                    &treasury_client,
                    &alice_keypair,
                    &bultin_mounter::id(),
                    "morgan_bpf_loader".as_bytes().to_vec(),
                );

                // Call user program
                let program_id = load_program(&treasury_client, &alice_keypair, &ldr_addr, elf);
                let instruction =
                    compose_call_opcode(alice_keypair.address(), program_id, &1u8);
                treasury_client
                    .snd_online_instruction(&alice_keypair, instruction)
                    .unwrap();
            }
        }
    }

    #[cfg(feature = "bpf_rust")]
    mod bpf_rust {
        use super::*;
        use morgan_interface::account_host::OnlineAccount;
        use morgan_interface::opcodes::{AccountMeta, OpCode};
        use morgan_interface::signature::{Keypair, KeypairUtil};
        use std::io::Read;

        #[test]
        fn test_program_bpf_rust() {
            morgan_logger::setup();

            let programs = [
                "morgan_bpf_rust_alloc",
                // Disable due to #4271 "morgan_bpf_rust_iter",
                "morgan_bpf_rust_noop",
            ];
            for program in programs.iter() {
                let filename = create_bpf_path(program);
                println!("Test program: {:?} from {:?}", program, filename);
                let mut file = File::open(filename).unwrap();
                let mut elf = Vec::new();
                file.read_to_end(&mut elf).unwrap();

                let (genesis_block, alice_keypair) = create_genesis_block(50);
                let treasury = Treasury::new(&genesis_block);
                let treasury_client = TreasuryClient::new(treasury);

                let ldr_addr = load_program(
                    &treasury_client,
                    &alice_keypair,
                    &bultin_mounter::id(),
                    "morgan_bpf_loader".as_bytes().to_vec(),
                );

                // Call user program
                let program_id = load_program(&treasury_client, &alice_keypair, &ldr_addr, elf);
                let account_metas = vec![
                    AccountMeta::new(alice_keypair.address(), true),
                    AccountMeta::new(Keypair::new().address(), false),
                ];
                let instruction = OpCode::new(program_id, &1u8, account_metas);
                treasury_client
                    .snd_online_instruction(&alice_keypair, instruction)
                    .unwrap();
            }
        }
    }
}
