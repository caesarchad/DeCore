//! @brief Example Rust-based BPF program that prints out the parameters passed to it

#![no_std]

extern crate morgan_interface_bpf_utils;

use morgan_interface_bpf_utils::entrypoint;
use morgan_interface_bpf_utils::entrypoint::*;
use morgan_interface_bpf_utils::log::*;

struct SStruct {
    x: u64,
    y: u64,
    z: u64,
}

#[inline(never)]
fn return_sstruct() -> SStruct {
    SStruct { x: 1, y: 2, z: 3 }
}

entrypoint!(process_instruction);
fn process_instruction(
    ka: &mut [Option<SolKeyedAccount>; MAX_ACCOUNTS],
    info: &SolClusterInfo,
    data: &[u8],
) -> bool {
    sol_log("Tick height:");
    sol_log_64(info.tick_height, 0, 0, 0, 0);
    sol_log("Program identifier:");
    sol_log_key(&info.program_id);

    // Log the provided account keys and instruction input data.  In the case of
    // the no-op program, no account keys or input data are expected but real
    // programs will have specific requirements so they can do their work.
    sol_log("Account keys and instruction input data:");
    sol_log_params(ka, data);

    {
        // Test - arch config
        #[cfg(not(target_arch = "bpf"))]
        assert!(false);
    }

    {
        // Test - use core methods, unwrap

        // valid bytes, in a stack-allocated array
        let sparkle_heart = [240, 159, 146, 150];
        let result_str = core::str::from_utf8(&sparkle_heart).unwrap();
        assert_eq!(4, result_str.len());
        assert_eq!("💖", result_str);
        sol_log(result_str);
    }

    {
        // Test - struct return

        let s = return_sstruct();
        assert_eq!(s.x + s.y + s.z, 6);
    }

    sol_log("Success");
    true
}
