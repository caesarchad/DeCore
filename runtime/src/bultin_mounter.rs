//! Native loader
use crate::context_handler::SymbolCache;
use bincode::deserialize;
#[cfg(unix)]
use libloading::os::unix::*;
#[cfg(windows)]
use libloading::os::windows::*;
use log::*;
use morgan_interface::account::KeyedAccount;
use morgan_interface::opcodes::OpCodeErr;
use morgan_interface::opcodes_utils;
use morgan_interface::mounter_opcode::MounterOpCode;
use morgan_interface::bvm_address::BvmAddr;
use std::env;
use std::path::PathBuf;
use std::str;
use morgan_helper::logHelper::*;

/// Dynamic link library prefixes
#[cfg(unix)]
const PLATFORM_FILE_PREFIX_NATIVE: &str = "lib";
#[cfg(windows)]
const PLATFORM_FILE_PREFIX_NATIVE: &str = "";

/// Dynamic link library file extension specific to the platform
#[cfg(any(target_os = "macos", target_os = "ios"))]
const PLATFORM_FILE_EXTENSION_NATIVE: &str = "dylib";
/// Dynamic link library file extension specific to the platform
#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios"))))]
const PLATFORM_FILE_EXTENSION_NATIVE: &str = "so";
/// Dynamic link library file extension specific to the platform
#[cfg(windows)]
const PLATFORM_FILE_EXTENSION_NATIVE: &str = "dll";

fn create_path(name: &str) -> PathBuf {
    let current_exe = env::current_exe()
        .unwrap_or_else(|e| panic!("create_path(\"{}\"): current exe not found: {:?}", name, e));
    let current_exe_directory = PathBuf::from(current_exe.parent().unwrap_or_else(|| {
        panic!(
            "create_path(\"{}\"): no parent directory of {:?}",
            name, current_exe,
        )
    }));

    let library_file_name = PathBuf::from(PLATFORM_FILE_PREFIX_NATIVE.to_string() + name)
        .with_extension(PLATFORM_FILE_EXTENSION_NATIVE);

    // Check the current_exe directory for the library as `cargo tests` are run
    // from the deps/ subdirectory
    let file_path = current_exe_directory.join(&library_file_name);
    if file_path.exists() {
        file_path
    } else {
        // `cargo build` places dependent libraries in the deps/ subdirectory
        current_exe_directory.join("deps").join(library_file_name)
    }
}

pub fn entrypoint(
    program_id: &BvmAddr,
    keyed_accounts: &mut [KeyedAccount],
    ix_data: &[u8],
    drop_height: u64,
    symbol_cache: &SymbolCache,
) -> Result<(), OpCodeErr> {
    if keyed_accounts[0].account.executable {
        // dispatch it
        let (names, params) = keyed_accounts.split_at_mut(1);
        let name_vec = &names[0].account.data;
        if let Some(entrypoint) = symbol_cache.read().unwrap().get(name_vec) {
            unsafe {
                return entrypoint(program_id, params, ix_data, drop_height);
            }
        }
        let name = match str::from_utf8(name_vec) {
            Ok(v) => v,
            Err(e) => {
                // warn!("Invalid UTF-8 sequence: {}", e);
                println!("{}",Warn(format!("Invalid UTF-8 sequence: {}", e).to_string(),module_path!().to_string()));
                return Err(OpCodeErr::GenericError);
            }
        };
        trace!("Call native {:?}", name);
        let path = create_path(&name);
        // TODO linux tls bug can cause crash on dlclose(), workaround by never unloading
        match Library::open(Some(&path), libc::RTLD_NODELETE | libc::RTLD_NOW) {
            Ok(library) => unsafe {
                let entrypoint: Symbol<opcodes_utils::Entrypoint> =
                    match library.get(opcodes_utils::ENTRYPOINT.as_bytes()) {
                        Ok(s) => s,
                        Err(e) => {
                            // warn!(
                            //     "{:?}: Unable to find {:?} in program",
                            //     e,
                            //     opcodes_utils::ENTRYPOINT
                            // );
                            println!(
                                "{}",
                                Warn(format!("{:?}: Unable to find {:?} in program",
                                    e,
                                    opcodes_utils::ENTRYPOINT).to_string(),
                                module_path!().to_string())
                            );

                            return Err(OpCodeErr::GenericError);
                        }
                    };
                let ret = entrypoint(program_id, params, ix_data, drop_height);
                symbol_cache
                    .write()
                    .unwrap()
                    .insert(name_vec.to_vec(), entrypoint);
                return ret;
            },
            Err(e) => {
                // warn!("Unable to load: {:?}", e);
                println!(
                    "{}",
                    Warn(format!("Unable to load: {:?}", e).to_string(),
                    module_path!().to_string())
                );
                return Err(OpCodeErr::GenericError);
            }
        }
    } else if let Ok(instruction) = deserialize(ix_data) {
        if keyed_accounts[0].signer_key().is_none() {
            // warn!("key[0] did not sign the transaction");
            println!(
                "{}",
                Warn(format!("key[0] did not sign the transaction").to_string(),
                module_path!().to_string())
            );
            return Err(OpCodeErr::GenericError);
        }
        match instruction {
            MounterOpCode::Write { offset, bytes } => {
                trace!("NativeLoader::Write offset {} bytes {:?}", offset, bytes);
                let offset = offset as usize;
                if keyed_accounts[0].account.data.len() < offset + bytes.len() {
                    // warn!(
                    //     "Error: Overflow, {} < {}",
                    //     keyed_accounts[0].account.data.len(),
                    //     offset + bytes.len()
                    // );
                    println!(
                        "{}",
                        Warn(
                            format!("Error: Overflow, {} < {}",
                                keyed_accounts[0].account.data.len(),
                                offset + bytes.len()).to_string(),
                            module_path!().to_string())
                    );
                    return Err(OpCodeErr::GenericError);
                }
                // native loader takes a name and we assume it all comes in at once
                keyed_accounts[0].account.data = bytes;
            }

            MounterOpCode::Finalize => {
                keyed_accounts[0].account.executable = true;
                trace!(
                    "NativeLoader::Finalize prog: {:?}",
                    keyed_accounts[0].signer_key().unwrap()
                );
            }
        }
    } else {
        // warn!("Invalid data in instruction: {:?}", ix_data);
        println!(
            "{}",
            Warn(
                format!("Invalid data in instruction: {:?}", ix_data).to_string(),
                module_path!().to_string())
        );
        return Err(OpCodeErr::GenericError);
    }
    Ok(())
}
