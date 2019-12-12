extern crate walkdir;

use std::env;
use std::path::Path;
use std::process::Command;
use walkdir::WalkDir;

fn rerun_if_changed(files: &[&str], directories: &[&str], excludes: &[&str]) {
    let mut all_files: Vec<_> = files.iter().map(|f| f.to_string()).collect();

    for directory in directories {
        let files_in_directory: Vec<_> = WalkDir::new(directory)
            .into_iter()
            .map(|entry| entry.unwrap())
            .filter(|entry| {
                if !entry.file_type().is_file() {
                    return false;
                }
                for exclude in excludes.iter() {
                    if entry.path().to_str().unwrap().contains(exclude) {
                        return false;
                    }
                }
                true
            })
            .map(|f| f.path().to_str().unwrap().to_owned())
            .collect();
        all_files.extend_from_slice(&files_in_directory[..]);
    }

    for file in all_files {
        if !Path::new(&file).is_file() {
            panic!("{} is not a file", file);
        }
        println!("cargo:rerun-if-changed={}", file);
    }
}

fn main() {
    let bpf_c = !env::var("CARGO_FEATURE_BPF_C").is_err();
    if bpf_c {
        let install_dir = "OUT_DIR=../../../target/".to_string()
            + &env::var("PROFILE").unwrap()
            + &"/bpf".to_string();

        println!("cargo:warning=(not a warning) Building C-based BPF programs");
        assert!(Command::new("make")
            .current_dir("c")
            .arg("programs")
            .arg(&install_dir)
            .status()
            .expect("Failed to build C-based BPF programs")
            .success());

        rerun_if_changed(&["c/makefile"], &["c/src", "../../interface"], &["/target/"]);
    }

    let bpf_rust = !env::var("CARGO_FEATURE_BPF_RUST").is_err();
    if bpf_rust {
        let install_dir =
            "../../target/".to_string() + &env::var("PROFILE").unwrap() + &"/bpf".to_string();

        assert!(Command::new("mkdir")
            .arg("-p")
            .arg(&install_dir)
            .status()
            .expect("Unable to create BPF install directory")
            .success());

        let rust_programs = ["alloc", "iter", "noop"];
        for program in rust_programs.iter() {
            println!(
                "cargo:warning=(not a warning) Building Rust-based BPF programs: morgan_bpf_rust_{}",
                program
            );
            assert!(Command::new("./build.sh")
                .current_dir("rust")
                .arg(program)
                .status()
                .expect(&format!(
                    "Failed to call morgan-bpf-rust-{}'s build.sh",
                    program
                ))
                .success());
            let src = format!(
                "rust/{}/target/bpfel-unknown-unknown/release/morgan_bpf_rust_{}.so",
                program, program,
            );
            assert!(Command::new("cp")
                .arg(&src)
                .arg(&install_dir)
                .status()
                .expect(&format!("Failed to cp {} to {}", src, install_dir))
                .success());
        }

        rerun_if_changed(&[], &["rust", "../../interface", &install_dir], &["/target/"]);
    }
}
