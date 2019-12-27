use clap::{crate_description, crate_name, crate_version, App, Arg, SubCommand};
use morgan_interface::bvm_address::write_address;
use morgan_interface::signature::{gen_keypair_file, read_keypair, KeypairUtil};
use std::error;

fn main() -> Result<(), Box<dyn error::Error>> {
    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(crate_version!())
        .arg(
            Arg::with_name("outfile")
                .short("o")
                .long("outfile")
                .value_name("PATH")
                .takes_value(true)
                .help("Path to generated file"),
        )
        .subcommand(
            SubCommand::with_name("new")
                .about("Generate new keypair file")
                .arg(
                    Arg::with_name("outfile")
                        .short("o")
                        .long("outfile")
                        .value_name("PATH")
                        .takes_value(true)
                        .help("Path to generated file"),
                ),
        )
        .subcommand(
            SubCommand::with_name("address")
                .about("Display the address from a keypair file")
                .arg(
                    Arg::with_name("infile")
                        .index(1)
                        .value_name("PATH")
                        .takes_value(true)
                        .help("Path to keypair file"),
                )
                .arg(
                    Arg::with_name("outfile")
                        .short("o")
                        .long("outfile")
                        .value_name("PATH")
                        .takes_value(true)
                        .help("Path to generated file"),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        ("address", Some(address_matches)) => {
            let mut path = dirs::home_dir().expect("home directory");
            let infile = if address_matches.is_present("infile") {
                address_matches.value_of("infile").unwrap()
            } else {
                path.extend(&[".config", "morgan", "id.json"]);
                path.to_str().unwrap()
            };
            let keypair = read_keypair(infile)?;

            if address_matches.is_present("outfile") {
                let outfile = address_matches.value_of("outfile").unwrap();
                write_address(outfile, keypair.address())?;
            } else {
                println!("{}", keypair.address());
            }
        }
        match_tuple => {
            let working_matches = if let (_, Some(new_matches)) = match_tuple {
                new_matches
            } else {
                &matches
            };

            let mut path = dirs::home_dir().expect("home directory");
            let outfile = if working_matches.is_present("outfile") {
                working_matches.value_of("outfile").unwrap()
            } else {
                path.extend(&[".config", "morgan", "id.json"]);
                path.to_str().unwrap()
            };

            let serialized_keypair = gen_keypair_file(outfile)?;
            if outfile == "-" {
                println!("{}", serialized_keypair);
            }
        }
    }

    Ok(())
}
