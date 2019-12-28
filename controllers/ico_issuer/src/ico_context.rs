use log::*;
use num_derive::FromPrimitive;
use serde_derive::{Deserialize, Serialize};
use morgan_interface::account::KeyedAccount;
use morgan_interface::opcodes_utils::DecodeError;
use morgan_interface::bvm_address::BvmAddr;
use morgan_helper::logHelper::*;

#[derive(Serialize, Debug, PartialEq, FromPrimitive)]
pub enum IcoErr {
    InvalidArgument,
    InsufficentFunds,
    NotOwner,
}

impl<T> DecodeError<T> for IcoErr {
    fn type_of(&self) -> &'static str {
        "IcoErr"
    }
}

impl std::fmt::Display for IcoErr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "error")
    }
}
impl std::error::Error for IcoErr {}

pub type Result<T> = std::result::Result<T, IcoErr>;

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct IcoInfo {
    supply: u64,

    decimals: u8,

    name: String,

    symbol: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct IcoAgent {
    genesis: BvmAddr,

    original_amount: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct TkActInfo {
    token: BvmAddr,

    owner: BvmAddr,

    amount: u64,

    agent_acct: Option<IcoAgent>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum IcoOpCode {
    NewICO(IcoInfo),
    NewIcoAcct,
    MoveToken(u64),
    SetALW(u64),
    SetOwner,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum IcoContext {
    Unallocated,
    Token(IcoInfo),
    Account(TkActInfo),
    Invalid,
}
impl Default for IcoContext {
    fn default() -> IcoContext {
        IcoContext::Unallocated
    }
}

impl IcoContext {
    #[allow(clippy::needless_pass_by_value)]
    fn map_to_invalid_args(err: std::boxed::Box<bincode::ErrorKind>) -> IcoErr {
        
        println!(
            "{}",
            Warn(
                format!("invalid argument: {:?}", err).to_string(),
                module_path!().to_string()
            )
        );
        IcoErr::InvalidArgument
    }

    pub fn deserialize(input: &[u8]) -> Result<IcoContext> {
        if input.is_empty() {
            Err(IcoErr::InvalidArgument)?;
        }

        match input[0] {
            0 => Ok(IcoContext::Unallocated),
            1 => Ok(IcoContext::Token(
                bincode::deserialize(&input[1..]).map_err(Self::map_to_invalid_args)?,
            )),
            2 => Ok(IcoContext::Account(
                bincode::deserialize(&input[1..]).map_err(Self::map_to_invalid_args)?,
            )),
            _ => Err(IcoErr::InvalidArgument),
        }
    }

    fn serialize(self: &IcoContext, output: &mut [u8]) -> Result<()> {
        if output.is_empty() {
            
            println!(
                "{}",
                Warn(
                    format!("serialize fail: ouput.len is 0").to_string(),
                    module_path!().to_string()
                )
            );
            Err(IcoErr::InvalidArgument)?;
        }
        match self {
            IcoContext::Unallocated | IcoContext::Invalid => Err(IcoErr::InvalidArgument),
            IcoContext::Token(ico_info) => {
                output[0] = 1;
                let writer = std::io::BufWriter::new(&mut output[1..]);
                bincode::serialize_into(writer, &ico_info).map_err(Self::map_to_invalid_args)
            }
            IcoContext::Account(account_info) => {
                output[0] = 2;
                let writer = std::io::BufWriter::new(&mut output[1..]);
                bincode::serialize_into(writer, &account_info).map_err(Self::map_to_invalid_args)
            }
        }
    }

    #[allow(dead_code)]
    pub fn amount(&self) -> Result<u64> {
        if let IcoContext::Account(account_info) = self {
            Ok(account_info.amount)
        } else {
            Err(IcoErr::InvalidArgument)
        }
    }

    #[allow(dead_code)]
    pub fn only_owner(&self, key: &BvmAddr) -> Result<()> {
        if *key != BvmAddr::default() {
            if let IcoContext::Account(account_info) = self {
                if account_info.owner == *key {
                    return Ok(());
                }
            }
        }
        
        println!(
            "{}",
            Warn(
                format!("IcoContext: non-owner rejected").to_string(),
                module_path!().to_string()
            )
        );
        Err(IcoErr::NotOwner)
    }

    pub fn issue_ico_cmd(
        info: &mut [KeyedAccount],
        ico_info: IcoInfo,
        in_accts: &[IcoContext],
        out_accts: &mut Vec<(usize, IcoContext)>,
    ) -> Result<()> {
        if in_accts.len() != 2 {
            
            println!(
                "{}",
                Error(
                    format!("Expected 2 accounts").to_string(),
                    module_path!().to_string()
                )
            );
            Err(IcoErr::InvalidArgument)?;
        }

        if let IcoContext::Account(dest_account) = &in_accts[1] {
            if info[0].signer_key().unwrap() != &dest_account.token {
                
                println!(
                    "{}",
                    Error(
                        format!("account 1 token mismatch").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(IcoErr::InvalidArgument)?;
            }

            if dest_account.agent_acct.is_some() {
                
                println!(
                    "{}",
                    Error(
                        format!("account 1 is a agent_acct and cannot accept tokens").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(IcoErr::InvalidArgument)?;
            }

            let mut output_dest_account = dest_account.clone();
            output_dest_account.amount = ico_info.supply;
            out_accts.push((1, IcoContext::Account(output_dest_account)));
        } else {
            
            println!(
                "{}",
                Error(
                    format!("account 1 invalid").to_string(),
                    module_path!().to_string()
                )
            );
            Err(IcoErr::InvalidArgument)?;
        }

        if in_accts[0] != IcoContext::Unallocated {
            
            println!(
                "{}",
                Error(
                    format!("account 0 not available").to_string(),
                    module_path!().to_string()
                )
            );
            Err(IcoErr::InvalidArgument)?;
        }
        out_accts.push((0, IcoContext::Token(ico_info)));
        Ok(())
    }

    pub fn crate_ico_acct(
        info: &mut [KeyedAccount],
        in_accts: &[IcoContext],
        out_accts: &mut Vec<(usize, IcoContext)>,
    ) -> Result<()> {
        // key 0 - Destination new token account
        // key 1 - Owner of the account
        // key 2 - Token this account is associated with
        // key 3 - Source account that this account is a agent_acct for (optional)
        if in_accts.len() < 3 {
            // error!("{}", Error(format!("Expected 3 accounts").to_string()));
            println!(
                "{}",
                Error(
                    format!("Expected 3 accounts").to_string(),
                    module_path!().to_string()
                )
            );
            Err(IcoErr::InvalidArgument)?;
        }
        if in_accts[0] != IcoContext::Unallocated {
            
            println!(
                "{}",
                Error(
                    format!("account 0 is already allocated").to_string(),
                    module_path!().to_string()
                )
            );
            Err(IcoErr::InvalidArgument)?;
        }
        let mut token_account_info = TkActInfo {
            token: *info[2].unsigned_key(),
            owner: *info[1].unsigned_key(),
            amount: 0,
            agent_acct: None,
        };
        if in_accts.len() >= 4 {
            token_account_info.agent_acct = Some(IcoAgent {
                genesis: *info[3].unsigned_key(),
                original_amount: 0,
            });
        }
        out_accts.push((0, IcoContext::Account(token_account_info)));
        Ok(())
    }

    pub fn transfer_token(
        info: &mut [KeyedAccount],
        amount: u64,
        in_accts: &[IcoContext],
        out_accts: &mut Vec<(usize, IcoContext)>,
    ) -> Result<()> {
        if in_accts.len() < 3 {
            
            println!(
                "{}",
                Error(
                    format!("Expected 3 accounts").to_string(),
                    module_path!().to_string()
                )
            );
            Err(IcoErr::InvalidArgument)?;
        }

        if let (IcoContext::Account(src_acct), IcoContext::Account(dest_account)) =
            (&in_accts[1], &in_accts[2])
        {
            if src_acct.token != dest_account.token {
                
                println!(
                    "{}",
                    Error(
                        format!("account 1/2 token mismatch").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(IcoErr::InvalidArgument)?;
            }

            if dest_account.agent_acct.is_some() {
                
                println!(
                    "{}",
                    Error(
                        format!("account 2 is a agent_acct and cannot accept tokens").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(IcoErr::InvalidArgument)?;
            }

            if info[0].signer_key().unwrap() != &src_acct.owner {
                
                println!(
                    "{}",
                    Error(
                        format!("owner of account 1 not present").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(IcoErr::InvalidArgument)?;
            }

            if src_acct.amount < amount {
                Err(IcoErr::InsufficentFunds)?;
            }

            let mut out_src_acct = src_acct.clone();
            out_src_acct.amount -= amount;
            out_accts.push((1, IcoContext::Account(out_src_acct)));

            if let Some(ref agent_acct_info) = src_acct.agent_acct {
                if in_accts.len() != 4 {
                    
                    println!(
                        "{}",
                        Error(
                            format!("Expected 4 accounts").to_string(),
                            module_path!().to_string()
                        )
                    );
                    Err(IcoErr::InvalidArgument)?;
                }

                let agent_account = src_acct;
                if let IcoContext::Account(src_acct) = &in_accts[3] {
                    if src_acct.token != agent_account.token {
                        
                        println!(
                            "{}",
                            Error(
                                format!("account 1/3 token mismatch").to_string(),
                                module_path!().to_string()
                            )
                        );
                        Err(IcoErr::InvalidArgument)?;
                    }
                    if info[3].unsigned_key() != &agent_acct_info.genesis {
                        
                        println!(
                            "{}",
                            Error(
                                format!("Account 1 is not a agent_acct of account 3").to_string(),
                                module_path!().to_string()
                            )
                        );
                        Err(IcoErr::InvalidArgument)?;
                    }

                    if src_acct.amount < amount {
                        Err(IcoErr::InsufficentFunds)?;
                    }

                    let mut out_src_acct = src_acct.clone();
                    out_src_acct.amount -= amount;
                    out_accts.push((3, IcoContext::Account(out_src_acct)));
                } else {
                    
                    println!(
                        "{}",
                        Error(
                            format!("account 3 is an invalid account").to_string(),
                            module_path!().to_string()
                        )
                    );
                    Err(IcoErr::InvalidArgument)?;
                }
            }

            let mut output_dest_account = dest_account.clone();
            output_dest_account.amount += amount;
            out_accts.push((2, IcoContext::Account(output_dest_account)));
        } else {
            
            println!(
                "{}",
                Error(
                    format!("account 1 and/or 2 are invalid accounts").to_string(),
                    module_path!().to_string()
                )
            );
            Err(IcoErr::InvalidArgument)?;
        }
        Ok(())
    }

    pub fn set_allowance(
        info: &mut [KeyedAccount],
        amount: u64,
        in_accts: &[IcoContext],
        out_accts: &mut Vec<(usize, IcoContext)>,
    ) -> Result<()> {
        if in_accts.len() != 3 {
            
            println!(
                "{}",
                Error(
                    format!("Expected 3 accounts").to_string(),
                    module_path!().to_string()
                )
            );
            Err(IcoErr::InvalidArgument)?;
        }

        if let (IcoContext::Account(src_acct), IcoContext::Account(agent_account)) =
            (&in_accts[1], &in_accts[2])
        {
            if src_acct.token != agent_account.token {
                
                println!(
                    "{}",
                    Error(
                        format!("account 1/2 token mismatch").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(IcoErr::InvalidArgument)?;
            }

            if info[0].signer_key().unwrap() != &src_acct.owner {
                
                println!(
                    "{}",
                    Error(
                        format!("owner of account 1 not present").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(IcoErr::InvalidArgument)?;
            }

            if src_acct.agent_acct.is_some() {
                
                println!(
                    "{}",
                    Error(
                        format!("account 1 is a agent_acct").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(IcoErr::InvalidArgument)?;
            }

            match &agent_account.agent_acct {
                None => {
                    
                    println!(
                        "{}",
                        Error(
                            format!("account 2 is not a agent_acct").to_string(),
                            module_path!().to_string()
                        )
                    );
                    Err(IcoErr::InvalidArgument)?;
                }
                Some(agent_acct_info) => {
                    if info[1].unsigned_key() != &agent_acct_info.genesis {
                        
                        println!(
                            "{}",
                            Error(
                                format!("account 2 is not a agent_acct of account 1").to_string(),
                                module_path!().to_string()
                            )
                        );
                        Err(IcoErr::InvalidArgument)?;
                    }

                    let mut output_delegate_account = agent_account.clone();
                    output_delegate_account.amount = amount;
                    output_delegate_account.agent_acct = Some(IcoAgent {
                        genesis: agent_acct_info.genesis,
                        original_amount: amount,
                    });
                    out_accts.push((2, IcoContext::Account(output_delegate_account)));
                }
            }
        } else {
            
            println!(
                "{}",
                Error(
                    format!("account 1 and/or 2 are invalid accounts").to_string(),
                    module_path!().to_string()
                )
            );
            Err(IcoErr::InvalidArgument)?;
        }
        Ok(())
    }

    pub fn set_ico_owner(
        info: &mut [KeyedAccount],
        in_accts: &[IcoContext],
        out_accts: &mut Vec<(usize, IcoContext)>,
    ) -> Result<()> {
        if in_accts.len() < 3 {
            
            println!(
                "{}",
                Error(
                    format!("Expected 3 accounts").to_string(),
                    module_path!().to_string()
                )
            );
            Err(IcoErr::InvalidArgument)?;
        }

        if let IcoContext::Account(src_acct) = &in_accts[1] {
            if info[0].signer_key().unwrap() != &src_acct.owner {
                
                let info:String = format!("owner of account 1 not present").to_string();
                println!("{}",
                    printLn(
                        info,
                        module_path!().to_string()
                    )
                );

                Err(IcoErr::InvalidArgument)?;
            }

            let mut out_src_acct = src_acct.clone();
            out_src_acct.owner = *info[2].unsigned_key();
            out_accts.push((1, IcoContext::Account(out_src_acct)));
        } else {
            
            let info:String = format!("account 1 is invalid").to_string();
            println!("{}",
                printLn(
                    info,
                    module_path!().to_string()
                )
            );
            Err(IcoErr::InvalidArgument)?;
        }
        Ok(())
    }

    pub fn process(program_id: &BvmAddr, info: &mut [KeyedAccount], input: &[u8]) -> Result<()> {
        let command =
            bincode::deserialize::<IcoOpCode>(input).map_err(Self::map_to_invalid_args)?;
        
        let loginfo:String = format!("process_transaction: command={:?}", command).to_string();
        println!("{}",
            printLn(
                loginfo,
                module_path!().to_string()
            )
        );
        if info[0].signer_key().is_none() {
            Err(IcoErr::InvalidArgument)?;
        }
        let in_accts: Vec<IcoContext> = info
            .iter()
            .map(|keyed_account| {
                let account = &keyed_account.account;
                if account.owner == *program_id {
                    match Self::deserialize(&account.data) {
                        Ok(token_state) => token_state,
                        Err(err) => {
                            
                            println!(
                                "{}",
                                Error(
                                    format!("deserialize failed: {:?}", err).to_string(),
                                    module_path!().to_string()
                                )
                            );
                            IcoContext::Invalid
                        }
                    }
                } else {
                    IcoContext::Invalid
                }
            })
            .collect();

        for account in &in_accts {
            
            let loginfo:String = format!("input_account: data={:?}", account).to_string();
            println!("{}",
                printLn(
                    loginfo,
                    module_path!().to_string()
                )
            );
        }

        let mut out_accts: Vec<(_, _)> = vec![];

        match command {
            IcoOpCode::NewICO(ico_info) => {
                Self::issue_ico_cmd(info, ico_info, &in_accts, &mut out_accts)?
            }
            IcoOpCode::NewIcoAcct => {
                Self::crate_ico_acct(info, &in_accts, &mut out_accts)?
            }

            IcoOpCode::MoveToken(amount) => {
                Self::transfer_token(info, amount, &in_accts, &mut out_accts)?
            }

            IcoOpCode::SetALW(amount) => {
                Self::set_allowance(info, amount, &in_accts, &mut out_accts)?
            }

            IcoOpCode::SetOwner => {
                Self::set_ico_owner(info, &in_accts, &mut out_accts)?
            }
        }
        for (index, account) in &out_accts {
            
            let loginfo:String = format!("output_account: index={} data={:?}", index, account).to_string();
            println!("{}",
                printLn(
                    loginfo,
                    module_path!().to_string()
                )
            );
            Self::serialize(account, &mut info[*index].account.data)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    pub fn serde() {
        assert_eq!(IcoContext::deserialize(&[0]), Ok(IcoContext::default()));

        let mut data = vec![0; 256];

        let account = IcoContext::Account(TkActInfo {
            token: BvmAddr::new(&[1; 32]),
            owner: BvmAddr::new(&[2; 32]),
            amount: 123,
            agent_acct: None,
        });
        account.serialize(&mut data).unwrap();
        assert_eq!(IcoContext::deserialize(&data), Ok(account));

        let account = IcoContext::Token(IcoInfo {
            supply: 12345,
            decimals: 2,
            name: "A test token".to_string(),
            symbol: "TEST".to_string(),
        });
        account.serialize(&mut data).unwrap();
        assert_eq!(IcoContext::deserialize(&data), Ok(account));
    }

    #[test]
    pub fn serde_expect_fail() {
        let mut data = vec![0; 256];

        
        let account = IcoContext::default();
        assert_eq!(account, IcoContext::Unallocated);
        assert!(account.serialize(&mut data).is_err());
        assert!(account.serialize(&mut data).is_err());
        let account = IcoContext::Invalid;
        assert!(account.serialize(&mut data).is_err());

        
        assert!(IcoContext::deserialize(&[]).is_err());
        assert!(IcoContext::deserialize(&[1]).is_err());
        assert!(IcoContext::deserialize(&[1, 2]).is_err());
        assert!(IcoContext::deserialize(&[2, 2]).is_err());
        assert!(IcoContext::deserialize(&[3]).is_err());
    }

    
}
