use log::*;
use num_derive::FromPrimitive;
use serde_derive::{Deserialize, Serialize};
use morgan_interface::account::KeyedAccount;
use morgan_interface::opcodes_utils::DecodeError;
use morgan_interface::pubkey::Pubkey;
use morgan_helper::logHelper::*;

#[derive(Serialize, Debug, PartialEq, FromPrimitive)]
pub enum TokenError {
    InvalidArgument,
    InsufficentFunds,
    NotOwner,
}

impl<T> DecodeError<T> for TokenError {
    fn type_of(&self) -> &'static str {
        "TokenError"
    }
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "error")
    }
}
impl std::error::Error for TokenError {}

pub type Result<T> = std::result::Result<T, TokenError>;

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct TokenInfo {
    supply: u64,

    decimals: u8,

    name: String,

    symbol: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct TokenAccountDelegateInfo {
    genesis: Pubkey,

    original_amount: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct TokenAccountInfo {
    token: Pubkey,

    owner: Pubkey,

    amount: u64,

    delegate: Option<TokenAccountDelegateInfo>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum TokenOpCode {
    NewToken(TokenInfo),
    NewTokenAccount,
    Transfer(u64),
    Approve(u64),
    SetOwner,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum TokenState {
    Unallocated,
    Token(TokenInfo),
    Account(TokenAccountInfo),
    Invalid,
}
impl Default for TokenState {
    fn default() -> TokenState {
        TokenState::Unallocated
    }
}

impl TokenState {
    #[allow(clippy::needless_pass_by_value)]
    fn map_to_invalid_args(err: std::boxed::Box<bincode::ErrorKind>) -> TokenError {
        
        println!(
            "{}",
            Warn(
                format!("invalid argument: {:?}", err).to_string(),
                module_path!().to_string()
            )
        );
        TokenError::InvalidArgument
    }

    pub fn deserialize(input: &[u8]) -> Result<TokenState> {
        if input.is_empty() {
            Err(TokenError::InvalidArgument)?;
        }

        match input[0] {
            0 => Ok(TokenState::Unallocated),
            1 => Ok(TokenState::Token(
                bincode::deserialize(&input[1..]).map_err(Self::map_to_invalid_args)?,
            )),
            2 => Ok(TokenState::Account(
                bincode::deserialize(&input[1..]).map_err(Self::map_to_invalid_args)?,
            )),
            _ => Err(TokenError::InvalidArgument),
        }
    }

    fn serialize(self: &TokenState, output: &mut [u8]) -> Result<()> {
        if output.is_empty() {
            
            println!(
                "{}",
                Warn(
                    format!("serialize fail: ouput.len is 0").to_string(),
                    module_path!().to_string()
                )
            );
            Err(TokenError::InvalidArgument)?;
        }
        match self {
            TokenState::Unallocated | TokenState::Invalid => Err(TokenError::InvalidArgument),
            TokenState::Token(token_info) => {
                output[0] = 1;
                let writer = std::io::BufWriter::new(&mut output[1..]);
                bincode::serialize_into(writer, &token_info).map_err(Self::map_to_invalid_args)
            }
            TokenState::Account(account_info) => {
                output[0] = 2;
                let writer = std::io::BufWriter::new(&mut output[1..]);
                bincode::serialize_into(writer, &account_info).map_err(Self::map_to_invalid_args)
            }
        }
    }

    #[allow(dead_code)]
    pub fn amount(&self) -> Result<u64> {
        if let TokenState::Account(account_info) = self {
            Ok(account_info.amount)
        } else {
            Err(TokenError::InvalidArgument)
        }
    }

    #[allow(dead_code)]
    pub fn only_owner(&self, key: &Pubkey) -> Result<()> {
        if *key != Pubkey::default() {
            if let TokenState::Account(account_info) = self {
                if account_info.owner == *key {
                    return Ok(());
                }
            }
        }
        
        println!(
            "{}",
            Warn(
                format!("TokenState: non-owner rejected").to_string(),
                module_path!().to_string()
            )
        );
        Err(TokenError::NotOwner)
    }

    pub fn process_newtoken(
        info: &mut [KeyedAccount],
        token_info: TokenInfo,
        input_accounts: &[TokenState],
        output_accounts: &mut Vec<(usize, TokenState)>,
    ) -> Result<()> {
        if input_accounts.len() != 2 {
            
            println!(
                "{}",
                Error(
                    format!("Expected 2 accounts").to_string(),
                    module_path!().to_string()
                )
            );
            Err(TokenError::InvalidArgument)?;
        }

        if let TokenState::Account(dest_account) = &input_accounts[1] {
            if info[0].signer_key().unwrap() != &dest_account.token {
                
                println!(
                    "{}",
                    Error(
                        format!("account 1 token mismatch").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(TokenError::InvalidArgument)?;
            }

            if dest_account.delegate.is_some() {
                
                println!(
                    "{}",
                    Error(
                        format!("account 1 is a delegate and cannot accept tokens").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(TokenError::InvalidArgument)?;
            }

            let mut output_dest_account = dest_account.clone();
            output_dest_account.amount = token_info.supply;
            output_accounts.push((1, TokenState::Account(output_dest_account)));
        } else {
            
            println!(
                "{}",
                Error(
                    format!("account 1 invalid").to_string(),
                    module_path!().to_string()
                )
            );
            Err(TokenError::InvalidArgument)?;
        }

        if input_accounts[0] != TokenState::Unallocated {
            
            println!(
                "{}",
                Error(
                    format!("account 0 not available").to_string(),
                    module_path!().to_string()
                )
            );
            Err(TokenError::InvalidArgument)?;
        }
        output_accounts.push((0, TokenState::Token(token_info)));
        Ok(())
    }

    pub fn process_newaccount(
        info: &mut [KeyedAccount],
        input_accounts: &[TokenState],
        output_accounts: &mut Vec<(usize, TokenState)>,
    ) -> Result<()> {
        // key 0 - Destination new token account
        // key 1 - Owner of the account
        // key 2 - Token this account is associated with
        // key 3 - Source account that this account is a delegate for (optional)
        if input_accounts.len() < 3 {
            // error!("{}", Error(format!("Expected 3 accounts").to_string()));
            println!(
                "{}",
                Error(
                    format!("Expected 3 accounts").to_string(),
                    module_path!().to_string()
                )
            );
            Err(TokenError::InvalidArgument)?;
        }
        if input_accounts[0] != TokenState::Unallocated {
            
            println!(
                "{}",
                Error(
                    format!("account 0 is already allocated").to_string(),
                    module_path!().to_string()
                )
            );
            Err(TokenError::InvalidArgument)?;
        }
        let mut token_account_info = TokenAccountInfo {
            token: *info[2].unsigned_key(),
            owner: *info[1].unsigned_key(),
            amount: 0,
            delegate: None,
        };
        if input_accounts.len() >= 4 {
            token_account_info.delegate = Some(TokenAccountDelegateInfo {
                genesis: *info[3].unsigned_key(),
                original_amount: 0,
            });
        }
        output_accounts.push((0, TokenState::Account(token_account_info)));
        Ok(())
    }

    pub fn process_transfer(
        info: &mut [KeyedAccount],
        amount: u64,
        input_accounts: &[TokenState],
        output_accounts: &mut Vec<(usize, TokenState)>,
    ) -> Result<()> {
        if input_accounts.len() < 3 {
            
            println!(
                "{}",
                Error(
                    format!("Expected 3 accounts").to_string(),
                    module_path!().to_string()
                )
            );
            Err(TokenError::InvalidArgument)?;
        }

        if let (TokenState::Account(source_account), TokenState::Account(dest_account)) =
            (&input_accounts[1], &input_accounts[2])
        {
            if source_account.token != dest_account.token {
                
                println!(
                    "{}",
                    Error(
                        format!("account 1/2 token mismatch").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(TokenError::InvalidArgument)?;
            }

            if dest_account.delegate.is_some() {
                
                println!(
                    "{}",
                    Error(
                        format!("account 2 is a delegate and cannot accept tokens").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(TokenError::InvalidArgument)?;
            }

            if info[0].signer_key().unwrap() != &source_account.owner {
                
                println!(
                    "{}",
                    Error(
                        format!("owner of account 1 not present").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(TokenError::InvalidArgument)?;
            }

            if source_account.amount < amount {
                Err(TokenError::InsufficentFunds)?;
            }

            let mut output_source_account = source_account.clone();
            output_source_account.amount -= amount;
            output_accounts.push((1, TokenState::Account(output_source_account)));

            if let Some(ref delegate_info) = source_account.delegate {
                if input_accounts.len() != 4 {
                    
                    println!(
                        "{}",
                        Error(
                            format!("Expected 4 accounts").to_string(),
                            module_path!().to_string()
                        )
                    );
                    Err(TokenError::InvalidArgument)?;
                }

                let delegate_account = source_account;
                if let TokenState::Account(source_account) = &input_accounts[3] {
                    if source_account.token != delegate_account.token {
                        
                        println!(
                            "{}",
                            Error(
                                format!("account 1/3 token mismatch").to_string(),
                                module_path!().to_string()
                            )
                        );
                        Err(TokenError::InvalidArgument)?;
                    }
                    if info[3].unsigned_key() != &delegate_info.genesis {
                        
                        println!(
                            "{}",
                            Error(
                                format!("Account 1 is not a delegate of account 3").to_string(),
                                module_path!().to_string()
                            )
                        );
                        Err(TokenError::InvalidArgument)?;
                    }

                    if source_account.amount < amount {
                        Err(TokenError::InsufficentFunds)?;
                    }

                    let mut output_source_account = source_account.clone();
                    output_source_account.amount -= amount;
                    output_accounts.push((3, TokenState::Account(output_source_account)));
                } else {
                    
                    println!(
                        "{}",
                        Error(
                            format!("account 3 is an invalid account").to_string(),
                            module_path!().to_string()
                        )
                    );
                    Err(TokenError::InvalidArgument)?;
                }
            }

            let mut output_dest_account = dest_account.clone();
            output_dest_account.amount += amount;
            output_accounts.push((2, TokenState::Account(output_dest_account)));
        } else {
            
            println!(
                "{}",
                Error(
                    format!("account 1 and/or 2 are invalid accounts").to_string(),
                    module_path!().to_string()
                )
            );
            Err(TokenError::InvalidArgument)?;
        }
        Ok(())
    }

    pub fn process_approve(
        info: &mut [KeyedAccount],
        amount: u64,
        input_accounts: &[TokenState],
        output_accounts: &mut Vec<(usize, TokenState)>,
    ) -> Result<()> {
        if input_accounts.len() != 3 {
            
            println!(
                "{}",
                Error(
                    format!("Expected 3 accounts").to_string(),
                    module_path!().to_string()
                )
            );
            Err(TokenError::InvalidArgument)?;
        }

        if let (TokenState::Account(source_account), TokenState::Account(delegate_account)) =
            (&input_accounts[1], &input_accounts[2])
        {
            if source_account.token != delegate_account.token {
                
                println!(
                    "{}",
                    Error(
                        format!("account 1/2 token mismatch").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(TokenError::InvalidArgument)?;
            }

            if info[0].signer_key().unwrap() != &source_account.owner {
                
                println!(
                    "{}",
                    Error(
                        format!("owner of account 1 not present").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(TokenError::InvalidArgument)?;
            }

            if source_account.delegate.is_some() {
                
                println!(
                    "{}",
                    Error(
                        format!("account 1 is a delegate").to_string(),
                        module_path!().to_string()
                    )
                );
                Err(TokenError::InvalidArgument)?;
            }

            match &delegate_account.delegate {
                None => {
                    
                    println!(
                        "{}",
                        Error(
                            format!("account 2 is not a delegate").to_string(),
                            module_path!().to_string()
                        )
                    );
                    Err(TokenError::InvalidArgument)?;
                }
                Some(delegate_info) => {
                    if info[1].unsigned_key() != &delegate_info.genesis {
                        
                        println!(
                            "{}",
                            Error(
                                format!("account 2 is not a delegate of account 1").to_string(),
                                module_path!().to_string()
                            )
                        );
                        Err(TokenError::InvalidArgument)?;
                    }

                    let mut output_delegate_account = delegate_account.clone();
                    output_delegate_account.amount = amount;
                    output_delegate_account.delegate = Some(TokenAccountDelegateInfo {
                        genesis: delegate_info.genesis,
                        original_amount: amount,
                    });
                    output_accounts.push((2, TokenState::Account(output_delegate_account)));
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
            Err(TokenError::InvalidArgument)?;
        }
        Ok(())
    }

    pub fn process_setowner(
        info: &mut [KeyedAccount],
        input_accounts: &[TokenState],
        output_accounts: &mut Vec<(usize, TokenState)>,
    ) -> Result<()> {
        if input_accounts.len() < 3 {
            
            println!(
                "{}",
                Error(
                    format!("Expected 3 accounts").to_string(),
                    module_path!().to_string()
                )
            );
            Err(TokenError::InvalidArgument)?;
        }

        if let TokenState::Account(source_account) = &input_accounts[1] {
            if info[0].signer_key().unwrap() != &source_account.owner {
                
                let info:String = format!("owner of account 1 not present").to_string();
                println!("{}",
                    printLn(
                        info,
                        module_path!().to_string()
                    )
                );

                Err(TokenError::InvalidArgument)?;
            }

            let mut output_source_account = source_account.clone();
            output_source_account.owner = *info[2].unsigned_key();
            output_accounts.push((1, TokenState::Account(output_source_account)));
        } else {
            
            let info:String = format!("account 1 is invalid").to_string();
            println!("{}",
                printLn(
                    info,
                    module_path!().to_string()
                )
            );
            Err(TokenError::InvalidArgument)?;
        }
        Ok(())
    }

    pub fn process(program_id: &Pubkey, info: &mut [KeyedAccount], input: &[u8]) -> Result<()> {
        let command =
            bincode::deserialize::<TokenOpCode>(input).map_err(Self::map_to_invalid_args)?;
        
        let loginfo:String = format!("process_transaction: command={:?}", command).to_string();
        println!("{}",
            printLn(
                loginfo,
                module_path!().to_string()
            )
        );
        if info[0].signer_key().is_none() {
            Err(TokenError::InvalidArgument)?;
        }
        let input_accounts: Vec<TokenState> = info
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
                            TokenState::Invalid
                        }
                    }
                } else {
                    TokenState::Invalid
                }
            })
            .collect();

        for account in &input_accounts {
            
            let loginfo:String = format!("input_account: data={:?}", account).to_string();
            println!("{}",
                printLn(
                    loginfo,
                    module_path!().to_string()
                )
            );
        }

        let mut output_accounts: Vec<(_, _)> = vec![];

        match command {
            TokenOpCode::NewToken(token_info) => {
                Self::process_newtoken(info, token_info, &input_accounts, &mut output_accounts)?
            }
            TokenOpCode::NewTokenAccount => {
                Self::process_newaccount(info, &input_accounts, &mut output_accounts)?
            }

            TokenOpCode::Transfer(amount) => {
                Self::process_transfer(info, amount, &input_accounts, &mut output_accounts)?
            }

            TokenOpCode::Approve(amount) => {
                Self::process_approve(info, amount, &input_accounts, &mut output_accounts)?
            }

            TokenOpCode::SetOwner => {
                Self::process_setowner(info, &input_accounts, &mut output_accounts)?
            }
        }
        for (index, account) in &output_accounts {
            
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
        assert_eq!(TokenState::deserialize(&[0]), Ok(TokenState::default()));

        let mut data = vec![0; 256];

        let account = TokenState::Account(TokenAccountInfo {
            token: Pubkey::new(&[1; 32]),
            owner: Pubkey::new(&[2; 32]),
            amount: 123,
            delegate: None,
        });
        account.serialize(&mut data).unwrap();
        assert_eq!(TokenState::deserialize(&data), Ok(account));

        let account = TokenState::Token(TokenInfo {
            supply: 12345,
            decimals: 2,
            name: "A test token".to_string(),
            symbol: "TEST".to_string(),
        });
        account.serialize(&mut data).unwrap();
        assert_eq!(TokenState::deserialize(&data), Ok(account));
    }

    #[test]
    pub fn serde_expect_fail() {
        let mut data = vec![0; 256];

        
        let account = TokenState::default();
        assert_eq!(account, TokenState::Unallocated);
        assert!(account.serialize(&mut data).is_err());
        assert!(account.serialize(&mut data).is_err());
        let account = TokenState::Invalid;
        assert!(account.serialize(&mut data).is_err());

        
        assert!(TokenState::deserialize(&[]).is_err());
        assert!(TokenState::deserialize(&[1]).is_err());
        assert!(TokenState::deserialize(&[1, 2]).is_err());
        assert!(TokenState::deserialize(&[2, 2]).is_err());
        assert!(TokenState::deserialize(&[3]).is_err());
    }

    
}
