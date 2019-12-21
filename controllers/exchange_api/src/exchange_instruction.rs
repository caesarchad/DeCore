//! Exchange program

use crate::exchange_state::*;
use crate::id;
use serde_derive::{Deserialize, Serialize};
use morgan_interface::opcodes::{AccountMeta, OpCode};
use morgan_interface::pubkey::Pubkey;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct TradeRequestInfo {
    /// Direction of trade
    pub direction: Direction,

    /// Token pair to trade
    pub pair: TokenPair,

    /// Number of tokens to exchange; refers to the primary or the secondary depending on the direction
    pub tokens: u64,

    /// The price ratio the primary price over the secondary price.  The primary price is fixed
    /// and equal to the variable `SCALER`.
    pub price: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum ExchangeInstruction {
    /// New token account
    /// key 0 - Signer
    /// key 1 - New token account
    AccountRequest,

    /// Transfer tokens between two accounts
    /// key 0 - Account to transfer tokens to
    /// key 1 - Account to transfer tokens from.  This can be the exchange program itself,
    ///         the exchange has a limitless number of tokens it can transfer.
    TransferRequest(Token, u64),

    /// Trade request
    /// key 0 - Signer
    /// key 1 - Account in which to record the trade order
    /// key 2 - Token account to genesis tokens from
    TradeRequest(TradeRequestInfo),

    /// Trade cancellation
    /// key 0 - Signer
    /// key 1 - Trade order to cancel
    TradeCancellation,

    /// Trade swap request
    /// key 0 - Signer
    /// key 2 - 'To' trade order
    /// key 3 - `From` trade order
    /// key 6 - Token account in which to deposit the brokers profit from the swap.
    SwapRequest,
}

pub fn account_request(owner: &Pubkey, new: &Pubkey) -> OpCode {
    let account_metas = vec![
        AccountMeta::new(*owner, true),
        AccountMeta::new(*new, false),
    ];
    OpCode::new(id(), &ExchangeInstruction::AccountRequest, account_metas)
}

pub fn transfer_request(
    owner: &Pubkey,
    to: &Pubkey,
    from: &Pubkey,
    token: Token,
    tokens: u64,
) -> OpCode {
    let account_metas = vec![
        AccountMeta::new(*owner, true),
        AccountMeta::new(*to, false),
        AccountMeta::new(*from, false),
    ];
    OpCode::new(
        id(),
        &ExchangeInstruction::TransferRequest(token, tokens),
        account_metas,
    )
}

pub fn trade_request(
    owner: &Pubkey,
    trade: &Pubkey,
    direction: Direction,
    pair: TokenPair,
    tokens: u64,
    price: u64,
    src_account: &Pubkey,
) -> OpCode {
    let account_metas = vec![
        AccountMeta::new(*owner, true),
        AccountMeta::new(*trade, false),
        AccountMeta::new(*src_account, false),
    ];
    OpCode::new(
        id(),
        &ExchangeInstruction::TradeRequest(TradeRequestInfo {
            direction,
            pair,
            tokens,
            price,
        }),
        account_metas,
    )
}

pub fn trade_cancellation(owner: &Pubkey, trade: &Pubkey) -> OpCode {
    let account_metas = vec![
        AccountMeta::new(*owner, true),
        AccountMeta::new(*trade, false),
    ];
    OpCode::new(id(), &ExchangeInstruction::TradeCancellation, account_metas)
}

pub fn swap_request(
    owner: &Pubkey,
    to_trade: &Pubkey,
    from_trade: &Pubkey,
    profit_account: &Pubkey,
) -> OpCode {
    let account_metas = vec![
        AccountMeta::new(*owner, true),
        AccountMeta::new(*to_trade, false),
        AccountMeta::new(*from_trade, false),
        AccountMeta::new(*profit_account, false),
    ];
    OpCode::new(id(), &ExchangeInstruction::SwapRequest, account_metas)
}
