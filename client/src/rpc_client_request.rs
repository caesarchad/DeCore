use crate::account_host_err::ClientError;
use crate::rpc_request::{RpcError, RpcRequest};
use log::*;
use reqwest;
use reqwest::header::CONTENT_TYPE;
use morgan_interface::timing::{DEFAULT_NUM_DROPS_PER_SECOND, DEFAULT_DROPS_PER_SLOT};
use std::thread::sleep;
use std::time::Duration;
use ansi_term::Color::{Green};
use morgan_helper::logHelper::*;
use serde_json::{Number, Value};
use morgan_interface::gas_cost::FeeCalculator;
use morgan_interface::transaction::{self, TransactionError};

pub(crate) trait GenericRpcClientRequest {
    fn send(
        &self,
        request: &RpcRequest,
        params: Option<serde_json::Value>,
        retries: usize,
    ) -> Result<serde_json::Value, ClientError>;
}


pub struct RpcClientRequest {
    client: reqwest::Client,
    url: String,
}

impl RpcClientRequest {
    pub fn new(url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            url,
        }
    }

    pub fn new_with_timeout(url: String, timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("build rpc client");

        Self { client, url }
    }
}


pub const PUBKEY: &str = "7RoSF9fUmdphVCpabEoefH81WwrW7orsWonXWqTXkKV8";
pub const SIGNATURE: &str =
    "43yNSFC6fYTuPgTNFFhF4axw7AfWxB2BPdurme8yrsWEYwm8299xh8n6TAHjGymiSub1XtyxTNyd9GBfY2hxoBw8";

pub struct MockRpcClientRequest {
    url: String,
}

impl MockRpcClientRequest {
    pub fn new(url: String) -> Self {
        Self { url }
    }
}

impl GenericRpcClientRequest for MockRpcClientRequest {
    fn send(
        &self,
        request: &RpcRequest,
        params: Option<serde_json::Value>,
        _retries: usize,
    ) -> Result<serde_json::Value, ClientError> {
        if self.url == "fails" {
            return Ok(Value::Null);
        }
        let val = match request {
            RpcRequest::ConfirmTransaction => {
                if let Some(Value::Array(param_array)) = params {
                    if let Value::String(param_string) = &param_array[0] {
                        Value::Bool(param_string == SIGNATURE)
                    } else {
                        Value::Null
                    }
                } else {
                    Value::Null
                }
            }
            RpcRequest::GetBalance => {
                let n = if self.url == "airdrop" { 0 } else { 50 };
                Value::Number(Number::from(n))
            }
            RpcRequest::GetRecentTransactionSeal => Value::Array(vec![
                Value::String(PUBKEY.to_string()),
                serde_json::to_value(FeeCalculator::default()).unwrap(),
            ]),
            RpcRequest::GetSignatureStatus => {
                let response: Option<transaction::Result<()>> = if self.url == "account_in_use" {
                    Some(Err(TransactionError::AccountInUse))
                } else if self.url == "sig_not_found" {
                    None
                } else {
                    Some(Ok(()))
                };
                serde_json::to_value(response).unwrap()
            }
            RpcRequest::GetTransactionCount => Value::Number(Number::from(1234)),
            RpcRequest::SendTransaction => Value::String(SIGNATURE.to_string()),
            _ => Value::Null,
        };
        Ok(val)
    }
}

impl GenericRpcClientRequest for RpcClientRequest {
    fn send(
        &self,
        request: &RpcRequest,
        params: Option<serde_json::Value>,
        mut retries: usize,
    ) -> Result<serde_json::Value, ClientError> {
        // Concurrent requests are not supported so reuse the same request id for all requests
        let request_id = 1;

        let request_json = request.build_request_json(request_id, params);

        loop {
            match self
                .client
                .post(&self.url)
                .header(CONTENT_TYPE, "application/json")
                .body(request_json.to_string())
                .send()
            {
                Ok(mut response) => {
                    let json: serde_json::Value = serde_json::from_str(&response.text()?)?;
                    if json["error"].is_object() {
                        Err(RpcError::RpcRequestError(format!(
                            "RPC Error response: {}",
                            serde_json::to_string(&json["error"]).unwrap()
                        )))?
                    }
                    return Ok(json["result"].clone());
                }
                Err(e) => {
                    // info!(
                    //     "{}",
                    //     Info(format!(
                    //         "make_rpc_request({:?}) failed, {} retries left: {:?}",
                    //         request, retries, e).to_string()
                    //     )
                    // );
                    let info:String = format!(
                        "make_rpc_request({:?}) failed, {} retries left: {:?}",
                        request, retries, e).to_string();
                    println!("{}",
                        printLn(
                            info,
                            module_path!().to_string()
                        )
                    );

                    if retries == 0 {
                        Err(e)?;
                    }
                    retries -= 1;

                    // Sleep for approximately half a slot
                    sleep(Duration::from_millis(
                        500 * DEFAULT_DROPS_PER_SLOT / DEFAULT_NUM_DROPS_PER_SECOND,
                    ));
                }
            }
        }
    }
}


