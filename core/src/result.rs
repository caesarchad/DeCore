//! The `result` module exposes a Result type that propagates one of many different Error types.

use crate::block_buffer_pool;
use crate::node_group_info;
use crate::packet;
use crate::water_clock_recorder;
use bincode;
use serde_json;
use morgan_interface::transaction;
use std;
use std::any::Any;
use prometheus::{
    proto::{LabelPair, Metric, MetricFamily, MetricType},
    Encoder, Result as PResult,
};
use std::{collections::HashMap, io::Write};

const JSON_FORMAT: &str = "application/json";

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    JSON(serde_json::Error),
    AddrParse(std::net::AddrParseError),
    JoinError(Box<dyn Any + Send + 'static>),
    RecvError(std::sync::mpsc::RecvError),
    RecvTimeoutError(std::sync::mpsc::RecvTimeoutError),
    TryRecvError(std::sync::mpsc::TryRecvError),
    Serialize(std::boxed::Box<bincode::ErrorKind>),
    TransactionError(transaction::TransactionError),
    NodeGroupInfoError(node_group_info::NodeGroupInfoError),
    BlobError(packet::BlobError),
    ErasureError(reed_solomon_erasure::Error),
    SendError,
    WaterClockRecorderErr(water_clock_recorder::WaterClockRecorderErr),
    BlockBufferPoolError(block_buffer_pool::BlockBufferPoolError),
}

pub type Result<T> = std::result::Result<T, Error>;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "morgan error")
    }
}

impl std::error::Error for Error {}

impl std::convert::From<std::sync::mpsc::RecvError> for Error {
    fn from(e: std::sync::mpsc::RecvError) -> Error {
        Error::RecvError(e)
    }
}
impl std::convert::From<std::sync::mpsc::TryRecvError> for Error {
    fn from(e: std::sync::mpsc::TryRecvError) -> Error {
        Error::TryRecvError(e)
    }
}
impl std::convert::From<std::sync::mpsc::RecvTimeoutError> for Error {
    fn from(e: std::sync::mpsc::RecvTimeoutError) -> Error {
        Error::RecvTimeoutError(e)
    }
}
impl std::convert::From<transaction::TransactionError> for Error {
    fn from(e: transaction::TransactionError) -> Error {
        Error::TransactionError(e)
    }
}
impl std::convert::From<node_group_info::NodeGroupInfoError> for Error {
    fn from(e: node_group_info::NodeGroupInfoError) -> Error {
        Error::NodeGroupInfoError(e)
    }
}
impl std::convert::From<reed_solomon_erasure::Error> for Error {
    fn from(e: reed_solomon_erasure::Error) -> Error {
        Error::ErasureError(e)
    }
}
impl<T> std::convert::From<std::sync::mpsc::SendError<T>> for Error {
    fn from(_e: std::sync::mpsc::SendError<T>) -> Error {
        Error::SendError
    }
}

/// An implementation of an [`Encoder`](::Encoder) that converts a `MetricFamily` proto message
/// into `fbagent` json
///
/// This implementation converts metric{dimensions,...} -> value to a flat string with a value.
/// e.g., "requests{method="GET", service="accounts"} -> 8 into
/// requests.GET.account -> 8
/// For now, it ignores timestamps (if set on the metric)
#[derive(Debug, Default)]
pub struct JsonEncoder;

impl Encoder for JsonEncoder {
    fn encode<W: Write>(&self, metric_familys: &[MetricFamily], writer: &mut W) -> PResult<()> {
        let mut export_me: HashMap<String, f64> = HashMap::new();

        for mf in metric_familys {
            let name = mf.get_name();
            let metric_type = mf.get_field_type();

            for m in mf.get_metric() {
                match metric_type {
                    MetricType::COUNTER => {
                        export_me.insert(
                            flatten_metric_with_labels(name, m),
                            m.get_counter().get_value(),
                        );
                    }
                    MetricType::GAUGE => {
                        export_me.insert(
                            flatten_metric_with_labels(name, m),
                            m.get_gauge().get_value(),
                        );
                    }
                    MetricType::HISTOGRAM => {
                        // write the sum and counts
                        let h = m.get_histogram();
                        export_me.insert(
                            flatten_metric_with_labels(&format!("{}_count", name), m),
                            h.get_sample_count() as f64,
                        );
                        export_me.insert(
                            flatten_metric_with_labels(&format!("{}_sum", name), m),
                            h.get_sample_sum(),
                        );
                    }
                    _ => {
                        // do nothing; unimplemented
                    }
                }
            }
        }

        writer.write_all(serde_json::to_string(&export_me).unwrap().as_bytes())?;
        Ok(())
    }

    fn format_type(&self) -> &str {
        JSON_FORMAT
    }
}

/**
This method takes Prometheus metrics with dimensions (represented as label:value tags)
and converts it into a dot-separated string.

Example:
Prometheus metric: error_count{method: "get_account", error="connection_error"}
Result: error_count.get_account.connection_error

If the set of labels is empty, only the name is returned
Example:
Prometheus metric: errors
Result: errors

This is useful when exporting metric data to flat time series.
*/
fn flatten_metric_with_labels(name: &str, metric: &Metric) -> String {
    let res = String::from(name);

    if metric.get_label().is_empty() {
        res
    } else {
        // string-list.join(".")
        let values: Vec<&str> = metric
            .get_label()
            .iter()
            .map(LabelPair::get_value)
            .filter(|&x| !x.is_empty())
            .collect();
        let values = values.join(".");
        if !values.is_empty() {
            format!("{}.{}", res, values)
        } else {
            res
        }
    }
}

impl std::convert::From<Box<dyn Any + Send + 'static>> for Error {
    fn from(e: Box<dyn Any + Send + 'static>) -> Error {
        Error::JoinError(e)
    }
}
impl std::convert::From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::IO(e)
    }
}
impl std::convert::From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Error {
        Error::JSON(e)
    }
}
impl std::convert::From<std::net::AddrParseError> for Error {
    fn from(e: std::net::AddrParseError) -> Error {
        Error::AddrParse(e)
    }
}
impl std::convert::From<std::boxed::Box<bincode::ErrorKind>> for Error {
    fn from(e: std::boxed::Box<bincode::ErrorKind>) -> Error {
        Error::Serialize(e)
    }
}
impl std::convert::From<water_clock_recorder::WaterClockRecorderErr> for Error {
    fn from(e: water_clock_recorder::WaterClockRecorderErr) -> Error {
        Error::WaterClockRecorderErr(e)
    }
}
impl std::convert::From<block_buffer_pool::BlockBufferPoolError> for Error {
    fn from(e: block_buffer_pool::BlockBufferPoolError) -> Error {
        Error::BlockBufferPoolError(e)
    }
}

#[cfg(test)]
mod tests {
    use crate::result::Error;
    use crate::result::Result;
    use serde_json;
    use std::io;
    use std::io::Write;
    use std::net::SocketAddr;
    use std::panic;
    use std::sync::mpsc::channel;
    use std::sync::mpsc::RecvError;
    use std::sync::mpsc::RecvTimeoutError;
    use std::thread;

    fn addr_parse_error() -> Result<SocketAddr> {
        let r = "12fdfasfsafsadfs".parse()?;
        Ok(r)
    }

    fn join_error() -> Result<()> {
        panic::set_hook(Box::new(|_info| {}));
        let r = thread::spawn(|| panic!("hi")).join()?;
        Ok(r)
    }
    fn json_error() -> Result<()> {
        let r = serde_json::from_slice("=342{;;;;:}".as_bytes())?;
        Ok(r)
    }
    fn send_error() -> Result<()> {
        let (s, r) = channel();
        drop(r);
        s.send(())?;
        Ok(())
    }

    #[test]
    fn from_test() {
        assert_matches!(addr_parse_error(), Err(Error::AddrParse(_)));
        assert_matches!(Error::from(RecvError {}), Error::RecvError(_));
        assert_matches!(
            Error::from(RecvTimeoutError::Timeout),
            Error::RecvTimeoutError(_)
        );
        assert_matches!(send_error(), Err(Error::SendError));
        assert_matches!(join_error(), Err(Error::JoinError(_)));
        let ioe = io::Error::new(io::ErrorKind::NotFound, "hi");
        assert_matches!(Error::from(ioe), Error::IO(_));
    }
    #[test]
    fn fmt_test() {
        write!(io::sink(), "{:?}", addr_parse_error()).unwrap();
        write!(io::sink(), "{:?}", Error::from(RecvError {})).unwrap();
        write!(io::sink(), "{:?}", Error::from(RecvTimeoutError::Timeout)).unwrap();
        write!(io::sink(), "{:?}", send_error()).unwrap();
        write!(io::sink(), "{:?}", join_error()).unwrap();
        write!(io::sink(), "{:?}", json_error()).unwrap();
        write!(
            io::sink(),
            "{:?}",
            Error::from(io::Error::new(io::ErrorKind::NotFound, "hi"))
        )
        .unwrap();
    }
}
