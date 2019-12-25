//! budget state
use crate::bvm_script::BvmScript;
use bincode::{self, deserialize, serialize_into};
use num_derive::FromPrimitive;
use serde_derive::{Deserialize, Serialize};
use morgan_interface::opcodes::OpCodeErr;
use morgan_interface::opcodes_utils::DecodeError;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, FromPrimitive)]
pub enum BudgetError {
    DestinationMissing,
}

impl<T> DecodeError<T> for BudgetError {
    fn type_of(&self) -> &'static str {
        "BudgetError"
    }
}

impl std::fmt::Display for BudgetError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "error")
    }
}
impl std::error::Error for BudgetError {}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct BudgetState {
    pub initialized: bool,
    pub pending_budget: Option<BvmScript>,
}

impl BudgetState {
    pub fn new(bvm_script: BvmScript) -> Self {
        Self {
            initialized: true,
            pending_budget: Some(bvm_script),
        }
    }

    pub fn is_pending(&self) -> bool {
        self.pending_budget.is_some()
    }

    pub fn serialize(&self, output: &mut [u8]) -> Result<(), OpCodeErr> {
        serialize_into(output, self).map_err(|_| OpCodeErr::AccountDataTooSmall)
    }

    pub fn deserialize(input: &[u8]) -> Result<Self, OpCodeErr> {
        deserialize(input).map_err(|_| OpCodeErr::InvalidAccountData)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::id;
    use morgan_interface::account::Account;

    #[test]
    fn test_serializer() {
        let mut a = Account::new(0, 0, 512, &id());
        let b = BudgetState::default();
        b.serialize(&mut a.data).unwrap();
        let c = BudgetState::deserialize(&a.data).unwrap();
        assert_eq!(b, c);
    }

    #[test]
    fn test_serializer_data_too_small() {
        let mut a = Account::new(0, 0, 1, &id());
        let b = BudgetState::default();
        assert_eq!(
            b.serialize(&mut a.data),
            Err(OpCodeErr::AccountDataTooSmall)
        );
    }
}
