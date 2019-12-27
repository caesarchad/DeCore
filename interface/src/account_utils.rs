//! useful extras for Account state
use crate::account::{Account, KeyedAccount};
use crate::opcodes::OpCodeErr;
use bincode::ErrorKind;

/// Convenience trait to covert bincode errors to instruction errors.
pub trait State<T> {
    fn state(&self) -> Result<T, OpCodeErr>;
    fn set_state(&mut self, state: &T) -> Result<(), OpCodeErr>;
}

impl<T> State<T> for Account
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    fn state(&self) -> Result<T, OpCodeErr> {
        self.deserialize_data()
            .map_err(|_| OpCodeErr::InvalidAccountData)
    }
    fn set_state(&mut self, state: &T) -> Result<(), OpCodeErr> {
        self.serialize_data(state).map_err(|err| match *err {
            ErrorKind::SizeLimit => OpCodeErr::AccountDataTooSmall,
            _ => OpCodeErr::GenericError,
        })
    }
}

impl<'a, T> State<T> for KeyedAccount<'a>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    fn state(&self) -> Result<T, OpCodeErr> {
        self.account.state()
    }
    fn set_state(&mut self, state: &T) -> Result<(), OpCodeErr> {
        self.account.set_state(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::Account;
    use crate::bvm_address::BvmAddr;

    #[test]
    fn test_account_state() {
        let state = 42u64;

        assert!(Account::default().set_state(&state).is_err());
        let res = Account::default().state() as Result<u64, OpCodeErr>;
        assert!(res.is_err());

        let mut account = Account::new(0, 0, std::mem::size_of::<u64>(), &BvmAddr::default());

        assert!(account.set_state(&state).is_ok());
        let stored_state: u64 = account.state().unwrap();
        assert_eq!(stored_state, state);
    }

}
