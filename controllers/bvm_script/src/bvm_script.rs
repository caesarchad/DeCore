//! The `bvm_script` module provides a domain-specific language for pa&yment plans. Users create BvmScript objects that
//! are given to an interpreter. The interpreter listens for `Endorsement` transactions,
//! which it uses to reduce the payment plan. When the budget is reduced to a
//! `AcctOp`, the payment is executed.

use chrono::prelude::*;
use serde_derive::{Deserialize, Serialize};
use morgan_interface::bvm_address::BvmAddr;
use std::mem;

/// The types of events a payment plan can process.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum Endorsement {
    /// The current time.
    Timestamp(DateTime<Utc>),

    /// A signature from BvmAddr.
    Signature,
}

/// Some amount of difs that should be sent to the `to` `BvmAddr`.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct AcctOp {
    /// Amount to be paid.
    pub difs: u64,

    /// The `BvmAddr` that `difs` should be paid to.
    pub to: BvmAddr,
}

/// A data type representing a `Endorsement` that the payment plan is waiting on.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum Switch {
    /// Wait for a `Timestamp` `Endorsement` at or after the given `DateTime`.
    Timestamp(DateTime<Utc>, BvmAddr),

    /// Wait for a `Signature` `Endorsement` from `BvmAddr`.
    Signature(BvmAddr),
}

impl Switch {
    /// Return true if the given Endorsement satisfies this Switch.
    pub fn depends(&self, witness: &Endorsement, from: &BvmAddr) -> bool {
        match (self, witness) {
            (Switch::Signature(address), Endorsement::Signature) => address == from,
            (Switch::Timestamp(dt, address), Endorsement::Timestamp(last_time)) => {
                address == from && dt <= last_time
            }
            _ => false,
        }
    }
}

/// A data type representing a payment plan.
#[repr(C)]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum BvmScript {
    /// Make a payment.
    Pay(AcctOp),

    /// Make a payment after some condition.
    On(Switch, Box<BvmScript>),

    /// Either make a payment after one condition or a different payment after another
    /// condition, which ever condition is satisfied first.
    Or((Switch, Box<BvmScript>), (Switch, Box<BvmScript>)),

    /// Make a payment after both of two conditions are satisfied
    And(Switch, Switch, Box<BvmScript>),

    
    
}

impl BvmScript {
    /// Create the simplest budget - one that pays `difs` to BvmAddr.
    pub fn new_payment(difs: u64, to: &BvmAddr) -> Self {
        BvmScript::Pay(AcctOp { difs, to: *to })
    }

    /// Create a budget that pays `difs` to `to` after being witnessed by `from`.
    pub fn appr_pay(from: &BvmAddr, difs: u64, to: &BvmAddr) -> Self {
        BvmScript::On(
            Switch::Signature(*from),
            Box::new(Self::new_payment(difs, to)),
        )
    }

    /// Create a budget that pays `difs` to `to` after being witnessed by `witness` unless
    /// canceled with a signature from `from`.
    pub fn pending_appr_pay(
        witness: &BvmAddr,
        difs: u64,
        to: &BvmAddr,
        from: Option<BvmAddr>,
    ) -> Self {
        if from.is_none() {
            return Self::appr_pay(witness, difs, to);
        }
        let from = from.unwrap();
        BvmScript::Or(
            (
                Switch::Signature(*witness),
                Box::new(BvmScript::new_payment(difs, to)),
            ),
            (
                Switch::Signature(from),
                Box::new(BvmScript::new_payment(difs, &from)),
            ),
        )
    }

    /// Create a budget that pays difs` to `to` after being witnessed by 2x `from`s
    pub fn two_o_two_pay(
        from0: &BvmAddr,
        from1: &BvmAddr,
        difs: u64,
        to: &BvmAddr,
    ) -> Self {
        BvmScript::And(
            Switch::Signature(*from0),
            Switch::Signature(*from1),
            Box::new(Self::new_payment(difs, to)),
        )
    }

    

    /// Create a budget that pays `difs` to `to` after the given DateTime signed
    /// by `dt_address`.
    pub fn planned_pay(
        dt: DateTime<Utc>,
        dt_address: &BvmAddr,
        difs: u64,
        to: &BvmAddr,
    ) -> Self {
        BvmScript::On(
            Switch::Timestamp(dt, *dt_address),
            Box::new(Self::new_payment(difs, to)),
        )
    }

    /// Create a budget that pays `difs` to `to` after the given DateTime
    /// signed by `dt_address` unless canceled by `from`.
    pub fn pending_planned_pay(
        dt: DateTime<Utc>,
        dt_address: &BvmAddr,
        difs: u64,
        to: &BvmAddr,
        from: Option<BvmAddr>,
    ) -> Self {
        if from.is_none() {
            return Self::planned_pay(dt, dt_address, difs, to);
        }
        let from = from.unwrap();
        BvmScript::Or(
            (
                Switch::Timestamp(dt, *dt_address),
                Box::new(Self::new_payment(difs, to)),
            ),
            (
                Switch::Signature(from),
                Box::new(Self::new_payment(difs, &from)),
            ),
        )
    }

    /// Return AcctOp if the budget requires no additional Witnesses.
    pub fn commit_pay(&self) -> Option<AcctOp> {
        match self {
            BvmScript::Pay(payment) => Some(payment.clone()),
            _ => None,
        }
    }

    /// Return true if the budget spends exactly `spendable_difs`.
    pub fn verify(&self, spendable_difs: u64) -> bool {
        match self {
            BvmScript::Pay(payment) => payment.difs == spendable_difs,
            BvmScript::On(_, sub_expr) | BvmScript::And(_, _, sub_expr) => {
                sub_expr.verify(spendable_difs)
            }
            BvmScript::Or(a, b) => {
                a.1.verify(spendable_difs) && b.1.verify(spendable_difs)
            }
        }
    }

    /// Apply a witness to the budget to see if the budget can be reduced.
    /// If so, modify the budget in-place.
    pub fn apply_witness(&mut self, witness: &Endorsement, from: &BvmAddr) {
        let new_expr = match self {
            BvmScript::On(cond, sub_expr) if cond.depends(witness, from) => {
                Some(sub_expr.clone())
            }
            BvmScript::Or((cond, sub_expr), _) if cond.depends(witness, from) => {
                Some(sub_expr.clone())
            }
            BvmScript::Or(_, (cond, sub_expr)) if cond.depends(witness, from) => {
                Some(sub_expr.clone())
            }
            BvmScript::And(cond0, cond1, sub_expr) => {
                if cond0.depends(witness, from) {
                    Some(Box::new(BvmScript::On(cond1.clone(), sub_expr.clone())))
                } else if cond1.depends(witness, from) {
                    Some(Box::new(BvmScript::On(cond0.clone(), sub_expr.clone())))
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some(expr) = new_expr {
            mem::replace(self, *expr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signature_satisfied() {
        let from = BvmAddr::default();
        assert!(Switch::Signature(from).depends(&Endorsement::Signature, &from));
    }

    #[test]
    fn test_timestamp_satisfied() {
        let dt1 = Utc.ymd(2014, 11, 14).and_hms(8, 9, 10);
        let dt2 = Utc.ymd(2014, 11, 14).and_hms(10, 9, 8);
        let from = BvmAddr::default();
        assert!(Switch::Timestamp(dt1, from).depends(&Endorsement::Timestamp(dt1), &from));
        assert!(Switch::Timestamp(dt1, from).depends(&Endorsement::Timestamp(dt2), &from));
        assert!(!Switch::Timestamp(dt2, from).depends(&Endorsement::Timestamp(dt1), &from));
    }

    #[test]
    fn test_verify() {
        let dt = Utc.ymd(2014, 11, 14).and_hms(8, 9, 10);
        let from = BvmAddr::default();
        let to = BvmAddr::default();
        assert!(BvmScript::new_payment(42, &to).verify(42));
        assert!(BvmScript::appr_pay(&from, 42, &to).verify(42));
        assert!(BvmScript::planned_pay(dt, &from, 42, &to).verify(42));
        assert!(
            BvmScript::pending_planned_pay(dt, &from, 42, &to, Some(from)).verify(42)
        );
    }

    #[test]
    fn test_authorized_payment() {
        let from = BvmAddr::default();
        let to = BvmAddr::default();

        let mut expr = BvmScript::appr_pay(&from, 42, &to);
        expr.apply_witness(&Endorsement::Signature, &from);
        assert_eq!(expr, BvmScript::new_payment(42, &to));
    }

    #[test]
    fn test_future_payment() {
        let dt = Utc.ymd(2014, 11, 14).and_hms(8, 9, 10);
        let from = BvmAddr::new_rand();
        let to = BvmAddr::new_rand();

        let mut expr = BvmScript::planned_pay(dt, &from, 42, &to);
        expr.apply_witness(&Endorsement::Timestamp(dt), &from);
        assert_eq!(expr, BvmScript::new_payment(42, &to));
    }

    #[test]
    fn test_unauthorized_future_payment() {
        // Ensure timestamp will only be acknowledged if it came from the
        // whitelisted public key.
        let dt = Utc.ymd(2014, 11, 14).and_hms(8, 9, 10);
        let from = BvmAddr::new_rand();
        let to = BvmAddr::new_rand();

        let mut expr = BvmScript::planned_pay(dt, &from, 42, &to);
        let orig_expr = expr.clone();
        expr.apply_witness(&Endorsement::Timestamp(dt), &to); // <-- Attack!
        assert_eq!(expr, orig_expr);
    }

    #[test]
    fn test_cancelable_future_payment() {
        let dt = Utc.ymd(2014, 11, 14).and_hms(8, 9, 10);
        let from = BvmAddr::default();
        let to = BvmAddr::default();

        let mut expr = BvmScript::pending_planned_pay(dt, &from, 42, &to, Some(from));
        expr.apply_witness(&Endorsement::Timestamp(dt), &from);
        assert_eq!(expr, BvmScript::new_payment(42, &to));

        let mut expr = BvmScript::pending_planned_pay(dt, &from, 42, &to, Some(from));
        expr.apply_witness(&Endorsement::Signature, &from);
        assert_eq!(expr, BvmScript::new_payment(42, &from));
    }
    #[test]
    fn test_2_2_multisig_payment() {
        let from0 = BvmAddr::new_rand();
        let from1 = BvmAddr::new_rand();
        let to = BvmAddr::default();

        let mut expr = BvmScript::two_o_two_pay(&from0, &from1, 42, &to);
        expr.apply_witness(&Endorsement::Signature, &from0);
        assert_eq!(expr, BvmScript::appr_pay(&from1, 42, &to));
    }

    #[test]
    fn test_multisig_after_sig() {
        let from0 = BvmAddr::new_rand();
        let from1 = BvmAddr::new_rand();
        let from2 = BvmAddr::new_rand();
        let to = BvmAddr::default();

        let expr = BvmScript::two_o_two_pay(&from0, &from1, 42, &to);
        let mut expr = BvmScript::On(Switch::Signature(from2), Box::new(expr));

        expr.apply_witness(&Endorsement::Signature, &from2);
        expr.apply_witness(&Endorsement::Signature, &from0);
        assert_eq!(expr, BvmScript::appr_pay(&from1, 42, &to));
    }

    #[test]
    fn test_multisig_after_ts() {
        let from0 = BvmAddr::new_rand();
        let from1 = BvmAddr::new_rand();
        let dt = Utc.ymd(2014, 11, 11).and_hms(7, 7, 7);
        let to = BvmAddr::default();

        let expr = BvmScript::two_o_two_pay(&from0, &from1, 42, &to);
        let mut expr = BvmScript::On(Switch::Timestamp(dt, from0), Box::new(expr));

        expr.apply_witness(&Endorsement::Timestamp(dt), &from0);
        assert_eq!(
            expr,
            BvmScript::two_o_two_pay(&from0, &from1, 42, &to)
        );

        expr.apply_witness(&Endorsement::Signature, &from0);
        assert_eq!(expr, BvmScript::appr_pay(&from1, 42, &to));
    }
}
