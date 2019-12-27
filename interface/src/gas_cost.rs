use crate::context::Context;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct GasCost {
    pub difs_per_signature: u64,
}

impl GasCost {
    pub fn new(difs_per_signature: u64) -> Self {
        Self {
            difs_per_signature,
        }
    }

    pub fn calculate_fee(&self, message: &Context) -> u64 {
        self.difs_per_signature * u64::from(message.header.num_required_signatures)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bvm_address::BvmAddr;
    use crate::sys_opcode;

    #[test]
    fn test_fee_calculator_calculate_fee() {
        // Default: no fee.
        let message = Context::new(vec![]);
        assert_eq!(GasCost::default().calculate_fee(&message), 0);

        // No signature, no fee.
        assert_eq!(GasCost::new(1).calculate_fee(&message), 0);

        // One signature, a fee.
        let pubkey0 = BvmAddr::new(&[0; 32]);
        let pubkey1 = BvmAddr::new(&[1; 32]);
        let ix0 = sys_opcode::transfer(&pubkey0, &pubkey1, 1);
        let message = Context::new(vec![ix0]);
        assert_eq!(GasCost::new(2).calculate_fee(&message), 2);

        // Two signatures, double the fee.
        let ix0 = sys_opcode::transfer(&pubkey0, &pubkey1, 1);
        let ix1 = sys_opcode::transfer(&pubkey1, &pubkey0, 1);
        let message = Context::new(vec![ix0, ix1]);
        assert_eq!(GasCost::new(2).calculate_fee(&message), 4);
    }
}
