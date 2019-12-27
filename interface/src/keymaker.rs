//! The `signature` module provides functionality for public, and private keys.

use rand::{rngs::OsRng,RngCore,Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use rayon::prelude::*;
use crate::signature::Keypair;
use serde::{Deserialize, Serialize};


pub struct ChaKeys {
    generator: ChaChaRng,
}

impl ChaKeys {
    pub fn new(seed: [u8; 32]) -> ChaKeys {
        let generator = ChaChaRng::from_seed(seed);
        ChaKeys { generator }
    }

    fn chacha_seed(&mut self) -> [u8; 32] {
        let mut seed = [0u8; 32];
        self.generator.fill(&mut seed);
        seed
    }

    
    fn chacha_seed_vec(&mut self, n: u64) -> Vec<[u8; 32]> {
        (0..n).map(|_| self.chacha_seed()).collect()
    }

    pub fn ed25519_keypair(&mut self) -> Keypair {
        Keypair::generate(&mut self.generator)
    }

    pub fn ed25519_keypair_vec(&mut self, n: u64) -> Vec<Keypair> {
        self.chacha_seed_vec(n)
            .into_par_iter()
            .map(|seed| Keypair::generate(&mut ChaChaRng::from_seed(seed)))
            .collect()
    }
}

pub struct MachineKeys{
    generator: OsRng,
}

impl MachineKeys{
    pub fn new() -> MachineKeys {
        let generator = OsRng::new().expect("can't access OsRng");
        MachineKeys { generator }
    }
    fn machine_seed(&mut self) -> [u8; 32] {
        let mut seed = [0u8; 32];
        self.generator.fill_bytes(&mut seed);
        seed
    }
    fn machine_seed_vec(&mut self, n: u64) -> Vec<[u8; 32]> {
        (0..n).map(|_| self.machine_seed()).collect()
    }
    pub fn ed25519_keypair(&mut self) -> Keypair {
        Keypair::generate(&mut self.generator)
    }
    pub fn ed25519_keypair_vec(&mut self, n: u64) -> Vec<Keypair> {
        self.machine_seed_vec(n)
            .into_par_iter()
            .map(|seed| Keypair::generate(&mut rand::rngs::StdRng::from_seed(seed)))
            .collect()
    }
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default)]
pub struct LoggerConfig {
    // Use async logging
    pub is_async: bool,
    // chan_size of slog async drain for node logging.
    pub chan_size: usize,
}

impl Default for LoggerConfig {
    fn default() -> LoggerConfig {
        LoggerConfig {
            is_async: true,
            chan_size: 256,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    pub use crate::bvm_address::BvmAddr;
    use crate::signature::KeypairUtil;
    use std::collections::HashSet;

    #[test]
    fn test_new_key_is_deterministic() {
        let seed = [0u8; 32];
        let mut gen0 = ChaKeys::new(seed);
        let mut gen1 = ChaKeys::new(seed);

        for _ in 0..100 {
            assert_eq!(gen0.chacha_seed().to_vec(), gen1.chacha_seed().to_vec());
        }
    }

    #[test]
    fn test_gen_keypair_is_deterministic() {
        let seed = [0u8; 32];
        let mut gen0 = ChaKeys::new(seed);
        let mut gen1 = ChaKeys::new(seed);
        assert_eq!(
            gen0.ed25519_keypair().to_bytes().to_vec(),
            gen1.ed25519_keypair().to_bytes().to_vec()
        );
    }

    fn gen_n_pubkeys(seed: [u8; 32], n: u64) -> HashSet<BvmAddr> {
        ChaKeys::new(seed)
            .ed25519_keypair_vec(n)
            .into_iter()
            .map(|x| x.pubkey())
            .collect()
    }

    #[test]
    fn test_gen_n_pubkeys_deterministic() {
        let seed = [0u8; 32];
        assert_eq!(gen_n_pubkeys(seed, 50), gen_n_pubkeys(seed, 50));
    }
}
