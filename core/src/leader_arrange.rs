use rand::distributions::{Distribution, WeightedIndex};
use rand::SeedableRng;
use rand_chacha::ChaChaRng;
use morgan_interface::bvm_address::BvmAddr;
use std::ops::Index;
use bytes::{Bytes, BytesMut};
use prost::{EncodeError, Message};

/// Stake-weighted leader schedule for one epoch.
#[derive(Debug, Default, PartialEq)]
pub struct LeaderSchedule {
    slot_leaders: Vec<BvmAddr>,
}

impl LeaderSchedule {
    // Note: passing in zero stakers will cause a panic.
    pub fn new(ids_and_stakes: &[(BvmAddr, u64)], seed: [u8; 32], len: u64, repeat: u64) -> Self {
        let (ids, stakes): (Vec<_>, Vec<_>) = ids_and_stakes.iter().cloned().unzip();
        let rng = &mut ChaChaRng::from_seed(seed);
        let weighted_index = WeightedIndex::new(stakes).unwrap();
        let mut current_node = BvmAddr::default();
        let slot_leaders = (0..len)
            .map(|i| {
                if i % repeat == 0 {
                    current_node = ids[weighted_index.sample(rng)];
                    current_node
                } else {
                    current_node
                }
            })
            .collect();
        Self { slot_leaders }
    }
}

impl Index<u64> for LeaderSchedule {
    type Output = BvmAddr;
    fn index(&self, index: u64) -> &BvmAddr {
        let index = index as usize;
        &self.slot_leaders[index % self.slot_leaders.len()]
    }
}

impl<T: ?Sized> MessageExt for T where T: Message {}

pub trait MessageExt: Message {
    fn to_bytes(&self) -> Result<Bytes, EncodeError>
    where
        Self: Sized,
    {
        let mut bytes = BytesMut::with_capacity(self.encoded_len());
        self.encode(&mut bytes)?;
        Ok(bytes.freeze())
    }

    fn to_vec(&self) -> Result<Vec<u8>, EncodeError>
    where
        Self: Sized,
    {
        let mut vec = Vec::with_capacity(self.encoded_len());
        self.encode(&mut vec)?;
        Ok(vec)
    }
}

pub mod test_helpers {
    use super::MessageExt;
    use std::convert::TryFrom;
    use std::fmt::Debug;

    /// Assert that protobuf encoding and decoding roundtrips correctly.
    ///
    /// This is meant to be used for `prost::Context` instances.
    pub fn assert_protobuf_encode_decode<P, T>(object: &T)
    where
        T: TryFrom<P> + Into<P> + Clone + Debug + Eq,
        T::Error: Debug,
        P: prost::Message + Default,
    {
        let proto: P = object.clone().into();
        let proto_bytes = proto.to_vec().unwrap();
        let from_proto = T::try_from(proto).unwrap();
        let from_proto_bytes = T::try_from(P::decode(&proto_bytes).unwrap()).unwrap();
        assert_eq!(*object, from_proto);
        assert_eq!(*object, from_proto_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leader_schedule_index() {
        let address0 = BvmAddr::new_rand();
        let address1 = BvmAddr::new_rand();
        let leader_schedule = LeaderSchedule {
            slot_leaders: vec![address0, address1],
        };
        assert_eq!(leader_schedule[0], address0);
        assert_eq!(leader_schedule[1], address1);
        assert_eq!(leader_schedule[2], address0);
    }

    #[test]
    fn test_leader_schedule_basic() {
        let num_keys = 10;
        let stakes: Vec<_> = (0..num_keys).map(|i| (BvmAddr::new_rand(), i)).collect();

        let seed = BvmAddr::new_rand();
        let mut seed_bytes = [0u8; 32];
        seed_bytes.copy_from_slice(seed.as_ref());
        let len = num_keys * 10;
        let leader_schedule = LeaderSchedule::new(&stakes, seed_bytes, len, 1);
        let leader_schedule2 = LeaderSchedule::new(&stakes, seed_bytes, len, 1);
        assert_eq!(leader_schedule.slot_leaders.len() as u64, len);
        // Check that the same schedule is reproducibly generated
        assert_eq!(leader_schedule, leader_schedule2);
    }

    #[test]
    fn test_repeated_leader_schedule() {
        let num_keys = 10;
        let stakes: Vec<_> = (0..num_keys).map(|i| (BvmAddr::new_rand(), i)).collect();

        let seed = BvmAddr::new_rand();
        let mut seed_bytes = [0u8; 32];
        seed_bytes.copy_from_slice(seed.as_ref());
        let len = num_keys * 10;
        let repeat = 8;
        let leader_schedule = LeaderSchedule::new(&stakes, seed_bytes, len, repeat);
        assert_eq!(leader_schedule.slot_leaders.len() as u64, len);
        let mut leader_node = BvmAddr::default();
        for (i, node) in leader_schedule.slot_leaders.iter().enumerate() {
            if i % repeat as usize == 0 {
                leader_node = *node;
            } else {
                assert_eq!(leader_node, *node);
            }
        }
    }

    #[test]
    fn test_repeated_leader_schedule_specific() {
        let alice_address = BvmAddr::new_rand();
        let bob_address = BvmAddr::new_rand();
        let stakes = vec![(alice_address, 2), (bob_address, 1)];

        let seed = BvmAddr::default();
        let mut seed_bytes = [0u8; 32];
        seed_bytes.copy_from_slice(seed.as_ref());
        let len = 8;
        // What the schedule looks like without any repeats
        let leaders1 = LeaderSchedule::new(&stakes, seed_bytes, len, 1).slot_leaders;

        // What the schedule looks like with repeats
        let leaders2 = LeaderSchedule::new(&stakes, seed_bytes, len, 2).slot_leaders;
        assert_eq!(leaders1.len(), leaders2.len());

        let leaders1_expected = vec![
            alice_address,
            alice_address,
            alice_address,
            bob_address,
            alice_address,
            alice_address,
            alice_address,
            alice_address,
        ];
        let leaders2_expected = vec![
            alice_address,
            alice_address,
            alice_address,
            alice_address,
            alice_address,
            alice_address,
            bob_address,
            bob_address,
        ];

        assert_eq!(leaders1, leaders1_expected);
        assert_eq!(leaders2, leaders2_expected);
    }
}
