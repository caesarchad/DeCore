use super::*;

pub struct RootedSlotIterator<'a> {
    next_slots: Vec<u64>,
    block_buffer_pool: &'a super::BlockBufferPool,
}

impl<'a> RootedSlotIterator<'a> {
    pub fn new(start_slot: u64, block_buffer_pool: &'a super::BlockBufferPool) -> Result<Self> {
        if block_buffer_pool.is_genesis(start_slot) {
            Ok(Self {
                next_slots: vec![start_slot],
                block_buffer_pool,
            })
        } else {
            Err(Error::BlockBufferPoolError(BlockBufferPoolError::SlotNotRooted))
        }
    }
}
impl<'a> Iterator for RootedSlotIterator<'a> {
    type Item = (u64, super::MetaInfoCol);

    fn next(&mut self) -> Option<Self::Item> {
        // Clone b/c passing the closure to the map below requires exclusive access to
        // `self`, which is borrowed here if we don't clone.
        let rooted_slot = self
            .next_slots
            .iter()
            .find(|x| self.block_buffer_pool.is_genesis(**x))
            .cloned();

        rooted_slot.map(|rooted_slot| {
            let slot_meta = self
                .block_buffer_pool
                .meta(rooted_slot)
                .expect("Database failure, couldnt fetch MetaInfoCol")
                .expect("MetaInfoCol in iterator didn't exist");

            self.next_slots = slot_meta.next_slots.clone();
            (rooted_slot, slot_meta)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_buffer_pool_processor::tests::fill_block_buffer_pool_slot_with_ticks;

    #[test]
    fn test_rooted_slot_iterator() {
        let block_buffer_pool_path = get_tmp_ledger_path("test_rooted_slot_iterator");
        let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();
        block_buffer_pool.set_genesis(0, 0).unwrap();
        let ticks_per_slot = 5;
        /*
            Build a block_buffer_pool in the ledger with the following fork structure:

                 slot 0
                   |
                 slot 1  <-- set_genesis(true)
                 /   \
            slot 2   |
               /     |
            slot 3   |
                     |
                   slot 4

        */

        // Fork 1, ending at slot 3
        let last_entry_hash = Hash::default();
        let fork_point = 1;
        let mut fork_hash = Hash::default();
        for slot in 0..=3 {
            let parent = {
                if slot == 0 {
                    0
                } else {
                    slot - 1
                }
            };
            let last_entry_hash = fill_block_buffer_pool_slot_with_ticks(
                &block_buffer_pool,
                ticks_per_slot,
                slot,
                parent,
                last_entry_hash,
            );

            if slot == fork_point {
                fork_hash = last_entry_hash;
            }
        }

        // Fork 2, ending at slot 4
        let _ =
            fill_block_buffer_pool_slot_with_ticks(&block_buffer_pool, ticks_per_slot, 4, fork_point, fork_hash);

        // Set a root
        block_buffer_pool.set_genesis(3, 0).unwrap();

        // Trying to get an iterator on a different fork will error
        assert!(RootedSlotIterator::new(4, &block_buffer_pool).is_err());

        // Trying to get an iterator on any slot on the root fork should succeed
        let result: Vec<_> = RootedSlotIterator::new(3, &block_buffer_pool)
            .unwrap()
            .into_iter()
            .map(|(slot, _)| slot)
            .collect();
        let expected = vec![3];
        assert_eq!(result, expected);

        let result: Vec<_> = RootedSlotIterator::new(0, &block_buffer_pool)
            .unwrap()
            .into_iter()
            .map(|(slot, _)| slot)
            .collect();
        let expected = vec![0, 1, 2, 3];
        assert_eq!(result, expected);

        drop(block_buffer_pool);
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }
}
