//! The `block_buffer_pool` module provides functions for parallel verification of the
//! system wide clock.
use crate::entry_info::Entry;
use crate::expunge::{self, Session};
use crate::packet::{Blob, SharedBlob, BLOB_HEADER_SIZE};
use crate::result::{Error, Result};

#[cfg(feature = "kvstore")]
use morgan_kvstore as kvstore;

use bincode::deserialize;

use hashbrown::HashMap;

#[cfg(not(feature = "kvstore"))]
use rocksdb;

use morgan_metricbot::{datapoint_error, datapoint_info};

use morgan_interface::genesis_block::GenesisBlock;
use morgan_interface::hash::Hash;
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_helper::logHelper::*;

use std::borrow::{Borrow, Cow};
use std::cell::RefCell;
use std::cmp;
use std::fs;
use std::io;
use std::rc::Rc;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, RwLock};

pub use self::meta::*;
pub use self::rooted_slot_iterator::*;

mod db;
mod meta;
mod rooted_slot_iterator;

macro_rules! db_imports {
    { $mod:ident, $db:ident, $db_path:expr } => {
        mod $mod;

        use $mod::$db;
        use db::columns as cf;

        pub use db::columns;

        pub type Database = db::Database<$db>;
        pub type Cursor<C> = db::Cursor<$db, C>;
        pub type LedgerColumn<C> = db::LedgerColumn<$db, C>;
        pub type WriteBatch = db::WriteBatch<$db>;
        type BatchProcessor = db::BatchProcessor<$db>;

        pub trait Column: db::Column<$db> {}
        impl<C: db::Column<$db>> Column for C {}

        pub const BLOCKBUFFERPOOL_DIR: &str = $db_path;
    };
}

#[cfg(not(feature = "kvstore"))]
db_imports! {rocks, RocksDB, "rocksdb"}
#[cfg(feature = "kvstore")]
db_imports! {kvs, Kvs, "kvstore"}

pub const MAX_COMPLETED_SLOTS_IN_CHANNEL: usize = 100_000;

pub type CompletedSlotsReceiver = Receiver<Vec<u64>>;

#[derive(Debug)]
pub enum BlockBufferPoolError {
    BlobForIndexExists,
    InvalidBlobData,
    RocksDb(rocksdb::Error),
    #[cfg(feature = "kvstore")]
    KvsDb(kvstore::Error),
    SlotNotRooted,
}

// ledger window
pub struct BlockBufferPool {
    db: Arc<Database>,
    m_c_g: LedgerColumn<cf::MetaInfoCol>,
    d_c_g: LedgerColumn<cf::DataColumn>,
    e_c_g: LedgerColumn<cf::ErasureColumn>,
    e_m_c_g: LedgerColumn<cf::ErasureMetaColumn>,
    sgt_c_g: LedgerColumn<cf::SingletonColumn>,
    batch_processor: Arc<RwLock<BatchProcessor>>,
    session: Arc<expunge::Session>,
    pub new_blobs_signals: Vec<SyncSender<bool>>,
    pub completed_slots_senders: Vec<SyncSender<Vec<u64>>>,
}

// Column family for metadata about a leader slot
pub const METAINFO_COLUMN_GROUP: &str = "meta";
// Column family for the data in a leader slot
pub const DATA_COLUMN_GROUP: &str = "data";
// Column family for erasure data
pub const ERASURE_COLUMN_GROUP: &str = "erasure";
pub const ERASURE_METAINFO_COLUMN_GROUP: &str = "erasure_meta";
// Column family for orphans data
pub const SINGLETON_COLUMN_GROUP: &str = "orphans";
// Column family for root data
pub const GENESIS_COLUMN_GROUP: &str = "root";

impl BlockBufferPool {
    /// Opens a Ledger in directory, provides "infinite" window of blobs
    pub fn open_ledger_file(ledger_path: &str) -> Result<BlockBufferPool> {
        use std::path::Path;

        fs::create_dir_all(&ledger_path)?;
        let ledger_path = Path::new(&ledger_path).join(BLOCKBUFFERPOOL_DIR);

        // Open the database
        let db = Database::open(&ledger_path)?;

        let batch_processor = unsafe { Arc::new(RwLock::new(db.batch_processor())) };

        // Create the metadata column family
        let m_c_g = db.column();

        // Create the data column family
        let d_c_g = db.column();

        // Create the erasure column family
        let e_c_g = db.column();

        let e_m_c_g = db.column();

        // Create the orphans column family. An "orphan" is defined as
        // the head of a detached chain of slots, i.e. a slot with no
        // known parent
        let sgt_c_g = db.column();

        // setup erasure
        let session = Arc::new(expunge::Session::default());

        let db = Arc::new(db);

        Ok(BlockBufferPool {
            db,
            m_c_g,
            d_c_g,
            e_c_g,
            e_m_c_g,
            sgt_c_g,
            session,
            new_blobs_signals: vec![],
            batch_processor,
            completed_slots_senders: vec![],
        })
    }

    pub fn open_by_message(
        ledger_path: &str,
    ) -> Result<(Self, Receiver<bool>, CompletedSlotsReceiver)> {
        let mut block_buffer_pool = Self::open_ledger_file(ledger_path)?;
        let (signal_sender, signal_receiver) = sync_channel(1);
        let (completed_slots_sender, completed_slots_receiver) =
            sync_channel(MAX_COMPLETED_SLOTS_IN_CHANNEL);
        block_buffer_pool.new_blobs_signals = vec![signal_sender];
        block_buffer_pool.completed_slots_senders = vec![completed_slots_sender];

        Ok((block_buffer_pool, signal_receiver, completed_slots_receiver))
    }

    pub fn remove_ledger_file(ledger_path: &str) -> Result<()> {
        // Database::destroy() fails is the path doesn't exist
        fs::create_dir_all(ledger_path)?;
        let path = std::path::Path::new(ledger_path).join(BLOCKBUFFERPOOL_DIR);
        Database::destroy(&path)
    }

    pub fn meta(&self, slot: u64) -> Result<Option<MetaInfoCol>> {
        self.m_c_g.get(slot)
    }

    pub fn erasure_meta(&self, slot: u64, set_index: u64) -> Result<Option<ErasureMetaColumn>> {
        self.e_m_c_g.get((slot, set_index))
    }

    pub fn singleton_slot(&self, slot: u64) -> Result<Option<bool>> {
        self.sgt_c_g.get(slot)
    }

    pub fn based_slot_repeater<'a>(&'a self, slot: u64) -> Result<RootedSlotIterator<'a>> {
        RootedSlotIterator::new(slot, self)
    }

    pub fn meta_info_col_looper(&self, slot: u64) -> Result<impl Iterator<Item = (u64, MetaInfoCol)>> {
        let meta_iter = self.db.iter::<cf::MetaInfoCol>(Some(slot))?;
        Ok(meta_iter.map(|(slot, slot_meta_bytes)| {
            (
                slot,
                deserialize(&slot_meta_bytes)
                    .unwrap_or_else(|_| panic!("Could not deserialize MetaInfoCol for slot {}", slot)),
            )
        }))
    }

    pub fn data_col_looper(
        &self,
        slot: u64,
    ) -> Result<impl Iterator<Item = ((u64, u64), Box<[u8]>)>> {
        let slot_iterator = self.db.iter::<cf::DataColumn>(Some((slot, 0)))?;
        Ok(slot_iterator.take_while(move |((blob_slot, _), _)| *blob_slot == slot))
    }

    pub fn record_public_objs<I>(&self, shared_blobs: I) -> Result<()>
    where
        I: IntoIterator,
        I::Item: Borrow<SharedBlob>,
    {
        let c_blobs: Vec<_> = shared_blobs
            .into_iter()
            .map(move |s| s.borrow().clone())
            .collect();

        let r_blobs: Vec<_> = c_blobs.iter().map(move |b| b.read().unwrap()).collect();

        let blobs = r_blobs.iter().map(|s| &**s);

        self.insert_data_blobs(blobs)
    }

    pub fn update_blobs<I>(&self, blobs: I) -> Result<()>
    where
        I: IntoIterator,
        I::Item: Borrow<Blob>,
    {
        self.insert_data_blobs(blobs)
    }

    pub fn update_entries<I>(
        &self,
        start_slot: u64,
        num_ticks_in_start_slot: u64,
        start_index: u64,
        ticks_per_slot: u64,
        entries: I,
    ) -> Result<()>
    where
        I: IntoIterator,
        I::Item: Borrow<Entry>,
    {
        assert!(num_ticks_in_start_slot < ticks_per_slot);
        let mut remaining_ticks_in_slot = ticks_per_slot - num_ticks_in_start_slot;

        let mut blobs = vec![];
        let mut current_index = start_index;
        let mut current_slot = start_slot;
        let mut parent_slot = {
            if current_slot == 0 {
                current_slot
            } else {
                current_slot - 1
            }
        };
        // Find all the entries for start_slot
        for entry in entries {
            if remaining_ticks_in_slot == 0 {
                current_slot += 1;
                current_index = 0;
                parent_slot = current_slot - 1;
                remaining_ticks_in_slot = ticks_per_slot;
            }

            let mut b = entry.borrow().to_blob();

            if entry.borrow().is_tick() {
                remaining_ticks_in_slot -= 1;
                if remaining_ticks_in_slot == 0 {
                    b.set_is_last_in_slot();
                }
            }

            b.set_index(current_index);
            b.set_slot(current_slot);
            b.set_parent(parent_slot);
            blobs.push(b);

            current_index += 1;
        }

        self.update_blobs(&blobs)
    }

    pub fn insert_data_blobs<I>(&self, new_blobs: I) -> Result<()>
    where
        I: IntoIterator,
        I::Item: Borrow<Blob>,
    {
        let db = &*self.db;
        let mut batch_processor = self.batch_processor.write().unwrap();
        let mut write_batch = batch_processor.batch()?;

        let new_blobs: Vec<_> = new_blobs.into_iter().collect();
        let mut restored_data = vec![];

        let mut prev_inserted_blob_datas = HashMap::new();
        // A map from slot to a 2-tuple of metadata: (working copy, backup copy),
        // so we can detect changes to the slot metadata later
        let mut slot_meta_working_set = HashMap::new();
        let mut erasure_meta_working_set = HashMap::new();

        for blob in new_blobs.iter() {
            let blob = blob.borrow();
            let blob_slot = blob.slot();

            let set_index = ErasureMetaColumn::set_index_for(blob.index());
            erasure_meta_working_set
                .entry((blob_slot, set_index))
                .or_insert_with(|| {
                    self.e_m_c_g
                        .get((blob_slot, set_index))
                        .expect("Expect database get to succeed")
                        .unwrap_or_else(|| ErasureMetaColumn::new(set_index))
                });
        }

        put_blob_into_ledger_in_batch(
            new_blobs.iter().map(Borrow::borrow),
            &db,
            &mut slot_meta_working_set,
            &mut erasure_meta_working_set,
            &mut prev_inserted_blob_datas,
            &mut write_batch,
        )?;

        for (&(slot, _), erasure_meta) in erasure_meta_working_set.iter_mut() {
            if let Some((data, coding)) = attempt_erasure_restore(
                &db,
                &self.session,
                &erasure_meta,
                slot,
                &prev_inserted_blob_datas,
                None,
            )? {
                for data_blob in data {
                    restored_data.push(data_blob);
                }

                for coding_blob in coding {
                    erasure_meta.set_coding_present(coding_blob.index(), true);

                    write_batch.put_bytes::<cf::ErasureColumn>(
                        (coding_blob.slot(), coding_blob.index()),
                        &coding_blob.data[..BLOB_HEADER_SIZE + coding_blob.size()],
                    )?;
                }
            }
        }

        put_blob_into_ledger_in_batch(
            restored_data.iter(),
            &db,
            &mut slot_meta_working_set,
            &mut erasure_meta_working_set,
            &mut prev_inserted_blob_datas,
            &mut write_batch,
        )?;

        // Handle chaining for the working set
        process_catenating(&db, &mut write_batch, &slot_meta_working_set)?;
        let mut should_signal = false;
        let mut newly_completed_slots = vec![];

        // Check if any metadata was changed, if so, insert the new version of the
        // metadata into the write batch
        for (slot, (meta, meta_backup)) in slot_meta_working_set.iter() {
            let meta: &MetaInfoCol = &RefCell::borrow(&*meta);
            if !self.completed_slots_senders.is_empty()
                && is_newly_completed_slot(meta, meta_backup)
            {
                newly_completed_slots.push(*slot);
            }
            // Check if the working copy of the metadata has changed
            if Some(meta) != meta_backup.as_ref() {
                should_signal = should_signal || renewal_on_slot(meta, &meta_backup);
                write_batch.put::<cf::MetaInfoCol>(*slot, &meta)?;
            }
        }

        for ((slot, set_index), erasure_meta) in erasure_meta_working_set {
            write_batch.put::<cf::ErasureMetaColumn>((slot, set_index), &erasure_meta)?;
        }

        batch_processor.write(write_batch)?;

        if should_signal {
            for signal in &self.new_blobs_signals {
                let _ = signal.try_send(true);
            }
        }

        if !self.completed_slots_senders.is_empty() && !newly_completed_slots.is_empty() {
            let mut slots: Vec<_> = (0..self.completed_slots_senders.len() - 1)
                .map(|_| newly_completed_slots.clone())
                .collect();

            slots.push(newly_completed_slots);

            for (signal, slots) in self.completed_slots_senders.iter().zip(slots.into_iter()) {
                let res = signal.try_send(slots);
                if let Err(TrySendError::Full(_)) = res {
                    datapoint_error!(
                        "block_buffer_pool_error",
                        (
                            "error",
                            "Unable to send newly completed slot because channel is full"
                                .to_string(),
                            String
                        ),
                    );
                }
            }
        }

        Ok(())
    }

    // Fill 'buf' with num_blobs or most number of consecutive
    // whole blobs that fit into buf.len()
    //
    // Return tuple of (number of blob read, total size of blobs read)
    pub fn read_db_by_bytes(
        &self,
        start_index: u64,
        num_blobs: u64,
        buf: &mut [u8],
        slot: u64,
    ) -> Result<(u64, u64)> {
        let mut db_iterator = self.db.cursor::<cf::DataColumn>()?;

        db_iterator.seek((slot, start_index));
        let mut total_blobs = 0;
        let mut total_current_size = 0;
        for expected_index in start_index..start_index + num_blobs {
            if !db_iterator.valid() {
                if expected_index == start_index {
                    return Err(Error::IO(io::Error::new(
                        io::ErrorKind::NotFound,
                        "Blob at start_index not found",
                    )));
                } else {
                    break;
                }
            }

            // Check key is the next sequential key based on
            // blob index
            let (_, index) = db_iterator.key().expect("Expected valid key");
            if index != expected_index {
                break;
            }

            // Get the blob data
            let value = &db_iterator.value_bytes();

            if value.is_none() {
                break;
            }

            let value = value.as_ref().unwrap();
            let blob_data_len = value.len();

            if total_current_size + blob_data_len > buf.len() {
                break;
            }

            buf[total_current_size..total_current_size + value.len()].copy_from_slice(value);
            total_current_size += blob_data_len;
            total_blobs += 1;

            // TODO: Change this logic to support looking for data
            // that spans multiple leader slots, once we support
            // a window that knows about different leader slots
            db_iterator.next();
        }

        Ok((total_blobs, total_current_size as u64))
    }

    pub fn fetch_coding_col_by_bytes(&self, slot: u64, index: u64) -> Result<Option<Vec<u8>>> {
        self.e_c_g.get_bytes((slot, index))
    }

    pub fn remove_coding_blob(&self, slot: u64, index: u64) -> Result<()> {
        let set_index = ErasureMetaColumn::set_index_for(index);
        let mut batch_processor = self.batch_processor.write().unwrap();

        let mut erasure_meta = self
            .e_m_c_g
            .get((slot, set_index))?
            .unwrap_or_else(|| ErasureMetaColumn::new(set_index));

        erasure_meta.set_coding_present(index, false);

        let mut batch = batch_processor.batch()?;

        batch.delete::<cf::ErasureColumn>((slot, index))?;
        batch.put::<cf::ErasureMetaColumn>((slot, set_index), &erasure_meta)?;

        batch_processor.write(batch)?;
        Ok(())
    }

    pub fn fetch_data_blob_bytes(&self, slot: u64, index: u64) -> Result<Option<Vec<u8>>> {
        self.d_c_g.get_bytes((slot, index))
    }

    /// For benchmarks, testing, and setup.
    /// Does no metadata tracking. Use with care.
    pub fn insert_data_blob_bytes(&self, slot: u64, index: u64, bytes: &[u8]) -> Result<()> {
        self.d_c_g.put_bytes((slot, index), bytes)
    }

    /// For benchmarks, testing, and setup.
    /// Does no metadata tracking. Use with care.
    pub fn insert_coding_blob_bytes_raw(&self, slot: u64, index: u64, bytes: &[u8]) -> Result<()> {
        self.e_c_g.put_bytes((slot, index), bytes)
    }

    /// this function will insert coding blobs and also automatically track erasure-related
    /// metadata. If recovery is available it will be done
    pub fn insert_coding_blob_bytes(&self, slot: u64, index: u64, bytes: &[u8]) -> Result<()> {
        let set_index = ErasureMetaColumn::set_index_for(index);
        let mut batch_processor = self.batch_processor.write().unwrap();

        let mut erasure_meta = self
            .e_m_c_g
            .get((slot, set_index))?
            .unwrap_or_else(|| ErasureMetaColumn::new(set_index));

        erasure_meta.set_coding_present(index, true);
        erasure_meta.set_size(bytes.len() - BLOB_HEADER_SIZE);

        let mut writebatch = batch_processor.batch()?;

        writebatch.put_bytes::<cf::ErasureColumn>((slot, index), bytes)?;

        let restored_data = {
            if let Some((data, coding)) = attempt_erasure_restore(
                &self.db,
                &self.session,
                &erasure_meta,
                slot,
                &HashMap::new(),
                Some((index, bytes)),
            )? {
                let mut erasure_meta_working_set = HashMap::new();
                erasure_meta_working_set.insert((slot, set_index), erasure_meta);
                erasure_meta = *erasure_meta_working_set.values().next().unwrap();

                for coding_blob in coding {
                    erasure_meta.set_coding_present(coding_blob.index(), true);

                    writebatch.put_bytes::<cf::ErasureColumn>(
                        (coding_blob.slot(), coding_blob.index()),
                        &coding_blob.data[..BLOB_HEADER_SIZE + coding_blob.size()],
                    )?;
                }
                Some(data)
            } else {
                None
            }
        };

        writebatch.put::<cf::ErasureMetaColumn>((slot, set_index), &erasure_meta)?;
        batch_processor.write(writebatch)?;
        drop(batch_processor);
        if let Some(data) = restored_data {
            if !data.is_empty() {
                self.insert_data_blobs(&data)?;
            }
        }

        Ok(())
    }

    pub fn insert_multiple_coding_blob_bytes(&self, coding_blobs: &[SharedBlob]) -> Result<()> {
        for shared_coding_blob in coding_blobs {
            let blob = shared_coding_blob.read().unwrap();
            assert!(blob.is_coding());
            let size = blob.size() + BLOB_HEADER_SIZE;
            self.insert_coding_blob_bytes(blob.slot(), blob.index(), &blob.data[..size])?
        }

        Ok(())
    }

    pub fn fetch_data_blob(&self, slot: u64, blob_index: u64) -> Result<Option<Blob>> {
        let bytes = self.fetch_data_blob_bytes(slot, blob_index)?;
        Ok(bytes.map(|bytes| {
            let blob = Blob::new(&bytes);
            assert!(blob.slot() == slot);
            assert!(blob.index() == blob_index);
            blob
        }))
    }

    pub fn fetch_entries_bytes(
        &self,
        _start_index: u64,
        _num_entries: u64,
        _buf: &mut [u8],
    ) -> io::Result<(u64, u64)> {
        Err(io::Error::new(io::ErrorKind::Other, "TODO"))
    }

    // Given a start and end entry index, find all the missing
    // indexes in the ledger in the range [start_index, end_index)
    // for the slot with the specified slot
    fn search_absent_indexes<C>(
        db_iterator: &mut Cursor<C>,
        slot: u64,
        start_index: u64,
        end_index: u64,
        max_missing: usize,
    ) -> Vec<u64>
    where
        C: Column<Index = (u64, u64)>,
    {
        if start_index >= end_index || max_missing == 0 {
            return vec![];
        }

        let mut missing_indexes = vec![];

        // Seek to the first blob with index >= start_index
        db_iterator.seek((slot, start_index));

        // The index of the first missing blob in the slot
        let mut prev_index = start_index;
        'outer: loop {
            if !db_iterator.valid() {
                for i in prev_index..end_index {
                    missing_indexes.push(i);
                    if missing_indexes.len() == max_missing {
                        break;
                    }
                }
                break;
            }
            let (current_slot, index) = db_iterator.key().expect("Expect a valid key");
            let current_index = {
                if current_slot > slot {
                    end_index
                } else {
                    index
                }
            };
            let upper_index = cmp::min(current_index, end_index);

            for i in prev_index..upper_index {
                missing_indexes.push(i);
                if missing_indexes.len() == max_missing {
                    break 'outer;
                }
            }

            if current_slot > slot {
                break;
            }

            if current_index >= end_index {
                break;
            }

            prev_index = current_index + 1;
            db_iterator.next();
        }

        missing_indexes
    }

    pub fn search_absent_data_indexes(
        &self,
        slot: u64,
        start_index: u64,
        end_index: u64,
        max_missing: usize,
    ) -> Vec<u64> {
        if let Ok(mut db_iterator) = self.db.cursor::<cf::DataColumn>() {
            Self::search_absent_indexes(&mut db_iterator, slot, start_index, end_index, max_missing)
        } else {
            vec![]
        }
    }

    /// Returns the entry vector for the slot starting with `blob_start_index`
    pub fn fetch_slot_entries(
        &self,
        slot: u64,
        blob_start_index: u64,
        max_entries: Option<u64>,
    ) -> Result<Vec<Entry>> {
        self.fetch_slot_entries_by_blob_len(slot, blob_start_index, max_entries)
            .map(|x| x.0)
    }

    pub fn fetch_iterator_blobs(&self) -> impl Iterator<Item = Blob> + '_ {
        let iter = self.db.iter::<cf::DataColumn>(None).unwrap();
        iter.map(|(_, blob_data)| Blob::new(&blob_data))
    }

    /// Return an iterator for all the entries in the given file.
    pub fn fetch_iterator(&self) -> Result<impl Iterator<Item = Entry>> {
        use crate::entry_info::EntrySlice;
        use std::collections::VecDeque;

        struct EntryIterator {
            db_iterator: Cursor<cf::DataColumn>,

            // TODO: remove me when replay_phase is iterating by block (BlockBufferPool)
            //    this verification is duplicating that of replay_phase, which
            //    can do this in parallel
            transaction_seal: Option<Hash>,
            // https://github.com/rust-rocksdb/rust-rocksdb/issues/234
            //   rocksdb issue: the _block_buffer_pool member must be lower in the struct to prevent a crash
            //   when the db_iterator member above is dropped.
            //   _block_buffer_pool is unused, but dropping _block_buffer_pool results in a broken db_iterator
            //   you have to hold the database open in order to iterate over it, and in order
            //   for db_iterator to be able to run Drop
            //    _block_buffer_pool: BlockBufferPool,
            entries: VecDeque<Entry>,
        }

        impl Iterator for EntryIterator {
            type Item = Entry;

            fn next(&mut self) -> Option<Entry> {
                if !self.entries.is_empty() {
                    return Some(self.entries.pop_front().unwrap());
                }

                if self.db_iterator.valid() {
                    if let Some(value) = self.db_iterator.value_bytes() {
                        if let Ok(next_entries) =
                            deserialize::<Vec<Entry>>(&value[BLOB_HEADER_SIZE..])
                        {
                            if let Some(transaction_seal) = self.transaction_seal {
                                if !next_entries.verify(&transaction_seal) {
                                    return None;
                                }
                            }
                            self.db_iterator.next();
                            if next_entries.is_empty() {
                                return None;
                            }
                            self.entries = VecDeque::from(next_entries);
                            let entry = self.entries.pop_front().unwrap();
                            self.transaction_seal = Some(entry.hash);
                            return Some(entry);
                        }
                    }
                }
                None
            }
        }
        let mut db_iterator = self.db.cursor::<cf::DataColumn>()?;

        db_iterator.seek_to_first();
        Ok(EntryIterator {
            entries: VecDeque::new(),
            db_iterator,
            transaction_seal: None,
        })
    }

    pub fn fetch_slot_entries_by_blob_len(
        &self,
        slot: u64,
        blob_start_index: u64,
        max_entries: Option<u64>,
    ) -> Result<(Vec<Entry>, usize)> {
        // Find the next consecutive block of blobs.
        let consecutive_blobs = fetch_slot_continuous_blobs(
            slot,
            &self.db,
            &HashMap::new(),
            blob_start_index,
            max_entries,
        )?;
        let num = consecutive_blobs.len();
        Ok((deserialize_blobs(&consecutive_blobs), num))
    }

    // Returns slots connecting to any element of the list `slots`.
    pub fn fetch_slot_from(&self, slots: &[u64]) -> Result<HashMap<u64, Vec<u64>>> {
        // Return error if there was a database error during lookup of any of the
        // slot indexes
        let slot_metas: Result<Vec<Option<MetaInfoCol>>> =
            slots.iter().map(|slot| self.meta(*slot)).collect();

        let slot_metas = slot_metas?;
        let result: HashMap<u64, Vec<u64>> = slots
            .iter()
            .zip(slot_metas)
            .filter_map(|(height, meta)| meta.map(|meta| (*height, meta.next_slots)))
            .collect();

        Ok(result)
    }

    pub fn fetch_entry_from_deserialized_blob(data: &[u8]) -> Result<Vec<Entry>> {
        let entries = deserialize(data)?;
        Ok(entries)
    }

    pub fn is_genesis(&self, slot: u64) -> bool {
        if let Ok(Some(true)) = self.db.get::<cf::GenesisColumn>(slot) {
            true
        } else {
            false
        }
    }

    pub fn set_genesis(&self, new_root: u64, prev_root: u64) -> Result<()> {
        let mut current_slot = new_root;
        unsafe {
            let mut batch_processor = self.db.batch_processor();
            let mut write_batch = batch_processor.batch()?;
            if new_root == 0 {
                write_batch.put::<cf::GenesisColumn>(0, &true)?;
            } else {
                while current_slot != prev_root {
                    write_batch.put::<cf::GenesisColumn>(current_slot, &true)?;
                    current_slot = self.meta(current_slot).unwrap().unwrap().parent_slot;
                }
            }

            batch_processor.write(write_batch)?;
        }
        Ok(())
    }

    pub fn fetch_singletons(&self, max: Option<usize>) -> Vec<u64> {
        let mut results = vec![];

        let mut iter = self.db.cursor::<cf::SingletonColumn>().unwrap();
        iter.seek_to_first();
        while iter.valid() {
            if let Some(max) = max {
                if results.len() > max {
                    break;
                }
            }
            results.push(iter.key().unwrap());
            iter.next();
        }
        results
    }

    // Handle special case of writing genesis blobs. For instance, the first two entries
    // don't count as ticks, even if they're empty entries
    fn update_genesis_blobs(&self, blobs: &[Blob]) -> Result<()> {
        // TODO: change bootstrap height to number of slots
        let mut bootstrap_meta = MetaInfoCol::new(0, 1);
        let last = blobs.last().unwrap();

        let mut batch_processor = self.batch_processor.write().unwrap();

        bootstrap_meta.consumed = last.index() + 1;
        bootstrap_meta.received = last.index() + 1;
        bootstrap_meta.is_connected = true;

        let mut batch = batch_processor.batch()?;
        batch.put::<cf::MetaInfoCol>(0, &bootstrap_meta)?;
        for blob in blobs {
            let serialized_blob_datas = &blob.data[..BLOB_HEADER_SIZE + blob.size()];
            batch.put_bytes::<cf::DataColumn>((blob.slot(), blob.index()), serialized_blob_datas)?;
        }
        batch_processor.write(batch)?;
        Ok(())
    }
}

fn put_blob_into_ledger_in_batch<'a, I>(
    new_blobs: I,
    db: &Database,
    slot_meta_working_set: &mut HashMap<u64, (Rc<RefCell<MetaInfoCol>>, Option<MetaInfoCol>)>,
    erasure_meta_working_set: &mut HashMap<(u64, u64), ErasureMetaColumn>,
    prev_inserted_blob_datas: &mut HashMap<(u64, u64), &'a [u8]>,
    write_batch: &mut WriteBatch,
) -> Result<()>
where
    I: IntoIterator<Item = &'a Blob>,
{
    for blob in new_blobs.into_iter() {
        let inserted = verify_new_data_blob(
            blob,
            db,
            slot_meta_working_set,
            prev_inserted_blob_datas,
            write_batch,
        );

        if inserted {
            erasure_meta_working_set
                .get_mut(&(blob.slot(), ErasureMetaColumn::set_index_for(blob.index())))
                .unwrap()
                .set_data_present(blob.index(), true);
        }
    }

    Ok(())
}

/// Insert a blob into ledger, updating the slot_meta if necessary
fn put_blob_into_ledger<'a>(
    blob_to_insert: &'a Blob,
    db: &Database,
    prev_inserted_blob_datas: &mut HashMap<(u64, u64), &'a [u8]>,
    slot_meta: &mut MetaInfoCol,
    write_batch: &mut WriteBatch,
) -> Result<()> {
    let blob_index = blob_to_insert.index();
    let blob_slot = blob_to_insert.slot();
    let blob_size = blob_to_insert.size();

    let new_consumed = {
        if slot_meta.consumed == blob_index {
            let blob_datas = fetch_slot_continuous_blobs(
                blob_slot,
                db,
                prev_inserted_blob_datas,
                // Don't start looking for consecutive blobs at blob_index,
                // because we haven't inserted/committed the new blob_to_insert
                // into the database or prev_inserted_blob_datas hashmap yet.
                blob_index + 1,
                None,
            )?;

            // Add one because we skipped this current blob when calling
            // fetch_slot_continuous_blobs() earlier
            slot_meta.consumed + blob_datas.len() as u64 + 1
        } else {
            slot_meta.consumed
        }
    };

    let serialized_blob_data = &blob_to_insert.data[..BLOB_HEADER_SIZE + blob_size];

    // Commit step: commit all changes to the mutable structures at once, or none at all.
    // We don't want only some of these changes going through.
    write_batch.put_bytes::<cf::DataColumn>((blob_slot, blob_index), serialized_blob_data)?;
    prev_inserted_blob_datas.insert((blob_slot, blob_index), serialized_blob_data);
    // Index is zero-indexed, while the "received" height starts from 1,
    // so received = index + 1 for the same blob.
    slot_meta.received = cmp::max(blob_index + 1, slot_meta.received);
    slot_meta.consumed = new_consumed;
    slot_meta.last_index = {
        // If the last index in the slot hasn't been set before, then
        // set it to this blob index
        if slot_meta.last_index == std::u64::MAX {
            if blob_to_insert.is_last_in_slot() {
                blob_index
            } else {
                std::u64::MAX
            }
        } else {
            slot_meta.last_index
        }
    };
    Ok(())
}

/// Checks to see if the data blob passes integrity checks for insertion. Proceeds with
/// insertion if it does.
fn verify_new_data_blob<'a>(
    blob: &'a Blob,
    db: &Database,
    slot_meta_working_set: &mut HashMap<u64, (Rc<RefCell<MetaInfoCol>>, Option<MetaInfoCol>)>,
    prev_inserted_blob_datas: &mut HashMap<(u64, u64), &'a [u8]>,
    write_batch: &mut WriteBatch,
) -> bool {
    let blob_slot = blob.slot();
    let parent_slot = blob.parent();
    let m_c_g = db.column::<cf::MetaInfoCol>();

    // Check if we've already inserted the slot metadata for this blob's slot
    let entry = slot_meta_working_set.entry(blob_slot).or_insert_with(|| {
        // Store a 2-tuple of the metadata (working copy, backup copy)
        if let Some(mut meta) = m_c_g
            .get(blob_slot)
            .expect("Expect database get to succeed")
        {
            let backup = Some(meta.clone());
            // If parent_slot == std::u64::MAX, then this is one of the orphans inserted
            // during the chaining process, see the function find_slot_meta_in_cached_state()
            // for details. Slots that are orphans are missing a parent_slot, so we should
            // fill in the parent now that we know it.
            if is_singleton(&meta) {
                meta.parent_slot = parent_slot;
            }

            (Rc::new(RefCell::new(meta)), backup)
        } else {
            (
                Rc::new(RefCell::new(MetaInfoCol::new(blob_slot, parent_slot))),
                None,
            )
        }
    });

    let slot_meta = &mut entry.0.borrow_mut();

    // This slot is full, skip the bogus blob
    // Check if this blob should be inserted
    if !shd_insrt_blob(&slot_meta, db, &prev_inserted_blob_datas, blob) {
        false
    } else {
        let _ = put_blob_into_ledger(blob, db, prev_inserted_blob_datas, slot_meta, write_batch);
        true
    }
}

fn shd_insrt_blob(
    slot: &MetaInfoCol,
    db: &Database,
    prev_inserted_blob_datas: &HashMap<(u64, u64), &[u8]>,
    blob: &Blob,
) -> bool {
    let blob_index = blob.index();
    let blob_slot = blob.slot();
    let d_c_g = db.column::<cf::DataColumn>();

    // Check that the blob doesn't already exist
    if blob_index < slot.consumed
        || prev_inserted_blob_datas.contains_key(&(blob_slot, blob_index))
        || d_c_g
            .get_bytes((blob_slot, blob_index))
            .map(|opt| opt.is_some())
            .unwrap_or(false)
    {
        return false;
    }

    // Check that we do not receive blobs >= than the last_index
    // for the slot
    let last_index = slot.last_index;
    if blob_index >= last_index {
        datapoint_error!(
            "block_buffer_pool_error",
            (
                "error",
                format!(
                    "Received last blob with index {} >= slot.last_index {}",
                    blob_index, last_index
                ),
                String
            )
        );
        return false;
    }

    // Check that we do not receive a blob with "last_index" true, but index
    // less than our current received
    if blob.is_last_in_slot() && blob_index < slot.received {
        datapoint_error!(
            "block_buffer_pool_error",
            (
                "error",
                format!(
                    "Received last blob with index {} < slot.received {}",
                    blob_index, slot.received
                ),
                String
            )
        );
        return false;
    }

    true
}

// 1) Find the slot metadata in the cache of dirty slot metadata we've previously touched,
// else:
// 2) Search the database for that slot metadata. If still no luck, then:
// 3) Create a dummy orphan slot in the database
fn find_slot_meta_else_create<'a>(
    db: &Database,
    working_set: &'a HashMap<u64, (Rc<RefCell<MetaInfoCol>>, Option<MetaInfoCol>)>,
    chained_slots: &'a mut HashMap<u64, Rc<RefCell<MetaInfoCol>>>,
    slot_index: u64,
) -> Result<Rc<RefCell<MetaInfoCol>>> {
    let result = find_slot_meta_in_cached_state(working_set, chained_slots, slot_index)?;
    if let Some(slot) = result {
        Ok(slot)
    } else {
        find_slot_meta_in_db_else_create(db, slot_index, chained_slots)
    }
}

// Search the database for that slot metadata. If still no luck, then
// create a dummy orphan slot in the database
fn find_slot_meta_in_db_else_create<'a>(
    db: &Database,
    slot: u64,
    insert_map: &'a mut HashMap<u64, Rc<RefCell<MetaInfoCol>>>,
) -> Result<Rc<RefCell<MetaInfoCol>>> {
    if let Some(slot_meta) = db.column::<cf::MetaInfoCol>().get(slot)? {
        insert_map.insert(slot, Rc::new(RefCell::new(slot_meta)));
        Ok(insert_map.get(&slot).unwrap().clone())
    } else {
        // If this slot doesn't exist, make a orphan slot. This way we
        // remember which slots chained to this one when we eventually get a real blob
        // for this slot
        insert_map.insert(
            slot,
            Rc::new(RefCell::new(MetaInfoCol::new(slot, std::u64::MAX))),
        );
        Ok(insert_map.get(&slot).unwrap().clone())
    }
}

// Find the slot metadata in the cache of dirty slot metadata we've previously touched
fn find_slot_meta_in_cached_state<'a>(
    working_set: &'a HashMap<u64, (Rc<RefCell<MetaInfoCol>>, Option<MetaInfoCol>)>,
    chained_slots: &'a HashMap<u64, Rc<RefCell<MetaInfoCol>>>,
    slot: u64,
) -> Result<Option<Rc<RefCell<MetaInfoCol>>>> {
    if let Some((entry, _)) = working_set.get(&slot) {
        Ok(Some(entry.clone()))
    } else if let Some(entry) = chained_slots.get(&slot) {
        Ok(Some(entry.clone()))
    } else {
        Ok(None)
    }
}

/// Returns the next consumed index and the number of ticks in the new consumed
/// range
fn fetch_slot_continuous_blobs<'a>(
    slot: u64,
    db: &Database,
    prev_inserted_blob_datas: &HashMap<(u64, u64), &'a [u8]>,
    mut current_index: u64,
    max_blobs: Option<u64>,
) -> Result<Vec<Cow<'a, [u8]>>> {
    let mut blobs: Vec<Cow<[u8]>> = vec![];
    let d_c_g = db.column::<cf::DataColumn>();

    loop {
        if Some(blobs.len() as u64) == max_blobs {
            break;
        }
        // Try to find the next blob we're looking for in the prev_inserted_blob_datas
        if let Some(prev_blob_data) = prev_inserted_blob_datas.get(&(slot, current_index)) {
            blobs.push(Cow::Borrowed(*prev_blob_data));
        } else if let Some(blob_data) = d_c_g.get_bytes((slot, current_index))? {
            // Try to find the next blob we're looking for in the database
            blobs.push(Cow::Owned(blob_data));
        } else {
            break;
        }

        current_index += 1;
    }

    Ok(blobs)
}

// Chaining based on latest discussion here: https://github.com/morgan-labs/morgan/pull/2253
fn process_catenating(
    db: &Database,
    write_batch: &mut WriteBatch,
    working_set: &HashMap<u64, (Rc<RefCell<MetaInfoCol>>, Option<MetaInfoCol>)>,
) -> Result<()> {
    let mut new_chained_slots = HashMap::new();
    let working_set_slots: Vec<_> = working_set.iter().map(|s| *s.0).collect();
    for slot in working_set_slots {
        process_catenating_for_slot(db, write_batch, working_set, &mut new_chained_slots, slot)?;
    }

    // Write all the newly changed slots in new_chained_slots to the write_batch
    for (slot, meta) in new_chained_slots.iter() {
        let meta: &MetaInfoCol = &RefCell::borrow(&*meta);
        write_batch.put::<cf::MetaInfoCol>(*slot, meta)?;
    }
    Ok(())
}

fn process_catenating_for_slot(
    db: &Database,
    write_batch: &mut WriteBatch,
    working_set: &HashMap<u64, (Rc<RefCell<MetaInfoCol>>, Option<MetaInfoCol>)>,
    new_chained_slots: &mut HashMap<u64, Rc<RefCell<MetaInfoCol>>>,
    slot: u64,
) -> Result<()> {
    let (meta, meta_backup) = working_set
        .get(&slot)
        .expect("Slot must exist in the working_set hashmap");

    {
        let mut meta_mut = meta.borrow_mut();
        let was_orphan_slot = meta_backup.is_some() && is_singleton(meta_backup.as_ref().unwrap());

        // If:
        // 1) This is a new slot
        // 2) slot != 0
        // then try to chain this slot to a previous slot
        if slot != 0 {
            let prev_slot = meta_mut.parent_slot;

            // Check if the slot represented by meta_mut is either a new slot or a orphan.
            // In both cases we need to run the chaining logic b/c the parent on the slot was
            // previously unknown.
            if meta_backup.is_none() || was_orphan_slot {
                let prev_slot_meta =
                    find_slot_meta_else_create(db, working_set, new_chained_slots, prev_slot)?;

                // This is a newly inserted slot/orphan so run the chaining logic to link it to a
                // newly discovered parent
                catenate_new_slot_to_last_slot(&mut prev_slot_meta.borrow_mut(), slot, &mut meta_mut);

                // If the parent of `slot` is a newly inserted orphan, insert it into the orphans
                // column family
                if is_singleton(&RefCell::borrow(&*prev_slot_meta)) {
                    write_batch.put::<cf::SingletonColumn>(prev_slot, &true)?;
                }
            }
        }

        // At this point this slot has received a parent, so it's no longer an orphan
        if was_orphan_slot {
            write_batch.delete::<cf::SingletonColumn>(slot)?;
        }
    }

    // If this is a newly inserted slot, then we know the children of this slot were not previously
    // connected to the trunk of the ledger. Thus if slot.is_connected is now true, we need to
    // update all child slots with `is_connected` = true because these children are also now newly
    // connected to to trunk of the the ledger
    let should_propagate_is_connected =
        is_newly_completed_slot(&RefCell::borrow(&*meta), meta_backup)
            && RefCell::borrow(&*meta).is_connected;

    if should_propagate_is_connected {
        // slot_function returns a boolean indicating whether to explore the children
        // of the input slot
        let slot_function = |slot: &mut MetaInfoCol| {
            slot.is_connected = true;

            // We don't want to set the is_connected flag on the children of non-full
            // slots
            slot.is_full()
        };

        loop_through_child_mut(
            db,
            slot,
            &meta,
            working_set,
            new_chained_slots,
            slot_function,
        )?;
    }

    Ok(())
}

fn loop_through_child_mut<F>(
    db: &Database,
    slot: u64,
    slot_meta: &Rc<RefCell<(MetaInfoCol)>>,
    working_set: &HashMap<u64, (Rc<RefCell<MetaInfoCol>>, Option<MetaInfoCol>)>,
    new_chained_slots: &mut HashMap<u64, Rc<RefCell<MetaInfoCol>>>,
    slot_function: F,
) -> Result<()>
where
    F: Fn(&mut MetaInfoCol) -> bool,
{
    let mut next_slots: Vec<(u64, Rc<RefCell<(MetaInfoCol)>>)> = vec![(slot, slot_meta.clone())];
    while !next_slots.is_empty() {
        let (_, current_slot) = next_slots.pop().unwrap();
        // Check whether we should explore the children of this slot
        if slot_function(&mut current_slot.borrow_mut()) {
            let current_slot = &RefCell::borrow(&*current_slot);
            for next_slot_index in current_slot.next_slots.iter() {
                let next_slot = find_slot_meta_else_create(
                    db,
                    working_set,
                    new_chained_slots,
                    *next_slot_index,
                )?;
                next_slots.push((*next_slot_index, next_slot));
            }
        }
    }

    Ok(())
}

fn is_singleton(meta: &MetaInfoCol) -> bool {
    // If we have no parent, then this is the head of a detached chain of
    // slots
    !meta.is_parent_set()
}

// 1) Chain current_slot to the previous slot defined by prev_slot_meta
// 2) Determine whether to set the is_connected flag
fn catenate_new_slot_to_last_slot(
    prev_slot_meta: &mut MetaInfoCol,
    current_slot: u64,
    current_slot_meta: &mut MetaInfoCol,
) {
    prev_slot_meta.next_slots.push(current_slot);
    current_slot_meta.is_connected = prev_slot_meta.is_connected && prev_slot_meta.is_full();
}

fn is_newly_completed_slot(slot_meta: &MetaInfoCol, backup_slot_meta: &Option<MetaInfoCol>) -> bool {
    slot_meta.is_full()
        && (backup_slot_meta.is_none()
            || slot_meta.consumed != backup_slot_meta.as_ref().unwrap().consumed)
}

/// Attempts recovery using erasure coding
fn attempt_erasure_restore(
    db: &Database,
    session: &Session,
    erasure_meta: &ErasureMetaColumn,
    slot: u64,
    prev_inserted_blob_datas: &HashMap<(u64, u64), &[u8]>,
    new_coding_blob: Option<(u64, &[u8])>,
) -> Result<Option<(Vec<Blob>, Vec<Blob>)>> {
    use crate::expunge::ERASURE_SET_SIZE;

    let set_index = erasure_meta.set_index;
    let start_index = erasure_meta.start_index();
    let (data_end_index, _) = erasure_meta.end_indexes();

    let submit_metrics = |attempted: bool, status: String| {
        datapoint_info!(
            "block_buffer_pool-erasure",
            ("slot", slot as i64, i64),
            ("start_index", start_index as i64, i64),
            ("end_index", data_end_index as i64, i64),
            ("recovery_attempted", attempted, bool),
            ("recovery_status", status, String),
        );
    };

    let blobs = match erasure_meta.status() {
        ErasureMetaStatus::CanRecover => {
            let erasure_result = restore(
                db,
                session,
                slot,
                erasure_meta,
                prev_inserted_blob_datas,
                new_coding_blob,
            );

            match erasure_result {
                Ok((data, coding)) => {
                    let recovered = data.len() + coding.len();

                    assert_eq!(
                        ERASURE_SET_SIZE,
                        recovered + (erasure_meta.num_coding() + erasure_meta.num_data()) as usize,
                        "Recovery should always complete a set"
                    );

                    submit_metrics(true, "complete".into());

                    debug!(
                        "[try_erasure] slot: {}, set_index: {}, recovered {} blobs",
                        slot, set_index, recovered
                    );

                    Some((data, coding))
                }
                Err(Error::ErasureError(e)) => {
                    submit_metrics(true, format!("error: {}", e));

                    // error!(
                    //     "{}",
                    //     Error(format!("[try_erasure] slot: {}, set_index: {}, recovery failed: cause: {}",
                    //     slot, erasure_meta.set_index, e).to_string())
                    // );
                    println!(
                        "{}",
                        Error(
                            format!("[try_erasure] slot: {}, set_index: {}, recovery failed: cause: {}",
                                slot, erasure_meta.set_index, e).to_string(),
                            module_path!().to_string()
                        )
                    );
                    None
                }

                Err(e) => return Err(e),
            }
        }
        ErasureMetaStatus::StillNeed(needed) => {
            submit_metrics(false, format!("still need: {}", needed));

            debug!(
                "[try_erasure] slot: {}, set_index: {}, still need {} blobs",
                slot, set_index, needed
            );

            None
        }
        ErasureMetaStatus::DataFull => {
            submit_metrics(false, "complete".into());

            debug!(
                "[try_erasure] slot: {}, set_index: {}, set full",
                slot, set_index,
            );

            None
        }
    };

    Ok(blobs)
}

fn restore(
    db: &Database,
    session: &Session,
    slot: u64,
    erasure_meta: &ErasureMetaColumn,
    prev_inserted_blob_datas: &HashMap<(u64, u64), &[u8]>,
    new_coding: Option<(u64, &[u8])>,
) -> Result<(Vec<Blob>, Vec<Blob>)> {
    use crate::expunge::ERASURE_SET_SIZE;

    let start_idx = erasure_meta.start_index();
    let size = erasure_meta.size();
    let d_c_g = db.column::<cf::DataColumn>();
    let e_c_g = db.column::<cf::ErasureColumn>();

    let (data_end_idx, coding_end_idx) = erasure_meta.end_indexes();

    let present = &mut [true; ERASURE_SET_SIZE];
    let mut blobs = Vec::with_capacity(ERASURE_SET_SIZE);

    for i in start_idx..coding_end_idx {
        if erasure_meta.is_coding_present(i) {
            let mut blob_bytes = match new_coding {
                Some((new_coding_index, bytes)) if new_coding_index == i => bytes.to_vec(),
                _ => e_c_g
                    .get_bytes((slot, i))?
                    .expect("ErasureMetaColumn must have no false positives"),
            };

            blob_bytes.drain(..BLOB_HEADER_SIZE);

            blobs.push(blob_bytes);
        } else {
            let set_relative_idx = erasure_meta.coding_index_in_set(i).unwrap() as usize;
            blobs.push(vec![0; size]);
            present[set_relative_idx] = false;
        }
    }

    assert_ne!(size, 0);

    for i in start_idx..data_end_idx {
        let set_relative_idx = erasure_meta.data_index_in_set(i).unwrap() as usize;

        if erasure_meta.is_data_present(i) {
            let mut blob_bytes = match prev_inserted_blob_datas.get(&(slot, i)) {
                Some(bytes) => bytes.to_vec(),
                None => d_c_g
                    .get_bytes((slot, i))?
                    .expect("erasure_meta must have no false positives"),
            };

            // If data is too short, extend it with zeroes
            blob_bytes.resize(size, 0u8);

            blobs.insert(set_relative_idx, blob_bytes);
        } else {
            blobs.insert(set_relative_idx, vec![0u8; size]);
            // data erasures must come before any coding erasures if present
            present[set_relative_idx] = false;
        }
    }

    let (restored_data, recovered_coding) =
        session.reconstruct_blobs(&mut blobs, present, size, start_idx, slot)?;

    trace!(
        "[restore] reconstruction OK slot: {}, indexes: [{},{})",
        slot,
        start_idx,
        data_end_idx
    );

    Ok((restored_data, recovered_coding))
}

fn deserialize_blobs<I>(blob_datas: &[I]) -> Vec<Entry>
where
    I: Borrow<[u8]>,
{
    blob_datas
        .iter()
        .flat_map(|blob_data| {
            let serialized_entries_data = &blob_data.borrow()[BLOB_HEADER_SIZE..];
            BlockBufferPool::fetch_entry_from_deserialized_blob(serialized_entries_data)
                .expect("Ledger should only contain well formed data")
        })
        .collect()
}

fn renewal_on_slot(slot_meta: &MetaInfoCol, slot_meta_backup: &Option<MetaInfoCol>) -> bool {
    // We should signal that there are updates if we extended the chain of consecutive blocks starting
    // from block 0, which is true iff:
    // 1) The block with index prev_block_index is itself part of the trunk of consecutive blocks
    // starting from block 0,
    slot_meta.is_connected &&
        // AND either:
        // 1) The slot didn't exist in the database before, and now we have a consecutive
        // block for that slot
        ((slot_meta_backup.is_none() && slot_meta.consumed != 0) ||
        // OR
        // 2) The slot did exist, but now we have a new consecutive block for that slot
        (slot_meta_backup.is_some() && slot_meta_backup.as_ref().unwrap().consumed != slot_meta.consumed))
}

// Creates a new ledger with slot 0 full of ticks (and only ticks).
//
// Returns the transaction_seal that can be used to append entries with.
pub fn make_new_ledger_file(ledger_path: &str, genesis_block: &GenesisBlock) -> Result<Hash> {
    let ticks_per_slot = genesis_block.ticks_per_slot;
    BlockBufferPool::remove_ledger_file(ledger_path)?;
    genesis_block.write(&ledger_path)?;

    // Fill slot 0 with ticks that link back to the genesis_block to bootstrap the ledger.
    let block_buffer_pool = BlockBufferPool::open_ledger_file(ledger_path)?;
    let entries = crate::entry_info::create_ticks(ticks_per_slot, genesis_block.hash());
    block_buffer_pool.update_entries(0, 0, 0, ticks_per_slot, &entries)?;

    Ok(entries.last().unwrap().hash)
}

pub fn source<'a, I>(ledger_path: &str, keypair: &Keypair, entries: I) -> Result<()>
where
    I: IntoIterator<Item = &'a Entry>,
{
    let block_buffer_pool = BlockBufferPool::open_ledger_file(ledger_path)?;

    // TODO sign these blobs with keypair
    let blobs: Vec<_> = entries
        .into_iter()
        .enumerate()
        .map(|(idx, entry)| {
            let mut b = entry.borrow().to_blob();
            b.set_index(idx as u64);
            b.set_id(&keypair.pubkey());
            b.set_slot(0);
            b
        })
        .collect();

    block_buffer_pool.update_genesis_blobs(&blobs[..])?;
    Ok(())
}

#[macro_export]
macro_rules! tmp_ledger_name {
    () => {
        &format!("{}-{}", file!(), line!())
    };
}

#[macro_export]
macro_rules! get_tmp_ledger_path {
    () => {
        fetch_interim_ledger_location(tmp_ledger_name!())
    };
}

pub fn fetch_interim_ledger_location(name: &str) -> String {
    use std::env;
    let out_dir = env::var("OUT_DIR").unwrap_or_else(|_| "target".to_string());
    let keypair = Keypair::new();

    let path = format!("{}/tmp/ledger/{}-{}", out_dir, name, keypair.pubkey());

    // whack any possible collision
    let _ignored = fs::remove_dir_all(&path);

    path
}

#[macro_export]
macro_rules! create_new_tmp_ledger {
    ($genesis_block:expr) => {
        create_new_tmp_ledger(tmp_ledger_name!(), $genesis_block)
    };
}

// Same as `make_new_ledger_file()` but use a temporary ledger name based on the provided `name`
//
// Note: like `make_new_ledger_file` the returned ledger will have slot 0 full of ticks (and only
// ticks)
pub fn create_new_tmp_ledger(name: &str, genesis_block: &GenesisBlock) -> (String, Hash) {
    let ledger_path = fetch_interim_ledger_location(name);
    let transaction_seal = make_new_ledger_file(&ledger_path, genesis_block).unwrap();
    (ledger_path, transaction_seal)
}

#[macro_export]
macro_rules! tmp_copy_block_buffer {
    ($from:expr) => {
        tmp_copy_block_buffer($from, tmp_ledger_name!())
    };
}

pub fn tmp_copy_block_buffer(from: &str, name: &str) -> String {
    let path = fetch_interim_ledger_location(name);

    let block_buffer_pool = BlockBufferPool::open_ledger_file(from).unwrap();
    let blobs = block_buffer_pool.fetch_iterator_blobs();
    let genesis_block = GenesisBlock::load(from).unwrap();

    BlockBufferPool::remove_ledger_file(&path).expect("Expected successful database destruction");
    let block_buffer_pool = BlockBufferPool::open_ledger_file(&path).unwrap();
    block_buffer_pool.update_blobs(blobs).unwrap();
    genesis_block.write(&path).unwrap();

    path
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::entry_info::{
        create_ticks, make_tiny_test_entries, make_tiny_test_entries_from_hash, Entry, EntrySlice,
    };
    use crate::expunge::{CodingGenerator, NUM_CODING, NUM_DATA};
    use crate::packet;
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use rand::Rng;
    use morgan_interface::hash::Hash;
    use morgan_interface::pubkey::Pubkey;
    use std::cmp::min;
    use std::collections::HashSet;
    use std::iter::once;
    use std::iter::FromIterator;
    use std::time::Duration;

    #[test]
    fn test_write_entries() {
        morgan_logger::setup();
        let ledger_path = get_tmp_ledger_path!();
        {
            let ticks_per_slot = 10;
            let num_slots = 10;
            let num_ticks = ticks_per_slot * num_slots;
            let ledger = BlockBufferPool::open_ledger_file(&ledger_path).unwrap();

            let ticks = create_ticks(num_ticks, Hash::default());
            ledger
                .update_entries(0, 0, 0, ticks_per_slot, ticks.clone())
                .unwrap();

            for i in 0..num_slots {
                let meta = ledger.meta(i).unwrap().unwrap();
                assert_eq!(meta.consumed, ticks_per_slot);
                assert_eq!(meta.received, ticks_per_slot);
                assert_eq!(meta.last_index, ticks_per_slot - 1);
                if i == num_slots - 1 {
                    assert!(meta.next_slots.is_empty());
                } else {
                    assert_eq!(meta.next_slots, vec![i + 1]);
                }
                if i == 0 {
                    assert_eq!(meta.parent_slot, 0);
                } else {
                    assert_eq!(meta.parent_slot, i - 1);
                }

                assert_eq!(
                    &ticks[(i * ticks_per_slot) as usize..((i + 1) * ticks_per_slot) as usize],
                    &ledger.fetch_slot_entries(i, 0, None).unwrap()[..]
                );
            }

            // Simulate writing to the end of a slot with existing ticks
            ledger
                .update_entries(
                    num_slots,
                    ticks_per_slot - 1,
                    ticks_per_slot - 2,
                    ticks_per_slot,
                    &ticks[0..2],
                )
                .unwrap();

            let meta = ledger.meta(num_slots).unwrap().unwrap();
            assert_eq!(meta.consumed, 0);
            // received blob was ticks_per_slot - 2, so received should be ticks_per_slot - 2 + 1
            assert_eq!(meta.received, ticks_per_slot - 1);
            // last blob index ticks_per_slot - 2 because that's the blob that made tick_height == ticks_per_slot
            // for the slot
            assert_eq!(meta.last_index, ticks_per_slot - 2);
            assert_eq!(meta.parent_slot, num_slots - 1);
            assert_eq!(meta.next_slots, vec![num_slots + 1]);
            assert_eq!(
                &ticks[0..1],
                &ledger
                    .fetch_slot_entries(num_slots, ticks_per_slot - 2, None)
                    .unwrap()[..]
            );

            // We wrote two entries, the second should spill into slot num_slots + 1
            let meta = ledger.meta(num_slots + 1).unwrap().unwrap();
            assert_eq!(meta.consumed, 1);
            assert_eq!(meta.received, 1);
            assert_eq!(meta.last_index, std::u64::MAX);
            assert_eq!(meta.parent_slot, num_slots);
            assert!(meta.next_slots.is_empty());

            assert_eq!(
                &ticks[1..2],
                &ledger.fetch_slot_entries(num_slots + 1, 0, None).unwrap()[..]
            );
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_put_get_simple() {
        let ledger_path = fetch_interim_ledger_location("test_put_get_simple");
        let ledger = BlockBufferPool::open_ledger_file(&ledger_path).unwrap();

        // Test meta column family
        let meta = MetaInfoCol::new(0, 1);
        ledger.m_c_g.put(0, &meta).unwrap();
        let result = ledger
            .m_c_g
            .get(0)
            .unwrap()
            .expect("Expected meta object to exist");

        assert_eq!(result, meta);

        // Test erasure column family
        let erasure = vec![1u8; 16];
        let erasure_key = (0, 0);
        ledger.e_c_g.put_bytes(erasure_key, &erasure).unwrap();

        let result = ledger
            .e_c_g
            .get_bytes(erasure_key)
            .unwrap()
            .expect("Expected erasure object to exist");

        assert_eq!(result, erasure);

        // Test data column family
        let data = vec![2u8; 16];
        let data_key = (0, 0);
        ledger.d_c_g.put_bytes(data_key, &data).unwrap();

        let result = ledger
            .d_c_g
            .get_bytes(data_key)
            .unwrap()
            .expect("Expected data object to exist");

        assert_eq!(result, data);

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_read_blobs_bytes() {
        let shared_blobs = make_tiny_test_entries(10).to_single_entry_shared_blobs();
        let slot = 0;
        packet::index_blobs(&shared_blobs, &Pubkey::new_rand(), 0, slot, 0);

        let blob_locks: Vec<_> = shared_blobs.iter().map(|b| b.read().unwrap()).collect();
        let blobs: Vec<&Blob> = blob_locks.iter().map(|b| &**b).collect();

        let ledger_path = fetch_interim_ledger_location("test_read_blobs_bytes");
        let ledger = BlockBufferPool::open_ledger_file(&ledger_path).unwrap();
        ledger.update_blobs(blobs.clone()).unwrap();

        let mut buf = [0; 1024];
        let (num_blobs, bytes) = ledger.read_db_by_bytes(0, 1, &mut buf, slot).unwrap();
        let bytes = bytes as usize;
        assert_eq!(num_blobs, 1);
        {
            let blob_data = &buf[..bytes];
            assert_eq!(blob_data, &blobs[0].data[..bytes]);
        }

        let (num_blobs, bytes2) = ledger.read_db_by_bytes(0, 2, &mut buf, slot).unwrap();
        let bytes2 = bytes2 as usize;
        assert_eq!(num_blobs, 2);
        assert!(bytes2 > bytes);
        {
            let blob_data_1 = &buf[..bytes];
            assert_eq!(blob_data_1, &blobs[0].data[..bytes]);

            let blob_data_2 = &buf[bytes..bytes2];
            assert_eq!(blob_data_2, &blobs[1].data[..bytes2 - bytes]);
        }

        // buf size part-way into blob[1], should just return blob[0]
        let mut buf = vec![0; bytes + 1];
        let (num_blobs, bytes3) = ledger.read_db_by_bytes(0, 2, &mut buf, slot).unwrap();
        assert_eq!(num_blobs, 1);
        let bytes3 = bytes3 as usize;
        assert_eq!(bytes3, bytes);

        let mut buf = vec![0; bytes2 - 1];
        let (num_blobs, bytes4) = ledger.read_db_by_bytes(0, 2, &mut buf, slot).unwrap();
        assert_eq!(num_blobs, 1);
        let bytes4 = bytes4 as usize;
        assert_eq!(bytes4, bytes);

        let mut buf = vec![0; bytes * 2];
        let (num_blobs, bytes6) = ledger.read_db_by_bytes(9, 1, &mut buf, slot).unwrap();
        assert_eq!(num_blobs, 1);
        let bytes6 = bytes6 as usize;

        {
            let blob_data = &buf[..bytes6];
            assert_eq!(blob_data, &blobs[9].data[..bytes6]);
        }

        // Read out of range
        assert!(ledger.read_db_by_bytes(20, 2, &mut buf, slot).is_err());

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_insert_data_blobs_basic() {
        let num_entries = 5;
        assert!(num_entries > 1);

        let (blobs, entries) = make_slot_entries(0, 0, num_entries);

        let ledger_path = fetch_interim_ledger_location("test_insert_data_blobs_basic");
        let ledger = BlockBufferPool::open_ledger_file(&ledger_path).unwrap();

        // Insert last blob, we're missing the other blobs, so no consecutive
        // blobs starting from slot 0, index 0 should exist.
        ledger
            .insert_data_blobs(once(&blobs[num_entries as usize - 1]))
            .unwrap();
        assert!(ledger.fetch_slot_entries(0, 0, None).unwrap().is_empty());

        let meta = ledger
            .meta(0)
            .unwrap()
            .expect("Expected new metadata object to be created");
        assert!(meta.consumed == 0 && meta.received == num_entries);

        // Insert the other blobs, check for consecutive returned entries
        ledger
            .insert_data_blobs(&blobs[0..(num_entries - 1) as usize])
            .unwrap();
        let result = ledger.fetch_slot_entries(0, 0, None).unwrap();

        assert_eq!(result, entries);

        let meta = ledger
            .meta(0)
            .unwrap()
            .expect("Expected new metadata object to exist");
        assert_eq!(meta.consumed, num_entries);
        assert_eq!(meta.received, num_entries);
        assert_eq!(meta.parent_slot, 0);
        assert_eq!(meta.last_index, num_entries - 1);
        assert!(meta.next_slots.is_empty());
        assert!(meta.is_connected);

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_insert_data_blobs_reverse() {
        let num_entries = 10;
        let (blobs, entries) = make_slot_entries(0, 0, num_entries);

        let ledger_path = fetch_interim_ledger_location("test_insert_data_blobs_reverse");
        let ledger = BlockBufferPool::open_ledger_file(&ledger_path).unwrap();

        // Insert blobs in reverse, check for consecutive returned blobs
        for i in (0..num_entries).rev() {
            ledger.insert_data_blobs(once(&blobs[i as usize])).unwrap();
            let result = ledger.fetch_slot_entries(0, 0, None).unwrap();

            let meta = ledger
                .meta(0)
                .unwrap()
                .expect("Expected metadata object to exist");
            assert_eq!(meta.parent_slot, 0);
            assert_eq!(meta.last_index, num_entries - 1);
            if i != 0 {
                assert_eq!(result.len(), 0);
                assert!(meta.consumed == 0 && meta.received == num_entries as u64);
            } else {
                assert_eq!(result, entries);
                assert!(meta.consumed == num_entries as u64 && meta.received == num_entries as u64);
            }
        }

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_insert_slots() {
        test_insert_data_blobs_slots("test_insert_data_blobs_slots_single", false);
        test_insert_data_blobs_slots("test_insert_data_blobs_slots_bulk", true);
    }

    #[test]
    pub fn test_iteration_order() {
        let slot = 0;
        let block_buffer_pool_path = fetch_interim_ledger_location("test_iteration_order");
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

            // Write entries
            let num_entries = 8;
            let entries = make_tiny_test_entries(num_entries);
            let mut blobs = entries.to_single_entry_blobs();

            for (i, b) in blobs.iter_mut().enumerate() {
                b.set_index(1 << (i * 8));
                b.set_slot(0);
            }

            block_buffer_pool
                .update_blobs(&blobs)
                .expect("Expected successful write of blobs");

            let mut db_iterator = block_buffer_pool
                .db
                .cursor::<cf::DataColumn>()
                .expect("Expected to be able to open database iterator");

            db_iterator.seek((slot, 1));

            // Iterate through ledger
            for i in 0..num_entries {
                assert!(db_iterator.valid());
                let (_, current_index) = db_iterator.key().expect("Expected a valid key");
                assert_eq!(current_index, (1 as u64) << (i * 8));
                db_iterator.next();
            }
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_get_slot_entries1() {
        let block_buffer_pool_path = fetch_interim_ledger_location("test_get_slot_entries1");
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();
            let entries = make_tiny_test_entries(8);
            let mut blobs = entries.clone().to_single_entry_blobs();
            for (i, b) in blobs.iter_mut().enumerate() {
                b.set_slot(1);
                if i < 4 {
                    b.set_index(i as u64);
                } else {
                    b.set_index(8 + i as u64);
                }
            }
            block_buffer_pool
                .update_blobs(&blobs)
                .expect("Expected successful write of blobs");

            assert_eq!(
                block_buffer_pool.fetch_slot_entries(1, 2, None).unwrap()[..],
                entries[2..4],
            );

            assert_eq!(
                block_buffer_pool.fetch_slot_entries(1, 12, None).unwrap()[..],
                entries[4..],
            );
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_get_slot_entries2() {
        let block_buffer_pool_path = fetch_interim_ledger_location("test_get_slot_entries2");
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

            // Write entries
            let num_slots = 5 as u64;
            let mut index = 0;
            for slot in 0..num_slots {
                let entries = make_tiny_test_entries(slot as usize + 1);
                let last_entry = entries.last().unwrap().clone();
                let mut blobs = entries.clone().to_single_entry_blobs();
                for b in blobs.iter_mut() {
                    b.set_index(index);
                    b.set_slot(slot as u64);
                    index += 1;
                }
                block_buffer_pool
                    .update_blobs(&blobs)
                    .expect("Expected successful write of blobs");
                assert_eq!(
                    block_buffer_pool.fetch_slot_entries(slot, index - 1, None).unwrap(),
                    vec![last_entry],
                );
            }
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_get_slot_entries3() {
        // Test inserting/fetching blobs which contain multiple entries per blob
        let block_buffer_pool_path = fetch_interim_ledger_location("test_get_slot_entries3");
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();
            let num_slots = 5 as u64;
            let blobs_per_slot = 5 as u64;
            let entry_serialized_size =
                bincode::serialized_size(&make_tiny_test_entries(1)).unwrap();
            let entries_per_slot =
                (blobs_per_slot * packet::BLOB_DATA_SIZE as u64) / entry_serialized_size;

            // Write entries
            for slot in 0..num_slots {
                let mut index = 0;
                let entries = make_tiny_test_entries(entries_per_slot as usize);
                let mut blobs = entries.clone().to_blobs();
                assert_eq!(blobs.len() as u64, blobs_per_slot);
                for b in blobs.iter_mut() {
                    b.set_index(index);
                    b.set_slot(slot as u64);
                    index += 1;
                }
                block_buffer_pool
                    .update_blobs(&blobs)
                    .expect("Expected successful write of blobs");
                assert_eq!(block_buffer_pool.fetch_slot_entries(slot, 0, None).unwrap(), entries,);
            }
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_insert_data_blobs_consecutive() {
        let block_buffer_pool_path = fetch_interim_ledger_location("test_insert_data_blobs_consecutive");
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();
            for i in 0..4 {
                let slot = i;
                let parent_slot = if i == 0 { 0 } else { i - 1 };
                // Write entries
                let num_entries = 21 as u64 * (i + 1);
                let (blobs, original_entries) = make_slot_entries(slot, parent_slot, num_entries);

                block_buffer_pool
                    .update_blobs(blobs.iter().skip(1).step_by(2))
                    .unwrap();

                assert_eq!(block_buffer_pool.fetch_slot_entries(slot, 0, None).unwrap(), vec![]);

                let meta = block_buffer_pool.meta(slot).unwrap().unwrap();
                if num_entries % 2 == 0 {
                    assert_eq!(meta.received, num_entries);
                } else {
                    debug!("got here");
                    assert_eq!(meta.received, num_entries - 1);
                }
                assert_eq!(meta.consumed, 0);
                assert_eq!(meta.parent_slot, parent_slot);
                if num_entries % 2 == 0 {
                    assert_eq!(meta.last_index, num_entries - 1);
                } else {
                    assert_eq!(meta.last_index, std::u64::MAX);
                }

                block_buffer_pool.update_blobs(blobs.iter().step_by(2)).unwrap();

                assert_eq!(
                    block_buffer_pool.fetch_slot_entries(slot, 0, None).unwrap(),
                    original_entries,
                );

                let meta = block_buffer_pool.meta(slot).unwrap().unwrap();
                assert_eq!(meta.received, num_entries);
                assert_eq!(meta.consumed, num_entries);
                assert_eq!(meta.parent_slot, parent_slot);
                assert_eq!(meta.last_index, num_entries - 1);
            }
        }

        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_insert_data_blobs_duplicate() {
        // Create RocksDb ledger
        let block_buffer_pool_path = fetch_interim_ledger_location("test_insert_data_blobs_duplicate");
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

            // Make duplicate entries and blobs
            let num_duplicates = 2;
            let num_unique_entries = 10;
            let (original_entries, blobs) = {
                let (blobs, entries) = make_slot_entries(0, 0, num_unique_entries);
                let entries: Vec<_> = entries
                    .into_iter()
                    .flat_map(|e| vec![e.clone(), e])
                    .collect();
                let blobs: Vec<_> = blobs.into_iter().flat_map(|b| vec![b.clone(), b]).collect();
                (entries, blobs)
            };

            block_buffer_pool
                .update_blobs(
                    blobs
                        .iter()
                        .skip(num_duplicates as usize)
                        .step_by(num_duplicates as usize * 2),
                )
                .unwrap();

            assert_eq!(block_buffer_pool.fetch_slot_entries(0, 0, None).unwrap(), vec![]);

            block_buffer_pool
                .update_blobs(blobs.iter().step_by(num_duplicates as usize * 2))
                .unwrap();

            let expected: Vec<_> = original_entries
                .into_iter()
                .step_by(num_duplicates as usize)
                .collect();

            assert_eq!(block_buffer_pool.fetch_slot_entries(0, 0, None).unwrap(), expected,);

            let meta = block_buffer_pool.meta(0).unwrap().unwrap();
            assert_eq!(meta.consumed, num_unique_entries);
            assert_eq!(meta.received, num_unique_entries);
            assert_eq!(meta.parent_slot, 0);
            assert_eq!(meta.last_index, num_unique_entries - 1);
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_genesis_and_entry_iterator() {
        let entries = make_tiny_test_entries_from_hash(&Hash::default(), 10);

        let ledger_path = fetch_interim_ledger_location("test_genesis_and_entry_iterator");
        {
            source(&ledger_path, &Keypair::new(), &entries).unwrap();

            let ledger = BlockBufferPool::open_ledger_file(&ledger_path).expect("open failed");

            let read_entries: Vec<Entry> =
                ledger.fetch_iterator().expect("fetch_iterator failed").collect();
            assert!(read_entries.verify(&Hash::default()));
            assert_eq!(entries, read_entries);
        }

        BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
    }
    #[test]
    pub fn test_entry_iterator_up_to_consumed() {
        let entries = make_tiny_test_entries_from_hash(&Hash::default(), 3);
        let ledger_path = fetch_interim_ledger_location("test_genesis_and_entry_iterator");
        {
            // put entries except last 2 into ledger
            source(&ledger_path, &Keypair::new(), &entries[..entries.len() - 2]).unwrap();

            let ledger = BlockBufferPool::open_ledger_file(&ledger_path).expect("open failed");

            // now write the last entry, ledger has a hole in it one before the end
            // +-+-+-+-+-+-+-+    +-+
            // | | | | | | | |    | |
            // +-+-+-+-+-+-+-+    +-+
            ledger
                .update_entries(
                    0u64,
                    0,
                    (entries.len() - 1) as u64,
                    16,
                    &entries[entries.len() - 1..],
                )
                .unwrap();

            let read_entries: Vec<Entry> =
                ledger.fetch_iterator().expect("fetch_iterator failed").collect();
            assert!(read_entries.verify(&Hash::default()));

            // enumeration should stop at the hole
            assert_eq!(entries[..entries.len() - 2].to_vec(), read_entries);
        }

        BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_new_blobs_signal() {
        // Initialize ledger
        let ledger_path = fetch_interim_ledger_location("test_new_blobs_signal");
        let (ledger, recvr, _) = BlockBufferPool::open_by_message(&ledger_path).unwrap();
        let ledger = Arc::new(ledger);

        let entries_per_slot = 10;
        // Create entries for slot 0
        let (blobs, _) = make_slot_entries(0, 0, entries_per_slot);

        // Insert second blob, but we're missing the first blob, so no consecutive
        // blobs starting from slot 0, index 0 should exist.
        ledger.insert_data_blobs(once(&blobs[1])).unwrap();
        let timer = Duration::new(1, 0);
        assert!(recvr.recv_timeout(timer).is_err());
        // Insert first blob, now we've made a consecutive block
        ledger.insert_data_blobs(once(&blobs[0])).unwrap();
        // Wait to get notified of update, should only be one update
        assert!(recvr.recv_timeout(timer).is_ok());
        assert!(recvr.try_recv().is_err());
        // Insert the rest of the ticks
        ledger
            .insert_data_blobs(&blobs[1..entries_per_slot as usize])
            .unwrap();
        // Wait to get notified of update, should only be one update
        assert!(recvr.recv_timeout(timer).is_ok());
        assert!(recvr.try_recv().is_err());

        // Create some other slots, and send batches of ticks for each slot such that each slot
        // is missing the tick at blob index == slot index - 1. Thus, no consecutive blocks
        // will be formed
        let num_slots = entries_per_slot;
        let mut blobs: Vec<Blob> = vec![];
        let mut missing_blobs = vec![];
        for slot in 1..num_slots + 1 {
            let (mut slot_blobs, _) = make_slot_entries(slot, slot - 1, entries_per_slot);
            let missing_blob = slot_blobs.remove(slot as usize - 1);
            blobs.extend(slot_blobs);
            missing_blobs.push(missing_blob);
        }

        // Should be no updates, since no new chains from block 0 were formed
        ledger.insert_data_blobs(blobs.iter()).unwrap();
        assert!(recvr.recv_timeout(timer).is_err());

        // Insert a blob for each slot that doesn't make a consecutive block, we
        // should get no updates
        let blobs: Vec<_> = (1..num_slots + 1)
            .flat_map(|slot| {
                let (mut blob, _) = make_slot_entries(slot, slot - 1, 1);
                blob[0].set_index(2 * num_slots as u64);
                blob
            })
            .collect();

        ledger.insert_data_blobs(blobs.iter()).unwrap();
        assert!(recvr.recv_timeout(timer).is_err());

        // For slots 1..num_slots/2, fill in the holes in one batch insertion,
        // so we should only get one signal
        ledger
            .insert_data_blobs(&missing_blobs[..(num_slots / 2) as usize])
            .unwrap();
        assert!(recvr.recv_timeout(timer).is_ok());
        assert!(recvr.try_recv().is_err());

        // Fill in the holes for each of the remaining slots, we should get a single update
        // for each
        for missing_blob in &missing_blobs[(num_slots / 2) as usize..] {
            ledger
                .insert_data_blobs(vec![missing_blob.clone()])
                .unwrap();
        }

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_completed_blobs_signal() {
        // Initialize ledger
        let ledger_path = fetch_interim_ledger_location("test_completed_blobs_signal");
        let (ledger, _, recvr) = BlockBufferPool::open_by_message(&ledger_path).unwrap();
        let ledger = Arc::new(ledger);

        let entries_per_slot = 10;

        // Create blobs for slot 0
        let (blobs, _) = make_slot_entries(0, 0, entries_per_slot);

        // Insert all but the first blob in the slot, should not be considered complete
        ledger
            .insert_data_blobs(&blobs[1..entries_per_slot as usize])
            .unwrap();
        assert!(recvr.try_recv().is_err());

        // Insert first blob, slot should now be considered complete
        ledger.insert_data_blobs(once(&blobs[0])).unwrap();
        assert_eq!(recvr.try_recv().unwrap(), vec![0]);
    }

    #[test]
    pub fn test_completed_blobs_signal_orphans() {
        // Initialize ledger
        let ledger_path = fetch_interim_ledger_location("test_completed_blobs_signal_orphans");
        let (ledger, _, recvr) = BlockBufferPool::open_by_message(&ledger_path).unwrap();
        let ledger = Arc::new(ledger);

        let entries_per_slot = 10;
        let slots = vec![2, 5, 10];
        let all_blobs = make_chaining_slot_entries(&slots[..], entries_per_slot);

        // Get the blobs for slot 5 chaining to slot 2
        let (ref orphan_blobs, _) = all_blobs[1];

        // Get the blobs for slot 10, chaining to slot 5
        let (ref orphan_child, _) = all_blobs[2];

        // Insert all but the first blob in the slot, should not be considered complete
        ledger
            .insert_data_blobs(&orphan_child[1..entries_per_slot as usize])
            .unwrap();
        assert!(recvr.try_recv().is_err());

        // Insert first blob, slot should now be considered complete
        ledger.insert_data_blobs(once(&orphan_child[0])).unwrap();
        assert_eq!(recvr.try_recv().unwrap(), vec![slots[2]]);

        // Insert the blobs for the orphan_slot
        ledger
            .insert_data_blobs(&orphan_blobs[1..entries_per_slot as usize])
            .unwrap();
        assert!(recvr.try_recv().is_err());

        // Insert first blob, slot should now be considered complete
        ledger.insert_data_blobs(once(&orphan_blobs[0])).unwrap();
        assert_eq!(recvr.try_recv().unwrap(), vec![slots[1]]);
    }

    #[test]
    pub fn test_completed_blobs_signal_many() {
        // Initialize ledger
        let ledger_path = fetch_interim_ledger_location("test_completed_blobs_signal_many");
        let (ledger, _, recvr) = BlockBufferPool::open_by_message(&ledger_path).unwrap();
        let ledger = Arc::new(ledger);

        let entries_per_slot = 10;
        let mut slots = vec![2, 5, 10];
        let all_blobs = make_chaining_slot_entries(&slots[..], entries_per_slot);
        let disconnected_slot = 4;

        let (ref blobs0, _) = all_blobs[0];
        let (ref blobs1, _) = all_blobs[1];
        let (ref blobs2, _) = all_blobs[2];
        let (ref blobs3, _) = make_slot_entries(disconnected_slot, 1, entries_per_slot);

        let mut all_blobs: Vec<_> = vec![blobs0, blobs1, blobs2, blobs3]
            .into_iter()
            .flatten()
            .collect();

        all_blobs.shuffle(&mut thread_rng());
        ledger.insert_data_blobs(all_blobs).unwrap();
        let mut result = recvr.try_recv().unwrap();
        result.sort();
        slots.push(disconnected_slot);
        slots.sort();
        assert_eq!(result, slots);
    }

    #[test]
    pub fn test_handle_chaining_basic() {
        let block_buffer_pool_path = fetch_interim_ledger_location("test_handle_chaining_basic");
        {
            let entries_per_slot = 2;
            let num_slots = 3;
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

            // Construct the blobs
            let (blobs, _) = make_many_slot_entries(0, num_slots, entries_per_slot);

            // 1) Write to the first slot
            block_buffer_pool
                .update_blobs(&blobs[entries_per_slot as usize..2 * entries_per_slot as usize])
                .unwrap();
            let s1 = block_buffer_pool.meta(1).unwrap().unwrap();
            assert!(s1.next_slots.is_empty());
            // Slot 1 is not trunk because slot 0 hasn't been inserted yet
            assert!(!s1.is_connected);
            assert_eq!(s1.parent_slot, 0);
            assert_eq!(s1.last_index, entries_per_slot - 1);

            // 2) Write to the second slot
            block_buffer_pool
                .update_blobs(&blobs[2 * entries_per_slot as usize..3 * entries_per_slot as usize])
                .unwrap();
            let s2 = block_buffer_pool.meta(2).unwrap().unwrap();
            assert!(s2.next_slots.is_empty());
            // Slot 2 is not trunk because slot 0 hasn't been inserted yet
            assert!(!s2.is_connected);
            assert_eq!(s2.parent_slot, 1);
            assert_eq!(s2.last_index, entries_per_slot - 1);

            // Check the first slot again, it should chain to the second slot,
            // but still isn't part of the trunk
            let s1 = block_buffer_pool.meta(1).unwrap().unwrap();
            assert_eq!(s1.next_slots, vec![2]);
            assert!(!s1.is_connected);
            assert_eq!(s1.parent_slot, 0);
            assert_eq!(s1.last_index, entries_per_slot - 1);

            // 3) Write to the zeroth slot, check that every slot
            // is now part of the trunk
            block_buffer_pool
                .update_blobs(&blobs[0..entries_per_slot as usize])
                .unwrap();
            for i in 0..3 {
                let s = block_buffer_pool.meta(i).unwrap().unwrap();
                // The last slot will not chain to any other slots
                if i != 2 {
                    assert_eq!(s.next_slots, vec![i + 1]);
                }
                if i == 0 {
                    assert_eq!(s.parent_slot, 0);
                } else {
                    assert_eq!(s.parent_slot, i - 1);
                }
                assert_eq!(s.last_index, entries_per_slot - 1);
                assert!(s.is_connected);
            }
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_handle_chaining_missing_slots() {
        let block_buffer_pool_path = fetch_interim_ledger_location("test_handle_chaining_missing_slots");
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();
            let num_slots = 30;
            let entries_per_slot = 2;

            // Separate every other slot into two separate vectors
            let mut slots = vec![];
            let mut missing_slots = vec![];
            for slot in 0..num_slots {
                let parent_slot = {
                    if slot == 0 {
                        0
                    } else {
                        slot - 1
                    }
                };
                let (slot_blobs, _) = make_slot_entries(slot, parent_slot, entries_per_slot);

                if slot % 2 == 1 {
                    slots.extend(slot_blobs);
                } else {
                    missing_slots.extend(slot_blobs);
                }
            }

            // Write the blobs for every other slot
            block_buffer_pool.update_blobs(&slots).unwrap();

            // Check metadata
            for i in 0..num_slots {
                // If "i" is the index of a slot we just inserted, then next_slots should be empty
                // for slot "i" because no slots chain to that slot, because slot i + 1 is missing.
                // However, if it's a slot we haven't inserted, aka one of the gaps, then one of the
                // slots we just inserted will chain to that gap, so next_slots for that orphan slot
                // won't be empty, but the parent slot is unknown so should equal std::u64::MAX.
                let s = block_buffer_pool.meta(i as u64).unwrap().unwrap();
                if i % 2 == 0 {
                    assert_eq!(s.next_slots, vec![i as u64 + 1]);
                    assert_eq!(s.parent_slot, std::u64::MAX);
                } else {
                    assert!(s.next_slots.is_empty());
                    assert_eq!(s.parent_slot, i - 1);
                }

                if i == 0 {
                    assert!(s.is_connected);
                } else {
                    assert!(!s.is_connected);
                }
            }

            // Write the blobs for the other half of the slots that we didn't insert earlier
            block_buffer_pool.update_blobs(&missing_slots[..]).unwrap();

            for i in 0..num_slots {
                // Check that all the slots chain correctly once the missing slots
                // have been filled
                let s = block_buffer_pool.meta(i as u64).unwrap().unwrap();
                if i != num_slots - 1 {
                    assert_eq!(s.next_slots, vec![i as u64 + 1]);
                } else {
                    assert!(s.next_slots.is_empty());
                }

                if i == 0 {
                    assert_eq!(s.parent_slot, 0);
                } else {
                    assert_eq!(s.parent_slot, i - 1);
                }
                assert_eq!(s.last_index, entries_per_slot - 1);
                assert!(s.is_connected);
            }
        }

        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_forward_chaining_is_connected() {
        let block_buffer_pool_path = fetch_interim_ledger_location("test_forward_chaining_is_connected");
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();
            let num_slots = 15;
            let entries_per_slot = 2;
            assert!(entries_per_slot > 1);

            let (blobs, _) = make_many_slot_entries(0, num_slots, entries_per_slot);

            // Write the blobs such that every 3rd slot has a gap in the beginning
            for (slot, slot_ticks) in blobs.chunks(entries_per_slot as usize).enumerate() {
                if slot % 3 == 0 {
                    block_buffer_pool
                        .update_blobs(&slot_ticks[1..entries_per_slot as usize])
                        .unwrap();
                } else {
                    block_buffer_pool
                        .update_blobs(&slot_ticks[..entries_per_slot as usize])
                        .unwrap();
                }
            }

            // Check metadata
            for i in 0..num_slots {
                let s = block_buffer_pool.meta(i as u64).unwrap().unwrap();
                // The last slot will not chain to any other slots
                if i as u64 != num_slots - 1 {
                    assert_eq!(s.next_slots, vec![i as u64 + 1]);
                } else {
                    assert!(s.next_slots.is_empty());
                }

                if i == 0 {
                    assert_eq!(s.parent_slot, 0);
                } else {
                    assert_eq!(s.parent_slot, i - 1);
                }

                assert_eq!(s.last_index, entries_per_slot - 1);

                // Other than slot 0, no slots should be part of the trunk
                if i != 0 {
                    assert!(!s.is_connected);
                } else {
                    assert!(s.is_connected);
                }
            }

            // Iteratively finish every 3rd slot, and check that all slots up to and including
            // slot_index + 3 become part of the trunk
            for (slot_index, slot_ticks) in blobs.chunks(entries_per_slot as usize).enumerate() {
                if slot_index % 3 == 0 {
                    block_buffer_pool.update_blobs(&slot_ticks[0..1]).unwrap();

                    for i in 0..num_slots {
                        let s = block_buffer_pool.meta(i as u64).unwrap().unwrap();
                        if i != num_slots - 1 {
                            assert_eq!(s.next_slots, vec![i as u64 + 1]);
                        } else {
                            assert!(s.next_slots.is_empty());
                        }
                        if i <= slot_index as u64 + 3 {
                            assert!(s.is_connected);
                        } else {
                            assert!(!s.is_connected);
                        }

                        if i == 0 {
                            assert_eq!(s.parent_slot, 0);
                        } else {
                            assert_eq!(s.parent_slot, i - 1);
                        }

                        assert_eq!(s.last_index, entries_per_slot - 1);
                    }
                }
            }
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_chaining_tree() {
        let block_buffer_pool_path = fetch_interim_ledger_location("test_chaining_tree");
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();
            let num_tree_levels = 6;
            assert!(num_tree_levels > 1);
            let branching_factor: u64 = 4;
            // Number of slots that will be in the tree
            let num_slots = (branching_factor.pow(num_tree_levels) - 1) / (branching_factor - 1);
            let entries_per_slot = NUM_DATA as u64;
            assert!(entries_per_slot > 1);

            let (mut blobs, _) = make_many_slot_entries(0, num_slots, entries_per_slot);

            // Insert tree one slot at a time in a random order
            let mut slots: Vec<_> = (0..num_slots).collect();

            // Get blobs for the slot
            slots.shuffle(&mut thread_rng());
            for slot in slots {
                // Get blobs for the slot "slot"
                let slot_blobs = &mut blobs
                    [(slot * entries_per_slot) as usize..((slot + 1) * entries_per_slot) as usize];
                for blob in slot_blobs.iter_mut() {
                    // Get the parent slot of the slot in the tree
                    let slot_parent = {
                        if slot == 0 {
                            0
                        } else {
                            (slot - 1) / branching_factor
                        }
                    };
                    blob.set_parent(slot_parent);
                }

                let shared_blobs: Vec<_> = slot_blobs
                    .iter()
                    .cloned()
                    .map(|blob| Arc::new(RwLock::new(blob)))
                    .collect();
                let mut coding_generator = CodingGenerator::new(Arc::clone(&block_buffer_pool.session));
                let coding_blobs = coding_generator.next(&shared_blobs);
                assert_eq!(coding_blobs.len(), NUM_CODING);

                let mut rng = thread_rng();

                // Randomly pick whether to insert erasure or coding blobs first
                if rng.gen_bool(0.5) {
                    block_buffer_pool.update_blobs(slot_blobs).unwrap();
                    block_buffer_pool.insert_multiple_coding_blob_bytes(&coding_blobs).unwrap();
                } else {
                    block_buffer_pool.insert_multiple_coding_blob_bytes(&coding_blobs).unwrap();
                    block_buffer_pool.update_blobs(slot_blobs).unwrap();
                }
            }

            // Make sure everything chains correctly
            let last_level =
                (branching_factor.pow(num_tree_levels - 1) - 1) / (branching_factor - 1);
            for slot in 0..num_slots {
                let slot_meta = block_buffer_pool.meta(slot).unwrap().unwrap();
                assert_eq!(slot_meta.consumed, entries_per_slot);
                assert_eq!(slot_meta.received, entries_per_slot);
                assert!(slot_meta.is_connected);
                let slot_parent = {
                    if slot == 0 {
                        0
                    } else {
                        (slot - 1) / branching_factor
                    }
                };
                assert_eq!(slot_meta.parent_slot, slot_parent);

                let expected_children: HashSet<_> = {
                    if slot >= last_level {
                        HashSet::new()
                    } else {
                        let first_child_slot = min(num_slots - 1, slot * branching_factor + 1);
                        let last_child_slot = min(num_slots - 1, (slot + 1) * branching_factor);
                        (first_child_slot..last_child_slot + 1).collect()
                    }
                };

                let result: HashSet<_> = slot_meta.next_slots.iter().cloned().collect();
                if expected_children.len() != 0 {
                    assert_eq!(slot_meta.next_slots.len(), branching_factor as usize);
                } else {
                    assert_eq!(slot_meta.next_slots.len(), 0);
                }
                assert_eq!(expected_children, result);
            }

            // No orphan slots should exist
            assert!(block_buffer_pool.sgt_c_g.is_empty().unwrap())
        }

        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_get_slots_since() {
        let block_buffer_pool_path = fetch_interim_ledger_location("test_get_slots_since");

        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

            // Slot doesn't exist
            assert!(block_buffer_pool.fetch_slot_from(&vec![0]).unwrap().is_empty());

            let mut meta0 = MetaInfoCol::new(0, 0);
            block_buffer_pool.m_c_g.put(0, &meta0).unwrap();

            // Slot exists, chains to nothing
            let expected: HashMap<u64, Vec<u64>> =
                HashMap::from_iter(vec![(0, vec![])].into_iter());
            assert_eq!(block_buffer_pool.fetch_slot_from(&vec![0]).unwrap(), expected);
            meta0.next_slots = vec![1, 2];
            block_buffer_pool.m_c_g.put(0, &meta0).unwrap();

            // Slot exists, chains to some other slots
            let expected: HashMap<u64, Vec<u64>> =
                HashMap::from_iter(vec![(0, vec![1, 2])].into_iter());
            assert_eq!(block_buffer_pool.fetch_slot_from(&vec![0]).unwrap(), expected);
            assert_eq!(block_buffer_pool.fetch_slot_from(&vec![0, 1]).unwrap(), expected);

            let mut meta3 = MetaInfoCol::new(3, 1);
            meta3.next_slots = vec![10, 5];
            block_buffer_pool.m_c_g.put(3, &meta3).unwrap();
            let expected: HashMap<u64, Vec<u64>> =
                HashMap::from_iter(vec![(0, vec![1, 2]), (3, vec![10, 5])].into_iter());
            assert_eq!(block_buffer_pool.fetch_slot_from(&vec![0, 1, 3]).unwrap(), expected);
        }

        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_orphans() {
        let block_buffer_pool_path = fetch_interim_ledger_location("test_orphans");
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

            // Create blobs and entries
            let entries_per_slot = 1;
            let (blobs, _) = make_many_slot_entries(0, 3, entries_per_slot);

            // Write slot 2, which chains to slot 1. We're missing slot 0,
            // so slot 1 is the orphan
            block_buffer_pool.update_blobs(once(&blobs[2])).unwrap();
            let meta = block_buffer_pool
                .meta(1)
                .expect("Expect database get to succeed")
                .unwrap();
            assert!(is_singleton(&meta));
            assert_eq!(block_buffer_pool.fetch_singletons(None), vec![1]);

            // Write slot 1 which chains to slot 0, so now slot 0 is the
            // orphan, and slot 1 is no longer the orphan.
            block_buffer_pool.update_blobs(once(&blobs[1])).unwrap();
            let meta = block_buffer_pool
                .meta(1)
                .expect("Expect database get to succeed")
                .unwrap();
            assert!(!is_singleton(&meta));
            let meta = block_buffer_pool
                .meta(0)
                .expect("Expect database get to succeed")
                .unwrap();
            assert!(is_singleton(&meta));
            assert_eq!(block_buffer_pool.fetch_singletons(None), vec![0]);

            // Write some slot that also chains to existing slots and orphan,
            // nothing should change
            let blob4 = &make_slot_entries(4, 0, 1).0[0];
            let blob5 = &make_slot_entries(5, 1, 1).0[0];
            block_buffer_pool.update_blobs(vec![blob4, blob5]).unwrap();
            assert_eq!(block_buffer_pool.fetch_singletons(None), vec![0]);

            // Write zeroth slot, no more orphans
            block_buffer_pool.update_blobs(once(&blobs[0])).unwrap();
            for i in 0..3 {
                let meta = block_buffer_pool
                    .meta(i)
                    .expect("Expect database get to succeed")
                    .unwrap();
                assert!(!is_singleton(&meta));
            }
            // SingletonColumn cf is empty
            assert!(block_buffer_pool.sgt_c_g.is_empty().unwrap())
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    fn test_insert_data_blobs_slots(name: &str, should_bulk_write: bool) {
        let block_buffer_pool_path = fetch_interim_ledger_location(name);
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

            // Create blobs and entries
            let num_entries = 20 as u64;
            let mut entries = vec![];
            let mut blobs = vec![];
            for slot in 0..num_entries {
                let parent_slot = {
                    if slot == 0 {
                        0
                    } else {
                        slot - 1
                    }
                };

                let (mut blob, entry) = make_slot_entries(slot, parent_slot, 1);
                blob[0].set_index(slot);
                blobs.extend(blob);
                entries.extend(entry);
            }

            // Write blobs to the database
            if should_bulk_write {
                block_buffer_pool.update_blobs(blobs.iter()).unwrap();
            } else {
                for i in 0..num_entries {
                    let i = i as usize;
                    block_buffer_pool.update_blobs(&blobs[i..i + 1]).unwrap();
                }
            }

            for i in 0..num_entries - 1 {
                assert_eq!(
                    block_buffer_pool.fetch_slot_entries(i, i, None).unwrap()[0],
                    entries[i as usize]
                );

                let meta = block_buffer_pool.meta(i).unwrap().unwrap();
                assert_eq!(meta.received, i + 1);
                assert_eq!(meta.last_index, i);
                if i != 0 {
                    assert_eq!(meta.parent_slot, i - 1);
                    assert!(meta.consumed == 0);
                } else {
                    assert_eq!(meta.parent_slot, 0);
                    assert!(meta.consumed == 1);
                }
            }
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_find_missing_data_indexes() {
        let slot = 0;
        let block_buffer_pool_path = get_tmp_ledger_path!();
        let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

        // Write entries
        let gap = 10;
        assert!(gap > 3);
        let num_entries = 10;
        let mut blobs = make_tiny_test_entries(num_entries).to_single_entry_blobs();
        for (i, b) in blobs.iter_mut().enumerate() {
            b.set_index(i as u64 * gap);
            b.set_slot(slot);
        }
        block_buffer_pool.update_blobs(&blobs).unwrap();

        // Index of the first blob is 0
        // Index of the second blob is "gap"
        // Thus, the missing indexes should then be [1, gap - 1] for the input index
        // range of [0, gap)
        let expected: Vec<u64> = (1..gap).collect();
        assert_eq!(
            block_buffer_pool.search_absent_data_indexes(slot, 0, gap, gap as usize),
            expected
        );
        assert_eq!(
            block_buffer_pool.search_absent_data_indexes(slot, 1, gap, (gap - 1) as usize),
            expected,
        );
        assert_eq!(
            block_buffer_pool.search_absent_data_indexes(slot, 0, gap - 1, (gap - 1) as usize),
            &expected[..expected.len() - 1],
        );
        assert_eq!(
            block_buffer_pool.search_absent_data_indexes(slot, gap - 2, gap, gap as usize),
            vec![gap - 2, gap - 1],
        );
        assert_eq!(
            block_buffer_pool.search_absent_data_indexes(slot, gap - 2, gap, 1),
            vec![gap - 2],
        );
        assert_eq!(
            block_buffer_pool.search_absent_data_indexes(slot, 0, gap, 1),
            vec![1],
        );

        // Test with end indexes that are greater than the last item in the ledger
        let mut expected: Vec<u64> = (1..gap).collect();
        expected.push(gap + 1);
        assert_eq!(
            block_buffer_pool.search_absent_data_indexes(slot, 0, gap + 2, (gap + 2) as usize),
            expected,
        );
        assert_eq!(
            block_buffer_pool.search_absent_data_indexes(slot, 0, gap + 2, (gap - 1) as usize),
            &expected[..expected.len() - 1],
        );

        for i in 0..num_entries as u64 {
            for j in 0..i {
                let expected: Vec<u64> = (j..i)
                    .flat_map(|k| {
                        let begin = k * gap + 1;
                        let end = (k + 1) * gap;
                        (begin..end)
                    })
                    .collect();
                assert_eq!(
                    block_buffer_pool.search_absent_data_indexes(
                        slot,
                        j * gap,
                        i * gap,
                        ((i - j) * gap) as usize
                    ),
                    expected,
                );
            }
        }

        drop(block_buffer_pool);
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_find_missing_data_indexes_sanity() {
        let slot = 0;

        let block_buffer_pool_path = get_tmp_ledger_path!();
        let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

        // Early exit conditions
        let empty: Vec<u64> = vec![];
        assert_eq!(block_buffer_pool.search_absent_data_indexes(slot, 0, 0, 1), empty);
        assert_eq!(block_buffer_pool.search_absent_data_indexes(slot, 5, 5, 1), empty);
        assert_eq!(block_buffer_pool.search_absent_data_indexes(slot, 4, 3, 1), empty);
        assert_eq!(block_buffer_pool.search_absent_data_indexes(slot, 1, 2, 0), empty);

        let mut blobs = make_tiny_test_entries(2).to_single_entry_blobs();

        const ONE: u64 = 1;
        const OTHER: u64 = 4;

        blobs[0].set_index(ONE);
        blobs[1].set_index(OTHER);

        // Insert one blob at index = first_index
        block_buffer_pool.update_blobs(&blobs).unwrap();

        const STARTS: u64 = OTHER * 2;
        const END: u64 = OTHER * 3;
        const MAX: usize = 10;
        // The first blob has index = first_index. Thus, for i < first_index,
        // given the input range of [i, first_index], the missing indexes should be
        // [i, first_index - 1]
        for start in 0..STARTS {
            let result = block_buffer_pool.search_absent_data_indexes(
                slot, start, // start
                END,   //end
                MAX,   //max
            );
            let expected: Vec<u64> = (start..END).filter(|i| *i != ONE && *i != OTHER).collect();
            assert_eq!(result, expected);
        }

        drop(block_buffer_pool);
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_no_missing_blob_indexes() {
        let slot = 0;
        let block_buffer_pool_path = get_tmp_ledger_path!();
        let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

        // Write entries
        let num_entries = 10;
        let shared_blobs = make_tiny_test_entries(num_entries).to_single_entry_shared_blobs();

        crate::packet::index_blobs(&shared_blobs, &Pubkey::new_rand(), 0, slot, 0);

        let blob_locks: Vec<_> = shared_blobs.iter().map(|b| b.read().unwrap()).collect();
        let blobs: Vec<&Blob> = blob_locks.iter().map(|b| &**b).collect();
        block_buffer_pool.update_blobs(blobs).unwrap();

        let empty: Vec<u64> = vec![];
        for i in 0..num_entries as u64 {
            for j in 0..i {
                assert_eq!(
                    block_buffer_pool.search_absent_data_indexes(slot, j, i, (i - j) as usize),
                    empty
                );
            }
        }

        drop(block_buffer_pool);
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_should_insert_blob() {
        let (mut blobs, _) = make_slot_entries(0, 0, 20);
        let block_buffer_pool_path = get_tmp_ledger_path!();
        let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

        // Insert the first 5 blobs, we don't have a "is_last" blob yet
        block_buffer_pool.insert_data_blobs(&blobs[0..5]).unwrap();

        // Trying to insert a blob less than consumed should fail
        let slot_meta = block_buffer_pool.meta(0).unwrap().unwrap();
        assert_eq!(slot_meta.consumed, 5);
        assert!(!shd_insrt_blob(
            &slot_meta,
            &block_buffer_pool.db,
            &HashMap::new(),
            &blobs[4].clone()
        ));

        // Trying to insert the same blob again should fail
        block_buffer_pool.insert_data_blobs(&blobs[7..8]).unwrap();
        let slot_meta = block_buffer_pool.meta(0).unwrap().unwrap();
        assert!(!shd_insrt_blob(
            &slot_meta,
            &block_buffer_pool.db,
            &HashMap::new(),
            &blobs[7].clone()
        ));

        // Trying to insert another "is_last" blob with index < the received index
        // should fail
        block_buffer_pool.insert_data_blobs(&blobs[8..9]).unwrap();
        let slot_meta = block_buffer_pool.meta(0).unwrap().unwrap();
        assert_eq!(slot_meta.received, 9);
        blobs[8].set_is_last_in_slot();
        assert!(!shd_insrt_blob(
            &slot_meta,
            &block_buffer_pool.db,
            &HashMap::new(),
            &blobs[8].clone()
        ));

        // Insert the 10th blob, which is marked as "is_last"
        blobs[9].set_is_last_in_slot();
        block_buffer_pool.insert_data_blobs(&blobs[9..10]).unwrap();
        let slot_meta = block_buffer_pool.meta(0).unwrap().unwrap();

        // Trying to insert a blob with index > the "is_last" blob should fail
        assert!(!shd_insrt_blob(
            &slot_meta,
            &block_buffer_pool.db,
            &HashMap::new(),
            &blobs[10].clone()
        ));

        drop(block_buffer_pool);
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_insert_multiple_is_last() {
        let (mut blobs, _) = make_slot_entries(0, 0, 20);
        let block_buffer_pool_path = get_tmp_ledger_path!();
        let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

        // Inserting multiple blobs with the is_last flag set should only insert
        // the first blob with the "is_last" flag, and drop the rest
        for i in 6..20 {
            blobs[i].set_is_last_in_slot();
        }

        block_buffer_pool.insert_data_blobs(&blobs[..]).unwrap();
        let slot_meta = block_buffer_pool.meta(0).unwrap().unwrap();

        assert_eq!(slot_meta.consumed, 7);
        assert_eq!(slot_meta.received, 7);
        assert_eq!(slot_meta.last_index, 6);
        assert!(slot_meta.is_full());

        drop(block_buffer_pool);
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_slot_data_iterator() {
        // Construct the blobs
        let block_buffer_pool_path = get_tmp_ledger_path!();
        let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();
        let blobs_per_slot = 10;
        let slots = vec![2, 4, 8, 12];
        let all_blobs = make_chaining_slot_entries(&slots, blobs_per_slot);
        let slot_8_blobs = all_blobs[2].0.clone();
        for (slot_blobs, _) in all_blobs {
            block_buffer_pool.insert_data_blobs(&slot_blobs[..]).unwrap();
        }

        // Slot doesnt exist, iterator should be empty
        let blob_iter = block_buffer_pool.data_col_looper(5).unwrap();
        let result: Vec<_> = blob_iter.collect();
        assert_eq!(result, vec![]);

        // Test that the iterator for slot 8 contains what was inserted earlier
        let blob_iter = block_buffer_pool.data_col_looper(8).unwrap();
        let result: Vec<_> = blob_iter.map(|(_, bytes)| Blob::new(&bytes)).collect();
        assert_eq!(result.len() as u64, blobs_per_slot);
        assert_eq!(result, slot_8_blobs);

        drop(block_buffer_pool);
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_set_root() {
        let block_buffer_pool_path = get_tmp_ledger_path!();
        let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();
        block_buffer_pool.set_genesis(0, 0).unwrap();
        let chained_slots = vec![0, 2, 4, 7, 12, 15];

        // Make a chain of slots
        let all_blobs = make_chaining_slot_entries(&chained_slots, 10);

        // Insert the chain of slots into the ledger
        for (slot_blobs, _) in all_blobs {
            block_buffer_pool.insert_data_blobs(&slot_blobs[..]).unwrap();
        }

        block_buffer_pool.set_genesis(4, 0).unwrap();
        for i in &chained_slots[0..3] {
            assert!(block_buffer_pool.is_genesis(*i));
        }

        for i in &chained_slots[3..] {
            assert!(!block_buffer_pool.is_genesis(*i));
        }

        block_buffer_pool.set_genesis(15, 4).unwrap();

        for i in chained_slots {
            assert!(block_buffer_pool.is_genesis(i));
        }

        drop(block_buffer_pool);
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    mod erasure {
        use super::*;
        use crate::block_buffer_pool::meta::ErasureMetaStatus;
        use crate::expunge::test::{generate_ledger_model, ErasureSpec, SlotSpec};
        use crate::expunge::{CodingGenerator, NUM_CODING, NUM_DATA};
        use rand::{thread_rng, Rng};
        use std::sync::RwLock;

        impl Into<SharedBlob> for Blob {
            fn into(self) -> SharedBlob {
                Arc::new(RwLock::new(self))
            }
        }

        #[test]
        fn test_erasure_meta_accuracy() {
            use crate::expunge::ERASURE_SET_SIZE;
            use ErasureMetaStatus::{DataFull, StillNeed};

            let path = get_tmp_ledger_path!();
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&path).unwrap();

            // two erasure sets
            let num_blobs = NUM_DATA as u64 * 2;
            let slot = 0;

            let (blobs, _) = make_slot_entries(slot, 0, num_blobs);
            let shared_blobs: Vec<_> = blobs
                .iter()
                .cloned()
                .map(|blob| Arc::new(RwLock::new(blob)))
                .collect();

            block_buffer_pool.update_blobs(&blobs[..2]).unwrap();

            let erasure_meta_opt = block_buffer_pool
                .erasure_meta(slot, 0)
                .expect("DB get must succeed");

            assert!(erasure_meta_opt.is_some());
            let erasure_meta = erasure_meta_opt.unwrap();

            let should_need = ERASURE_SET_SIZE - NUM_CODING - 2;
            match erasure_meta.status() {
                StillNeed(n) => assert_eq!(n, should_need),
                _ => panic!("Should still need more blobs"),
            };

            block_buffer_pool.update_blobs(&blobs[2..NUM_DATA]).unwrap();

            let erasure_meta = block_buffer_pool
                .erasure_meta(slot, 0)
                .expect("DB get must succeed")
                .unwrap();

            assert_eq!(erasure_meta.status(), DataFull);

            // insert all coding blobs in first set
            let mut coding_generator = CodingGenerator::new(Arc::clone(&block_buffer_pool.session));
            let coding_blobs = coding_generator.next(&shared_blobs[..NUM_DATA]);

            for shared_coding_blob in coding_blobs {
                let blob = shared_coding_blob.read().unwrap();
                let size = blob.size() + BLOB_HEADER_SIZE;
                block_buffer_pool
                    .insert_coding_blob_bytes(blob.slot(), blob.index(), &blob.data[..size])
                    .unwrap();
            }

            let erasure_meta = block_buffer_pool
                .erasure_meta(slot, 0)
                .expect("DB get must succeed")
                .unwrap();

            assert_eq!(erasure_meta.status(), DataFull);

            // insert blobs in the 2nd set until recovery should be possible given all coding blobs
            let set2 = &blobs[NUM_DATA..];
            let mut end = 1;
            let blobs_needed = ERASURE_SET_SIZE - NUM_CODING;
            while end < blobs_needed {
                block_buffer_pool.update_blobs(&set2[end - 1..end]).unwrap();

                let erasure_meta = block_buffer_pool
                    .erasure_meta(slot, 1)
                    .expect("DB get must succeed")
                    .unwrap();

                match erasure_meta.status() {
                    StillNeed(n) => assert_eq!(n, blobs_needed - end),
                    _ => panic!("Should still need more blobs"),
                };

                end += 1;
            }

            // insert all coding blobs in 2nd set. Should trigger recovery
            let mut coding_generator = CodingGenerator::new(Arc::clone(&block_buffer_pool.session));
            let coding_blobs = coding_generator.next(&shared_blobs[NUM_DATA..]);

            for shared_coding_blob in coding_blobs {
                let blob = shared_coding_blob.read().unwrap();
                let size = blob.size() + BLOB_HEADER_SIZE;
                block_buffer_pool
                    .insert_coding_blob_bytes(blob.slot(), blob.index(), &blob.data[..size])
                    .unwrap();
            }

            let erasure_meta = block_buffer_pool
                .erasure_meta(slot, 1)
                .expect("DB get must succeed")
                .unwrap();

            assert_eq!(erasure_meta.status(), DataFull);

            // remove coding blobs, erasure meta should still report being full
            let (start_idx, coding_end_idx) =
                (erasure_meta.start_index(), erasure_meta.end_indexes().1);

            for idx in start_idx..coding_end_idx {
                block_buffer_pool.remove_coding_blob(slot, idx).unwrap();
            }

            let erasure_meta = block_buffer_pool
                .erasure_meta(slot, 1)
                .expect("DB get must succeed")
                .unwrap();

            assert_eq!(erasure_meta.status(), ErasureMetaStatus::DataFull);
        }

        #[test]
        pub fn test_recovery_basic() {
            morgan_logger::setup();

            let slot = 0;

            let ledger_path = get_tmp_ledger_path!();

            let block_buffer_pool = BlockBufferPool::open_ledger_file(&ledger_path).unwrap();
            let num_sets = 3;
            let data_blobs = make_slot_entries(slot, 0, num_sets * NUM_DATA as u64)
                .0
                .into_iter()
                .map(Blob::into)
                .collect::<Vec<_>>();

            let mut coding_generator = CodingGenerator::new(Arc::clone(&block_buffer_pool.session));

            for (set_index, data_blobs) in data_blobs.chunks_exact(NUM_DATA).enumerate() {
                let focused_index = (set_index + 1) * NUM_DATA - 1;
                let coding_blobs = coding_generator.next(&data_blobs);

                assert_eq!(coding_blobs.len(), NUM_CODING);

                let deleted_data = data_blobs[NUM_DATA - 1].clone();

                block_buffer_pool
                    .record_public_objs(&data_blobs[..NUM_DATA - 1])
                    .unwrap();

                // This should trigger recovery of the missing data blob
                for shared_coding_blob in coding_blobs {
                    let blob = shared_coding_blob.read().unwrap();
                    let size = blob.size() + BLOB_HEADER_SIZE;

                    block_buffer_pool
                        .insert_coding_blob_bytes(slot, blob.index(), &blob.data[..size])
                        .expect("Inserting coding blobs must succeed");
                    (slot, blob.index());
                }

                // Verify the slot meta
                let slot_meta = block_buffer_pool.meta(slot).unwrap().unwrap();
                assert_eq!(slot_meta.consumed, (NUM_DATA * (set_index + 1)) as u64);
                assert_eq!(slot_meta.received, (NUM_DATA * (set_index + 1)) as u64);
                assert_eq!(slot_meta.parent_slot, 0);
                assert!(slot_meta.next_slots.is_empty());
                assert_eq!(slot_meta.is_connected, true);
                if set_index as u64 == num_sets - 1 {
                    assert_eq!(
                        slot_meta.last_index,
                        (NUM_DATA * (set_index + 1) - 1) as u64
                    );
                }

                let erasure_meta = block_buffer_pool
                    .e_m_c_g
                    .get((slot, set_index as u64))
                    .expect("Erasure Meta should be present")
                    .unwrap();

                assert_eq!(erasure_meta.status(), ErasureMetaStatus::DataFull);

                let retrieved_data = block_buffer_pool
                    .d_c_g
                    .get_bytes((slot, focused_index as u64))
                    .unwrap();

                assert!(retrieved_data.is_some());

                let data_blob = Blob::new(&retrieved_data.unwrap());

                assert_eq!(&data_blob, &*deleted_data.read().unwrap());
            }

            drop(block_buffer_pool);

            BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expect successful BlockBufferPool destruction");
        }

        #[test]
        fn test_recovery_fails_safely() {
            const SLOT: u64 = 0;
            const SET_INDEX: u64 = 0;

            morgan_logger::setup();
            let ledger_path = get_tmp_ledger_path!();
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&ledger_path).unwrap();
            let data_blobs = make_slot_entries(SLOT, 0, NUM_DATA as u64)
                .0
                .into_iter()
                .map(Blob::into)
                .collect::<Vec<_>>();

            let mut coding_generator = CodingGenerator::new(Arc::clone(&block_buffer_pool.session));

            let shared_coding_blobs = coding_generator.next(&data_blobs);
            assert_eq!(shared_coding_blobs.len(), NUM_CODING);

            // Insert coding blobs except 1 and no data. Not enough to do recovery
            for shared_blob in shared_coding_blobs.iter().skip(1) {
                let blob = shared_blob.read().unwrap();
                let size = blob.size() + BLOB_HEADER_SIZE;

                block_buffer_pool
                    .insert_coding_blob_bytes(SLOT, blob.index(), &blob.data[..size])
                    .expect("Inserting coding blobs must succeed");
            }

            // try recovery even though there aren't enough blobs
            let erasure_meta = block_buffer_pool
                .e_m_c_g
                .get((SLOT, SET_INDEX))
                .unwrap()
                .unwrap();

            assert_eq!(erasure_meta.status(), ErasureMetaStatus::StillNeed(1));

            let prev_inserted_blob_datas = HashMap::new();

            let attempt_result = attempt_erasure_restore(
                &block_buffer_pool.db,
                &block_buffer_pool.session,
                &erasure_meta,
                SLOT,
                &prev_inserted_blob_datas,
                None,
            );

            assert!(attempt_result.is_ok());
            let recovered_blobs_opt = attempt_result.unwrap();

            assert!(recovered_blobs_opt.is_none());
        }

        #[test]
        fn test_recovery_multi_slot_multi_thread() {
            use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
            use std::thread;

            const N_THREADS: usize = 3;

            let slots = vec![0, 3, 5, 50, 100];
            let max_erasure_sets = 16;
            morgan_logger::setup();

            let path = get_tmp_ledger_path!();
            let mut rng = thread_rng();

            // Specification should generate a ledger where each slot has an random number of
            // erasure sets. Odd erasure sets will have all coding blobs and between 1-4 data blobs
            // missing, and even ones will have between 1-2 data blobs missing and 1-2 coding blobs
            // missing
            let specs = slots
                .iter()
                .map(|&slot| {
                    let num_erasure_sets = rng.gen_range(0, max_erasure_sets);

                    let set_specs = (0..num_erasure_sets)
                        .map(|set_index| {
                            let (num_data, num_coding) = if set_index % 2 == 0 {
                                (
                                    NUM_DATA - rng.gen_range(1, 3),
                                    NUM_CODING - rng.gen_range(1, 3),
                                )
                            } else {
                                (NUM_DATA - rng.gen_range(1, 5), NUM_CODING)
                            };
                            ErasureSpec {
                                set_index,
                                num_data,
                                num_coding,
                            }
                        })
                        .collect();

                    SlotSpec { slot, set_specs }
                })
                .collect::<Vec<_>>();

            let model = generate_ledger_model(specs);
            let block_buffer_pool = Arc::new(BlockBufferPool::open_ledger_file(&path).unwrap());

            // Write to each slot in a different thread simultaneously.
            // These writes should trigger the recovery. Every erasure set should have all of its
            // data blobs and coding_blobs at the end
            let mut handles = vec![];

            // Each thread will attempt to write to each slot in order. Within a slot, each thread
            // will try to write each erasure set in a random order. Within each erasure set, there
            // is a 50/50 chance of attempting to write the coding blobs first or the data blobs
            // first.
            // The goal is to be as racey as possible and cover a wide range of situations
            for thread_id in 0..N_THREADS {
                let block_buffer_pool = Arc::clone(&block_buffer_pool);
                let mut rng = SmallRng::from_rng(&mut rng).unwrap();
                let model = model.clone();
                let handle = thread::spawn(move || {
                    for slot_model in model {
                        let slot = slot_model.slot;
                        let num_erasure_sets = slot_model.chunks.len();
                        let unordered_sets = slot_model
                            .chunks
                            .choose_multiple(&mut rng, num_erasure_sets);

                        for erasure_set in unordered_sets {
                            let mut attempt = 0;
                            loop {
                                if rng.gen() {
                                    block_buffer_pool
                                        .record_public_objs(&erasure_set.data)
                                        .expect("Writing data blobs must succeed");
                                    debug!(
                                        "multislot: wrote data: slot: {}, erasure_set: {}",
                                        slot, erasure_set.set_index
                                    );

                                    for shared_coding_blob in &erasure_set.coding {
                                        let blob = shared_coding_blob.read().unwrap();
                                        let size = blob.size() + BLOB_HEADER_SIZE;
                                        block_buffer_pool
                                            .insert_coding_blob_bytes(
                                                slot,
                                                blob.index(),
                                                &blob.data[..size],
                                            )
                                            .expect("Writing coding blobs must succeed");
                                    }
                                    debug!(
                                        "multislot: wrote coding: slot: {}, erasure_set: {}",
                                        slot, erasure_set.set_index
                                    );
                                } else {
                                    // write coding blobs first, then write the data blobs.
                                    for shared_coding_blob in &erasure_set.coding {
                                        let blob = shared_coding_blob.read().unwrap();
                                        let size = blob.size() + BLOB_HEADER_SIZE;
                                        block_buffer_pool
                                            .insert_coding_blob_bytes(
                                                slot,
                                                blob.index(),
                                                &blob.data[..size],
                                            )
                                            .expect("Writing coding blobs must succeed");
                                    }
                                    debug!(
                                        "multislot: wrote coding: slot: {}, erasure_set: {}",
                                        slot, erasure_set.set_index
                                    );

                                    block_buffer_pool
                                        .record_public_objs(&erasure_set.data)
                                        .expect("Writing data blobs must succeed");
                                    debug!(
                                        "multislot: wrote data: slot: {}, erasure_set: {}",
                                        slot, erasure_set.set_index
                                    );
                                }

                                // due to racing, some blobs might not be inserted. don't stop
                                // trying until *some* thread succeeds in writing everything and
                                // triggering recovery.
                                let erasure_meta = block_buffer_pool
                                    .e_m_c_g
                                    .get((slot, erasure_set.set_index))
                                    .unwrap()
                                    .unwrap();

                                let status = erasure_meta.status();
                                attempt += 1;

                                debug!(
                                    "[multi_slot] thread_id: {}, attempt: {}, slot: {}, set_index: {}, status: {:?}",
                                    thread_id, attempt, slot, erasure_set.set_index, status
                                );
                                if status == ErasureMetaStatus::DataFull {
                                    break;
                                }
                            }
                        }
                    }
                });

                handles.push(handle);
            }

            handles
                .into_iter()
                .for_each(|handle| handle.join().unwrap());

            for slot_model in model {
                let slot = slot_model.slot;

                for erasure_set_model in slot_model.chunks {
                    let set_index = erasure_set_model.set_index as u64;

                    let erasure_meta = block_buffer_pool
                        .e_m_c_g
                        .get((slot, set_index))
                        .expect("DB get must succeed")
                        .expect("ErasureMetaColumn must be present for each erasure set");

                    debug!(
                        "multislot: got erasure_meta: slot: {}, set_index: {}, erasure_meta: {:?}",
                        slot, set_index, erasure_meta
                    );

                    // all possibility for recovery should be exhausted
                    assert_eq!(erasure_meta.status(), ErasureMetaStatus::DataFull);
                    // Should have all data
                    assert_eq!(erasure_meta.num_data(), NUM_DATA);
                    // Should have all coding
                    assert_eq!(erasure_meta.num_coding(), NUM_CODING);
                }
            }

            drop(block_buffer_pool);
            BlockBufferPool::remove_ledger_file(&path).expect("BlockBufferPool destruction must succeed");
        }
    }

    pub fn entries_to_blobs(
        entries: &Vec<Entry>,
        slot: u64,
        parent_slot: u64,
        is_full_slot: bool,
    ) -> Vec<Blob> {
        let mut blobs = entries.clone().to_single_entry_blobs();
        for (i, b) in blobs.iter_mut().enumerate() {
            b.set_index(i as u64);
            b.set_slot(slot);
            b.set_parent(parent_slot);
        }
        if is_full_slot {
            blobs.last_mut().unwrap().set_is_last_in_slot();
        }
        blobs
    }

    pub fn make_slot_entries(
        slot: u64,
        parent_slot: u64,
        num_entries: u64,
    ) -> (Vec<Blob>, Vec<Entry>) {
        let entries = make_tiny_test_entries(num_entries as usize);
        let blobs = entries_to_blobs(&entries, slot, parent_slot, true);
        (blobs, entries)
    }

    pub fn make_many_slot_entries(
        start_slot: u64,
        num_slots: u64,
        entries_per_slot: u64,
    ) -> (Vec<Blob>, Vec<Entry>) {
        let mut blobs = vec![];
        let mut entries = vec![];
        for slot in start_slot..start_slot + num_slots {
            let parent_slot = if slot == 0 { 0 } else { slot - 1 };

            let (slot_blobs, slot_entries) = make_slot_entries(slot, parent_slot, entries_per_slot);
            blobs.extend(slot_blobs);
            entries.extend(slot_entries);
        }

        (blobs, entries)
    }

    // Create blobs for slots that have a parent-child relationship defined by the input `chain`
    pub fn make_chaining_slot_entries(
        chain: &[u64],
        entries_per_slot: u64,
    ) -> Vec<(Vec<Blob>, Vec<Entry>)> {
        let mut slots_blobs_and_entries = vec![];
        for (i, slot) in chain.iter().enumerate() {
            let parent_slot = {
                if *slot == 0 {
                    0
                } else if i == 0 {
                    std::u64::MAX
                } else {
                    chain[i - 1]
                }
            };

            let result = make_slot_entries(*slot, parent_slot, entries_per_slot);
            slots_blobs_and_entries.push(result);
        }

        slots_blobs_and_entries
    }
}
