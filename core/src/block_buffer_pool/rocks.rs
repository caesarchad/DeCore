use crate::block_buffer_pool::db::columns as cf;
use crate::block_buffer_pool::db::{DaemonDb, Column, DbCursor, IWriteBatch, TypedColumn};
use crate::block_buffer_pool::BlockBufferPoolError;
use crate::result::{Error, Result};

use byteorder::{BigEndian, ByteOrder};

use rocksdb::{
    self, ColumnFamily, ColumnFamilyDescriptor, DBIterator, DBRawIterator, Direction, IteratorMode,
    Options, WriteBatch as RWriteBatch, DB,
};

use std::fs;
use std::path::Path;

// A good value for this is the number of cores on the machine
const TOTAL_THREADS: i32 = 8;
const MAX_WRITE_BUFFER_SIZE: usize = 512 * 1024 * 1024;

#[derive(Debug)]
pub struct RocksDB(rocksdb::DB);

impl DaemonDb for RocksDB {
    type Key = [u8];
    type OwnedKey = Vec<u8>;
    type ColumnFamily = ColumnFamily;
    type Cursor = DBRawIterator;
    type Iter = DBIterator;
    type WriteBatch = RWriteBatch;
    type Error = rocksdb::Error;

    fn open(path: &Path) -> Result<RocksDB> {
        use crate::block_buffer_pool::db::columns::{ErasureColumn, DataColumn, ErasureMetaColumn, SingletonColumn, GenesisColumn, MetaInfoCol};

        fs::create_dir_all(&path)?;

        // Use default database options
        let db_options = get_db_options();

        // Column family names
        /*
        let metainfo_column_group_info = ColumnFamilyDescriptor::new(MetaInfoCol::NAME, get_cf_options());
        let data_column_group_info = ColumnFamilyDescriptor::new(DataColumn::NAME, get_cf_options());
        let erasure_column_group_info = ColumnFamilyDescriptor::new(ErasureColumn::NAME, get_cf_options());
        let erasure_metainfo_column_group_info =
            ColumnFamilyDescriptor::new(ErasureMetaColumn::NAME, get_cf_options());
        let singleton_column_group_info = ColumnFamilyDescriptor::new(SingletonColumn::NAME, get_cf_options());
        let genesis_column_group_info = ColumnFamilyDescriptor::new(GenesisColumn::NAME, get_cf_options());
        */
        let (
            metainfo_column_group_info,
            data_column_group_info,
            erasure_column_group_info,
            erasure_metainfo_column_group_info,
            singleton_column_group_info,
            genesis_column_group_info
        ) = (
            ColumnFamilyDescriptor::new(MetaInfoCol::NAME, get_cf_options()),
            ColumnFamilyDescriptor::new(DataColumn::NAME, get_cf_options()),
            ColumnFamilyDescriptor::new(ErasureColumn::NAME, get_cf_options()),
            ColumnFamilyDescriptor::new(ErasureMetaColumn::NAME, get_cf_options()),
            ColumnFamilyDescriptor::new(SingletonColumn::NAME, get_cf_options()),
            ColumnFamilyDescriptor::new(GenesisColumn::NAME, get_cf_options())
        );




        let cfs = vec![
            metainfo_column_group_info,
            data_column_group_info,
            erasure_column_group_info,
            erasure_metainfo_column_group_info,
            singleton_column_group_info,
            genesis_column_group_info,
        ];

        // Open the database
        let db = RocksDB(DB::open_cf_descriptors(&db_options, path, cfs)?);

        Ok(db)
    }

    fn columns(&self) -> Vec<&'static str> {
        use crate::block_buffer_pool::db::columns::{ErasureColumn, DataColumn, ErasureMetaColumn, SingletonColumn, GenesisColumn, MetaInfoCol};

        vec![
            ErasureColumn::NAME,
            ErasureMetaColumn::NAME,
            DataColumn::NAME,
            SingletonColumn::NAME,
            GenesisColumn::NAME,
            MetaInfoCol::NAME,
        ]
    }

    fn destroy(path: &Path) -> Result<()> {
        DB::destroy(&Options::default(), path)?;

        Ok(())
    }

    fn remove(path: &Path) -> Result<()> {
        DB::destroy(&Options::default(), path)?;

        Ok(())
    }

    fn repair(path: &Path) -> Result<()> {
        DB::repair(Options::default(), path)?;

        Ok(())
    }

    fn cf_handle(&self, cf: &str) -> ColumnFamily {
        self.0
            .cf_handle(cf)
            .expect("should never get an unknown column")
    }

    fn get_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let opt = self.0.get_cf(cf, key)?.map(|db_vec| db_vec.to_vec());
        Ok(opt)
    }

    fn put_cf(&self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<()> {
        self.0.put_cf(cf, key, value)?;
        Ok(())
    }

    fn delete_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<()> {
        self.0.delete_cf(cf, key)?;
        Ok(())
    }

    fn iterator_cf(&self, cf: ColumnFamily, start_from: Option<&[u8]>) -> Result<DBIterator> {
        let iter = {
            if let Some(start_from) = start_from {
                self.0
                    .iterator_cf(cf, IteratorMode::From(start_from, Direction::Forward))?
            } else {
                self.0.iterator_cf(cf, IteratorMode::Start)?
            }
        };

        Ok(iter)
    }

    fn raw_iterator_cf(&self, cf: ColumnFamily) -> Result<DBRawIterator> {
        let raw_iter = self.0.raw_iterator_cf(cf)?;

        Ok(raw_iter)
    }

    fn batch(&self) -> Result<RWriteBatch> {
        Ok(RWriteBatch::default())
    }

    fn write(&self, batch: RWriteBatch) -> Result<()> {
        self.0.write(batch)?;
        Ok(())
    }
}

impl Column<RocksDB> for cf::ErasureColumn {
    const NAME: &'static str = super::ERASURE_COLUMN_GROUP;
    type Index = (u64, u64);

    fn key(index: (u64, u64)) -> Vec<u8> {
        cf::DataColumn::key(index)
    }

    fn index(key: &[u8]) -> (u64, u64) {
        cf::DataColumn::index(key)
    }
}

impl Column<RocksDB> for cf::DataColumn {
    const NAME: &'static str = super::DATA_COLUMN_GROUP;
    type Index = (u64, u64);

    fn key((slot, index): (u64, u64)) -> Vec<u8> {
        let mut key = vec![0; 16];
        BigEndian::write_u64(&mut key[..8], slot);
        BigEndian::write_u64(&mut key[8..16], index);
        key
    }

    fn index(key: &[u8]) -> (u64, u64) {
        let slot = BigEndian::read_u64(&key[..8]);
        let index = BigEndian::read_u64(&key[8..16]);
        (slot, index)
    }
}

impl Column<RocksDB> for cf::SingletonColumn {
    const NAME: &'static str = super::SINGLETON_COLUMN_GROUP;
    type Index = u64;

    fn key(slot: u64) -> Vec<u8> {
        let mut key = vec![0; 8];
        BigEndian::write_u64(&mut key[..], slot);
        key
    }

    fn index(key: &[u8]) -> u64 {
        BigEndian::read_u64(&key[..8])
    }
}

impl TypedColumn<RocksDB> for cf::SingletonColumn {
    type Type = bool;
}

impl Column<RocksDB> for cf::GenesisColumn {
    const NAME: &'static str = super::GENESIS_COLUMN_GROUP;
    type Index = u64;

    fn key(slot: u64) -> Vec<u8> {
        let mut key = vec![0; 8];
        BigEndian::write_u64(&mut key[..], slot);
        key
    }

    fn index(key: &[u8]) -> u64 {
        BigEndian::read_u64(&key[..8])
    }
}

impl TypedColumn<RocksDB> for cf::GenesisColumn {
    type Type = bool;
}

impl Column<RocksDB> for cf::MetaInfoCol {
    const NAME: &'static str = super::METAINFO_COLUMN_GROUP;
    type Index = u64;

    fn key(slot: u64) -> Vec<u8> {
        let mut key = vec![0; 8];
        BigEndian::write_u64(&mut key[..], slot);
        key
    }

    fn index(key: &[u8]) -> u64 {
        BigEndian::read_u64(&key[..8])
    }
}

impl TypedColumn<RocksDB> for cf::MetaInfoCol {
    type Type = super::MetaInfoCol;
}

impl Column<RocksDB> for cf::ErasureMetaColumn {
    const NAME: &'static str = super::ERASURE_METAINFO_COLUMN_GROUP;
    type Index = (u64, u64);

    fn index(key: &[u8]) -> (u64, u64) {
        let slot = BigEndian::read_u64(&key[..8]);
        let set_index = BigEndian::read_u64(&key[8..]);

        (slot, set_index)
    }

    fn key((slot, set_index): (u64, u64)) -> Vec<u8> {
        let mut key = vec![0; 16];
        BigEndian::write_u64(&mut key[..8], slot);
        BigEndian::write_u64(&mut key[8..], set_index);
        key
    }
}

impl TypedColumn<RocksDB> for cf::ErasureMetaColumn {
    type Type = super::ErasureMetaColumn;
}

impl DbCursor<RocksDB> for DBRawIterator {
    fn valid(&self) -> bool {
        DBRawIterator::valid(self)
    }

    fn seek(&mut self, key: &[u8]) {
        DBRawIterator::seek(self, key);
    }

    fn seek_to_first(&mut self) {
        DBRawIterator::seek_to_first(self);
    }

    fn next(&mut self) {
        DBRawIterator::next(self);
    }

    fn key(&self) -> Option<Vec<u8>> {
        DBRawIterator::key(self)
    }

    fn value(&self) -> Option<Vec<u8>> {
        DBRawIterator::value(self)
    }
}

impl IWriteBatch<RocksDB> for RWriteBatch {
    fn put_cf(&mut self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<()> {
        RWriteBatch::put_cf(self, cf, key, value)?;
        Ok(())
    }

    fn delete_cf(&mut self, cf: ColumnFamily, key: &[u8]) -> Result<()> {
        RWriteBatch::delete_cf(self, cf, key)?;
        Ok(())
    }
}

impl std::convert::From<rocksdb::Error> for Error {
    fn from(e: rocksdb::Error) -> Error {
        Error::BlockBufferPoolError(BlockBufferPoolError::RocksDb(e))
    }
}

fn get_cf_options() -> Options {
    let mut options = Options::default();
    options.set_max_write_buffer_number(32);
    options.set_write_buffer_size(MAX_WRITE_BUFFER_SIZE);
    options.set_max_bytes_for_level_base(MAX_WRITE_BUFFER_SIZE as u64);
    options
}

fn get_db_options() -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.increase_parallelism(TOTAL_THREADS);
    options.set_max_background_flushes(4);
    options.set_max_background_compactions(4);
    options.set_max_write_buffer_number(32);
    options.set_write_buffer_size(MAX_WRITE_BUFFER_SIZE);
    options.set_max_bytes_for_level_base(MAX_WRITE_BUFFER_SIZE as u64);
    options
}
