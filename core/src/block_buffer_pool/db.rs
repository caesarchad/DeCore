use crate::result::{Error, Result};

use bincode::{deserialize, serialize};

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;


pub enum DaemonDbDumpPolicy {
    
    NeverDump,
    
    AutoDump,
    
    DumpUponRequest,
 
    PeriodicDump(Duration),
}


pub mod columns {
    #[derive(Debug)]
    /// MetaInfoCol Column
    pub struct MetaInfoCol;

    #[derive(Debug)]
    /// SingletonColumn Column
    pub struct SingletonColumn;

    #[derive(Debug)]
    /// Erasure Column
    pub struct ErasureColumn;

    #[derive(Debug)]
    /// DataColumn Column
    pub struct DataColumn;

    #[derive(Debug)]
    /// The erasure meta column
    pub struct ErasureMetaColumn;

    #[derive(Debug)]
    /// The root column
    pub struct GenesisColumn;
}

pub trait DaemonDb: Sized + Send + Sync {
    type Key: ?Sized + ToOwned<Owned = Self::OwnedKey>;
    type OwnedKey: Borrow<Self::Key>;
    type ColumnFamily: Clone;
    type Cursor: DbCursor<Self>;
    type Iter: Iterator<Item = (Box<Self::Key>, Box<[u8]>)>;
    type WriteBatch: IWriteBatch<Self>;
    type Error: Into<Error>;

    fn open(path: &Path) -> Result<Self>;

    fn columns(&self) -> Vec<&'static str>;

    fn destroy(path: &Path) -> Result<()>;

    fn remove(path: &Path) -> Result<()>;

    fn repair(path: &Path) -> Result<()>;

    fn cf_handle(&self, cf: &str) -> Self::ColumnFamily;

    fn get_cf(&self, cf: Self::ColumnFamily, key: &Self::Key) -> Result<Option<Vec<u8>>>;

    fn put_cf(&self, cf: Self::ColumnFamily, key: &Self::Key, value: &[u8]) -> Result<()>;

    fn delete_cf(&self, cf: Self::ColumnFamily, key: &Self::Key) -> Result<()>;

    fn iterator_cf(&self, cf: Self::ColumnFamily, from: Option<&Self::Key>) -> Result<Self::Iter>;

    fn raw_iterator_cf(&self, cf: Self::ColumnFamily) -> Result<Self::Cursor>;

    fn write(&self, batch: Self::WriteBatch) -> Result<()>;

    fn batch(&self) -> Result<Self::WriteBatch>;
}

pub trait Column<B>
where
    B: DaemonDb,
{
    const NAME: &'static str;
    type Index;

    fn key(index: Self::Index) -> B::OwnedKey;
    fn index(key: &B::Key) -> Self::Index;
}

pub trait DbCursor<B>
where
    B: DaemonDb,
{
    fn valid(&self) -> bool;

    fn seek(&mut self, key: &B::Key);

    fn seek_to_first(&mut self);

    fn next(&mut self);

    fn key(&self) -> Option<B::OwnedKey>;

    fn value(&self) -> Option<Vec<u8>>;
}

pub trait IWriteBatch<B>
where
    B: DaemonDb,
{
    fn put_cf(&mut self, cf: B::ColumnFamily, key: &B::Key, value: &[u8]) -> Result<()>;
    fn delete_cf(&mut self, cf: B::ColumnFamily, key: &B::Key) -> Result<()>;
}

pub trait TypedColumn<B>: Column<B>
where
    B: DaemonDb,
{
    type Type: Serialize + DeserializeOwned;
}

#[derive(Debug, Clone)]
pub struct Database<B>
where
    B: DaemonDb,
{
    backend: Arc<B>,
}

#[derive(Debug, Clone)]
pub struct BatchProcessor<B>
where
    B: DaemonDb,
{
    backend: Arc<B>,
}

#[derive(Debug, Clone)]
pub struct Cursor<B, C>
where
    B: DaemonDb,
    C: Column<B>,
{
    db_cursor: B::Cursor,
    column: PhantomData<C>,
    backend: PhantomData<B>,
}

#[derive(Debug, Clone)]
pub struct LedgerColumn<B, C>
where
    B: DaemonDb,
    C: Column<B>,
{
    backend: Arc<B>,
    column: PhantomData<C>,
}

#[derive(Debug)]
pub struct WriteBatch<B>
where
    B: DaemonDb,
{
    write_batch: B::WriteBatch,
    backend: PhantomData<B>,
    map: HashMap<&'static str, B::ColumnFamily>,
}

impl<B> Database<B>
where
    B: DaemonDb,
{
    pub fn open(path: &Path) -> Result<Self> {
        let backend = Arc::new(B::open(path)?);

        Ok(Database { backend })
    }

    pub fn destroy(path: &Path) -> Result<()> {
        B::destroy(path)?;

        Ok(())
    }

    pub fn remove(path: &Path) -> Result<()> {
        B::remove(path)?;

        Ok(())
    }

    pub fn repair(path: &Path) -> Result<()> {
        B::repair(path)?;

        Ok(())
    }

    pub fn get_bytes<C>(&self, key: C::Index) -> Result<Option<Vec<u8>>>
    where
        C: Column<B>,
    {
        self.backend
            .get_cf(self.cf_handle::<C>(), C::key(key).borrow())
    }

    pub fn put_bytes<C>(&self, key: C::Index, data: &[u8]) -> Result<()>
    where
        C: Column<B>,
    {
        self.backend
            .put_cf(self.cf_handle::<C>(), C::key(key).borrow(), data)
    }

    pub fn delete<C>(&self, key: C::Index) -> Result<()>
    where
        C: Column<B>,
    {
        self.backend
            .delete_cf(self.cf_handle::<C>(), C::key(key).borrow())
    }

    pub fn get<C>(&self, key: C::Index) -> Result<Option<C::Type>>
    where
        C: TypedColumn<B>,
    {
        if let Some(serialized_value) = self
            .backend
            .get_cf(self.cf_handle::<C>(), C::key(key).borrow())?
        {
            let value = deserialize(&serialized_value)?;

            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub fn put<C>(&self, key: C::Index, value: &C::Type) -> Result<()>
    where
        C: TypedColumn<B>,
    {
        let serialized_value = serialize(value)?;

        self.backend.put_cf(
            self.cf_handle::<C>(),
            C::key(key).borrow(),
            &serialized_value,
        )
    }

    pub fn cursor<C>(&self) -> Result<Cursor<B, C>>
    where
        C: Column<B>,
    {
        let db_cursor = self.backend.raw_iterator_cf(self.cf_handle::<C>())?;

        Ok(Cursor {
            db_cursor,
            column: PhantomData,
            backend: PhantomData,
        })
    }

    pub fn iter<C>(
        &self,
        start_from: Option<C::Index>,
    ) -> Result<impl Iterator<Item = (C::Index, Box<[u8]>)>>
    where
        C: Column<B>,
    {
        let iter = {
            if let Some(index) = start_from {
                let key = C::key(index);
                self.backend
                    .iterator_cf(self.cf_handle::<C>(), Some(key.borrow()))?
            } else {
                self.backend.iterator_cf(self.cf_handle::<C>(), None)?
            }
        };

        Ok(iter.map(|(key, value)| (C::index(&key), value)))
    }

    #[inline]
    pub fn cf_handle<C>(&self) -> B::ColumnFamily
    where
        C: Column<B>,
    {
        self.backend.cf_handle(C::NAME).clone()
    }

    pub fn column<C>(&self) -> LedgerColumn<B, C>
    where
        C: Column<B>,
    {
        LedgerColumn {
            backend: Arc::clone(&self.backend),
            column: PhantomData,
        }
    }

    // Note this returns an object that can be used to directly write to multiple column families.
    // This circumvents the synchronization around APIs that in BlockBufferPool that use
    // block_buffer_pool.batch_processor, so this API should only be used if the caller is sure they
    // are writing to data in columns that will not be corrupted by any simultaneous block_buffer_pool
    // operations.
    pub unsafe fn batch_processor(&self) -> BatchProcessor<B> {
        BatchProcessor {
            backend: Arc::clone(&self.backend),
        }
    }
}

impl<B> BatchProcessor<B>
where
    B: DaemonDb,
{
    pub fn batch(&mut self) -> Result<WriteBatch<B>> {
        let db_write_batch = self.backend.batch()?;
        let map = self
            .backend
            .columns()
            .into_iter()
            .map(|desc| (desc, self.backend.cf_handle(desc)))
            .collect();

        Ok(WriteBatch {
            write_batch: db_write_batch,
            backend: PhantomData,
            map,
        })
    }

    pub fn write(&mut self, batch: WriteBatch<B>) -> Result<()> {
        self.backend.write(batch.write_batch)
    }
}

impl<B, C> Cursor<B, C>
where
    B: DaemonDb,
    C: Column<B>,
{
    pub fn valid(&self) -> bool {
        self.db_cursor.valid()
    }

    pub fn seek(&mut self, key: C::Index) {
        self.db_cursor.seek(C::key(key).borrow());
    }

    pub fn seek_to_first(&mut self) {
        self.db_cursor.seek_to_first();
    }

    pub fn next(&mut self) {
        self.db_cursor.next();
    }

    pub fn key(&self) -> Option<C::Index> {
        if let Some(key) = self.db_cursor.key() {
            Some(C::index(key.borrow()))
        } else {
            None
        }
    }

    pub fn value_bytes(&self) -> Option<Vec<u8>> {
        self.db_cursor.value()
    }
}

impl<B, C> Cursor<B, C>
where
    B: DaemonDb,
    C: TypedColumn<B>,
{
    pub fn value(&self) -> Option<C::Type> {
        if let Some(bytes) = self.db_cursor.value() {
            let value = deserialize(&bytes).ok()?;
            Some(value)
        } else {
            None
        }
    }
}

impl<B, C> LedgerColumn<B, C>
where
    B: DaemonDb,
    C: Column<B>,
{
    pub fn get_bytes(&self, key: C::Index) -> Result<Option<Vec<u8>>> {
        self.backend.get_cf(self.handle(), C::key(key).borrow())
    }

    pub fn cursor(&self) -> Result<Cursor<B, C>> {
        let db_cursor = self.backend.raw_iterator_cf(self.handle())?;

        Ok(Cursor {
            db_cursor,
            column: PhantomData,
            backend: PhantomData,
        })
    }

    pub fn iter(
        &self,
        start_from: Option<C::Index>,
    ) -> Result<impl Iterator<Item = (C::Index, Box<[u8]>)>> {
        let iter = {
            if let Some(index) = start_from {
                let key = C::key(index);
                self.backend
                    .iterator_cf(self.handle(), Some(key.borrow()))?
            } else {
                self.backend.iterator_cf(self.handle(), None)?
            }
        };

        Ok(iter.map(|(key, value)| (C::index(&key), value)))
    }

    #[inline]
    pub fn handle(&self) -> B::ColumnFamily {
        self.backend.cf_handle(C::NAME).clone()
    }

    pub fn is_empty(&self) -> Result<bool> {
        let mut cursor = self.cursor()?;
        cursor.seek_to_first();
        Ok(!cursor.valid())
    }

    pub fn put_bytes(&self, key: C::Index, value: &[u8]) -> Result<()> {
        self.backend
            .put_cf(self.handle(), C::key(key).borrow(), value)
    }

    pub fn delete(&self, key: C::Index) -> Result<()> {
        self.backend.delete_cf(self.handle(), C::key(key).borrow())
    }
}

impl<B, C> LedgerColumn<B, C>
where
    B: DaemonDb,
    C: TypedColumn<B>,
{
    pub fn get(&self, key: C::Index) -> Result<Option<C::Type>> {
        if let Some(serialized_value) = self.backend.get_cf(self.handle(), C::key(key).borrow())? {
            let value = deserialize(&serialized_value)?;

            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub fn put(&self, key: C::Index, value: &C::Type) -> Result<()> {
        let serialized_value = serialize(value)?;

        self.backend
            .put_cf(self.handle(), C::key(key).borrow(), &serialized_value)
    }
}

impl<B> WriteBatch<B>
where
    B: DaemonDb,
{
    pub fn put_bytes<C: Column<B>>(&mut self, key: C::Index, bytes: &[u8]) -> Result<()> {
        self.write_batch
            .put_cf(self.get_cf::<C>(), C::key(key).borrow(), bytes)
    }

    pub fn delete<C: Column<B>>(&mut self, key: C::Index) -> Result<()> {
        self.write_batch
            .delete_cf(self.get_cf::<C>(), C::key(key).borrow())
    }

    pub fn put<C: TypedColumn<B>>(&mut self, key: C::Index, value: &C::Type) -> Result<()> {
        let serialized_value = serialize(&value)?;
        self.write_batch
            .put_cf(self.get_cf::<C>(), C::key(key).borrow(), &serialized_value)
    }

    #[inline]
    fn get_cf<C: Column<B>>(&self) -> B::ColumnFamily {
        self.map[C::NAME].clone()
    }
}
