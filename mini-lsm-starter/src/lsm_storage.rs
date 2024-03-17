#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::cmp::max;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, CompactionTask, LeveledCompactionController,
    LeveledCompactionOptions, SimpleLeveledCompactionController, SimpleLeveledCompactionOptions,
    TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::mini_lsm_debug;
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        self.flush_notifier.send(()).ok();
        self.compaction_notifier.send(()).ok();

        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow!("compaction_thread join err: {:?}", e))?;
        }

        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow!("flush_thread join err: {:?}", e))?;
        }

        if self.inner.options.enable_wal {
            mini_lsm_debug!("WAL flush");
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        while {
            let guard = self.inner.state.read();
            !guard.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        fs::create_dir_all(path)?;

        let mut state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        // load MANIFEST
        let manifest_path = path.join("MANIFEST");
        let (manifest, manifest_records) = Manifest::recover(manifest_path)?;
        let mut sst_ids_set: HashSet<usize> = HashSet::new();
        let mut memtables = BTreeSet::new();
        for record in manifest_records.iter() {
            match record {
                ManifestRecord::Flush(sst_id) => {
                    sst_ids_set.insert(*sst_id);

                    if compaction_controller.flush_to_l0() {
                        state.l0_sstables.insert(0, *sst_id);
                    } else {
                        state.levels.insert(0, (*sst_id, vec![*sst_id]));
                    }
                    mini_lsm_debug!("MANIFEST flush {} to l0", *sst_id);
                }
                ManifestRecord::NewMemtable(id) => {
                    memtables.insert(*id);
                }
                ManifestRecord::Compaction(task, compacted_sst_ids) => {
                    let (new_state, ssts_need_remove) = compaction_controller
                        .apply_compaction_result(&state, task, compacted_sst_ids);
                    for sst_id in ssts_need_remove.iter() {
                        sst_ids_set.remove(sst_id);
                    }
                    for sst_id in compacted_sst_ids.iter() {
                        sst_ids_set.insert(*sst_id);
                    }
                    state = new_state;

                    match task {
                        CompactionTask::Leveled(_) => {
                            mini_lsm_debug!(
                                "MANIFEST Leveled {:?} to {:?}",
                                ssts_need_remove,
                                compacted_sst_ids
                            );
                        }
                        CompactionTask::Tiered(_) => {
                            mini_lsm_debug!(
                                "MANIFEST Tiered {:?} to {:?}",
                                ssts_need_remove,
                                compacted_sst_ids
                            );
                        }
                        CompactionTask::Simple(_) => {
                            mini_lsm_debug!(
                                "MANIFEST Simple {:?} to {:?}",
                                ssts_need_remove,
                                compacted_sst_ids
                            );
                        }
                        CompactionTask::ForceFullCompaction { .. } => {
                            mini_lsm_debug!(
                                "MANIFEST ForceFullCompaction {:?} to {:?}",
                                ssts_need_remove,
                                compacted_sst_ids
                            );
                        }
                    }
                }
            }
        }

        mini_lsm_debug!("MANIFEST recover l0: {:?}", state.l0_sstables);
        mini_lsm_debug!("MANIFEST recover levels: {:?}", state.levels);

        // recover sstable and block cache from MANIFEST
        let block_cache = Arc::new(BlockCache::new(1024));
        for sst_id in sst_ids_set.iter() {
            let sst_path = Self::path_of_sst_static(path, *sst_id);
            let sst_handler = FileObject::open(&sst_path)?;
            let sst = SsTable::open(*sst_id, Some(Arc::clone(&block_cache)), sst_handler)?;
            state.sstables.insert(*sst_id, Arc::new(sst));
        }

        mini_lsm_debug!("MANIFEST recover sstables: {:?}", state.sstables.keys());

        // next sst id = max{memtable id, sst id} + 1
        let mut next_sst_id = max(
            sst_ids_set.iter().max().unwrap_or(&0),
            memtables.iter().max().unwrap_or(&0),
        ) + 1;

        // recover or create WAL, and update manifest
        if options.enable_wal {
            state.memtable = Arc::new(MemTable::recover_from_wal(
                next_sst_id,
                Self::path_of_wal_static(path, next_sst_id),
            )?);

            for id in memtables {
                let imm_memtable =
                    MemTable::recover_from_wal(id, Self::path_of_wal_static(path, id))?;
                state.imm_memtables.insert(0, Arc::new(imm_memtable));
            }
        } else {
            state.memtable = Arc::new(MemTable::create(next_sst_id));
        }

        // add NewMemtable record and flush
        manifest.add_record_when_init(ManifestRecord::NewMemtable(next_sst_id))?;
        File::open(path)?.sync_all()?;

        next_sst_id += 1;

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::clone(&block_cache),
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        let guard = self.state.read();

        guard.memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // try search in memtable
        if let Some(value) = snapshot.memtable.get(key) {
            return if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(value))
            };
        }

        // try search in imm memtables
        for imm_memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = imm_memtable.get(key) {
                return if value.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(value))
                };
            }
        }

        // try search in l0 ssts
        let mut sst_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_id in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables.get(sst_id).unwrap_or_else(|| {
                panic!(
                    "target sst[{}] not found in l0: {:?}",
                    sst_id, snapshot.l0_sstables
                )
            });

            if !sst.may_contain_key(key) {
                continue;
            }

            let sst_iter = SsTableIterator::create_and_seek_to_key(
                Arc::clone(sst),
                KeySlice::from_slice(key),
            )?;
            sst_iters.push(Box::new(sst_iter));
        }

        let mut l0_merge_iter = MergeIterator::create(sst_iters);
        while l0_merge_iter.is_valid() && l0_merge_iter.key().raw_ref() <= key {
            if l0_merge_iter.key().raw_ref() == key {
                if l0_merge_iter.value().is_empty() {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(l0_merge_iter.value())));
            }

            l0_merge_iter.next()?;
        }

        // try search in lower level ssts
        let mut concat_iters = Vec::new();
        for (level, sst_ids) in snapshot.levels.iter() {
            let ssts = sst_ids
                .iter()
                .map(|id| {
                    snapshot
                        .sstables
                        .get(id)
                        .unwrap_or_else(|| panic!("target sst[{}] not found in l[{}]", id, level))
                })
                .map(Arc::clone)
                .collect();
            let concat_iter =
                SstConcatIterator::create_and_seek_to_key(ssts, KeySlice::from_slice(key))?;
            concat_iters.push(Box::new(concat_iter));
        }

        let mut lower_level_merge_iter = MergeIterator::create(concat_iters);
        while lower_level_merge_iter.is_valid() && lower_level_merge_iter.key().raw_ref() <= key {
            if lower_level_merge_iter.key().raw_ref() == key {
                if lower_level_merge_iter.value().is_empty() {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(lower_level_merge_iter.value())));
            }

            lower_level_merge_iter.next()?;
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let size_after_put = {
            let guard = self.state.read();
            guard.memtable.put(key, value)?;
            guard.memtable.approximate_size()
        };

        if size_after_put >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            self.force_freeze_memtable(&state_lock)?;
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;

        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable = if self.options.enable_wal {
            let id = self.next_sst_id();
            MemTable::create_with_wal(id, self.path_of_wal(id))?
        } else {
            MemTable::create(self.next_sst_id())
        };

        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();

        snapshot.imm_memtables.insert(0, snapshot.memtable);
        snapshot.memtable = Arc::new(memtable);

        if let Some(manifest) = &self.manifest {
            manifest.add_record(
                &state_lock_observer,
                ManifestRecord::NewMemtable(snapshot.memtable.id()),
            )?;
            self.sync_dir()?;
        }

        *guard = Arc::new(snapshot);

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };

        if snapshot.imm_memtables.is_empty() {
            return Ok(());
        }

        let oldest_memtable = snapshot.imm_memtables.last().unwrap();
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        oldest_memtable.flush(&mut sst_builder)?;

        let sst_id = self.next_sst_id();
        let sst_path = self.path_of_sst(sst_id);
        let sst = sst_builder.build(sst_id, Some(Arc::clone(&self.block_cache)), sst_path)?;
        self.sync_dir()?;

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }

            snapshot.sstables.insert(sst_id, Arc::new(sst));
            snapshot.imm_memtables.pop();

            *guard = Arc::new(snapshot);
        }

        if let Some(manifest) = &self.manifest {
            manifest.add_record(&state_lock, ManifestRecord::Flush(sst_id))?;
            self.sync_dir()?;
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // create memtable merge iterator
        let mut memtable_iters = Vec::with_capacity(1 + snapshot.imm_memtables.len());
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));

        for imm_memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(imm_memtable.scan(lower, upper)));
        }

        let memtable_merge_iter = MergeIterator::create(memtable_iters);

        // create l0 sst merge iterator
        let mut l0_iters = Vec::new();

        for sst_id in snapshot.l0_sstables.iter() {
            let sst = snapshot
                .sstables
                .get(sst_id)
                .unwrap_or_else(|| panic!("target sst[{}] not found in l0", sst_id));

            if !sst.range_overlap(map_bound(lower), map_bound(upper)) {
                continue;
            }

            let sst_iter = match lower {
                Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                    Arc::clone(sst),
                    KeySlice::from_slice(key),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        Arc::clone(sst),
                        KeySlice::from_slice(key),
                    )?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }

                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(Arc::clone(sst))?,
            };
            l0_iters.push(Box::new(sst_iter));
        }
        let l0_merge_iter = MergeIterator::create(l0_iters);

        // create lower levels sst merge-concat iterator
        let mut concat_iters = Vec::new();
        for (level, sst_ids) in snapshot.levels.iter() {
            let ssts = sst_ids
                .iter()
                .map(|id| {
                    snapshot
                        .sstables
                        .get(id)
                        .unwrap_or_else(|| panic!("target sst[{}] not found in l{}", id, level))
                })
                .map(Arc::clone)
                .collect();

            let concat_iter = match lower {
                Bound::Included(key) => {
                    SstConcatIterator::create_and_seek_to_key(ssts, KeySlice::from_slice(key))?
                }
                Bound::Excluded(key) => {
                    let mut iter =
                        SstConcatIterator::create_and_seek_to_key(ssts, KeySlice::from_slice(key))?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }

                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ssts)?,
            };

            concat_iters.push(Box::new(concat_iter));
        }
        let lower_level_merge_iter = MergeIterator::create(concat_iters);

        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(
                TwoMergeIterator::create(memtable_merge_iter, l0_merge_iter)?,
                lower_level_merge_iter,
            )?,
            upper,
        )?))
    }
}
