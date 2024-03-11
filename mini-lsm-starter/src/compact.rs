#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::Leveled(_) => {
                unimplemented!()
            }
            CompactionTask::Tiered(_) => {
                unimplemented!()
            }
            CompactionTask::Simple(_) => {
                unimplemented!()
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.compact_two_levels(l0_sstables, l1_sstables),
        }
    }

    // TODO(tomxczhang) bug here
    fn compact_two_levels(
        &self,
        upper_level: &Vec<usize>,
        lower_level: &Vec<usize>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };

        let mut sst_iters = Vec::with_capacity(upper_level.len());
        for sst_id in upper_level.iter() {
            let sst = snapshot
                .sstables
                .get(sst_id)
                .expect("no target sst found in upper level");
            let sst_iter = SsTableIterator::create_and_seek_to_first(Arc::clone(sst))?;
            sst_iters.push(Box::new(sst_iter));
        }
        let upper_merge_iter = MergeIterator::create(sst_iters);

        let mut lower_ssts = Vec::with_capacity(lower_level.len());
        for sst_id in lower_level.iter() {
            let sst = snapshot
                .sstables
                .get(sst_id)
                .expect("no target sst found in lower level");
            lower_ssts.push(Arc::clone(sst));
        }
        let lower_concat_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;

        let mut two_merge_iter = TwoMergeIterator::create(upper_merge_iter, lower_concat_iter)?;

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        let mut compacted_ssts = Vec::new();
        while two_merge_iter.is_valid() {
            let (key, value) = (two_merge_iter.key(), two_merge_iter.value());
            if value.is_empty() {
                two_merge_iter.next()?;
                continue;
            }

            sst_builder.add(key, value);

            if sst_builder.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                let sst_path = self.path_of_sst(sst_id);
                let sst =
                    sst_builder.build(sst_id, Some(Arc::clone(&self.block_cache)), sst_path)?;
                compacted_ssts.push(Arc::new(sst));
                sst_builder = SsTableBuilder::new(self.options.block_size);
            }

            two_merge_iter.next()?;
        }

        // lease data should collect to a new sst
        let sst_id = self.next_sst_id();
        let sst_path = self.path_of_sst(sst_id);
        let sst = sst_builder.build(sst_id, Some(Arc::clone(&self.block_cache)), sst_path)?;
        compacted_ssts.push(Arc::new(sst));

        Ok(compacted_ssts)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();

        let compaction_task = {
            let guard = self.state.read();

            CompactionTask::ForceFullCompaction {
                l0_sstables: guard.l0_sstables.clone(),
                l1_sstables: guard.levels.first().expect("l1 not exist").1.clone(),
            }
        };

        let compacted_ssts = self.compact(&compaction_task)?;
        let compacted_sst_ids = compacted_ssts.iter().map(|sst| sst.sst_id()).collect();

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            for old_sst_id in snapshot.l0_sstables.iter() {
                snapshot.sstables.remove(old_sst_id);
            }
            snapshot.l0_sstables.clear();

            for old_sst_id in snapshot.levels[0].1.iter() {
                snapshot.sstables.remove(old_sst_id);
            }
            snapshot.levels[0] = (1, compacted_sst_ids);

            for sst in compacted_ssts {
                snapshot.sstables.insert(sst.sst_id(), sst);
            }

            *guard = Arc::new(snapshot);
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let num_memtable = {
            let guard = self.state.read();
            guard.imm_memtables.len()
        };

        if num_memtable >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
