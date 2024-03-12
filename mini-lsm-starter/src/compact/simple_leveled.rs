use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // l0 compaction
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot
                    .levels
                    .first()
                    .unwrap_or(&(0, Vec::new()))
                    .1
                    .clone(),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        }

        // index: [0, 1, 2, 3...]
        // level: [1, 2, 3, 4...]
        for i in 2..=self.options.max_levels {
            let (lower_level_idx, upper_level_idx) = (i - 1, i - 2);
            let (lower_level, upper_level) = (i, i - 1);

            let lower_level_sst = snapshot
                .levels
                .get(lower_level_idx)
                .unwrap_or(&(0, Vec::new()))
                .1
                .clone();
            let upper_level_sst = snapshot
                .levels
                .get(upper_level_idx)
                .unwrap_or(&(0, Vec::new()))
                .1
                .clone();

            let (lower_level_sst_count, upper_level_sst_count) =
                (lower_level_sst.len(), upper_level_sst.len());

            if upper_level_sst_count == 0 {
                continue;
            }

            let ratio = 100 * lower_level_sst_count / upper_level_sst_count;
            if ratio < self.options.size_ratio_percent {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(upper_level),
                    upper_level_sst_ids: upper_level_sst,
                    lower_level,
                    lower_level_sst_ids: lower_level_sst,
                    is_lower_level_bottom_level: i == self.options.max_levels,
                });
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        compacted_sst_ids: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut ssts_need_remove = Vec::new();

        if let Some(upper_level) = task.upper_level {
            ssts_need_remove.extend(snapshot.levels[upper_level - 1].1.clone());
            snapshot.levels[upper_level - 1].1.clear();
        } else {
            // l0 compaction
            ssts_need_remove.extend(snapshot.l0_sstables.clone());
            snapshot.l0_sstables.clear();
        }

        ssts_need_remove.extend(snapshot.levels[task.lower_level - 1].1.clone());
        snapshot.levels[task.lower_level - 1].1 = compacted_sst_ids.to_vec();

        (snapshot, ssts_need_remove)
    }
}
