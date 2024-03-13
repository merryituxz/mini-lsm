use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        let snapshot = snapshot.clone();

        // precondition
        if snapshot.levels.len() <= self.options.num_tiers {
            return None;
        }

        // triggered by space amplification ratio
        let all_levels_size = snapshot
            .levels
            .iter()
            .map(|level| level.1.len())
            .sum::<usize>();
        let last_level_size = snapshot.levels.last().expect("no level in state").1.len();
        let ratio = 100 * (all_levels_size - last_level_size) / last_level_size;
        if ratio >= self.options.max_size_amplification_percent {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels,
                bottom_tier_included: true,
            });
        }

        // size ratio
        let mut prev_size = 0;
        for i in 1..snapshot.levels.len() {
            prev_size += snapshot.levels[i - 1].1.len();
            let ratio = 100 * prev_size / snapshot.levels[i].1.len();
            if ratio >= 100 * self.options.size_ratio && i + 1 > self.options.min_merge_width {
                return Some(TieredCompactionTask {
                    tiers: snapshot
                        .levels
                        .iter()
                        .take(i + 1)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included: i == snapshot.levels.len() - 1,
                });
            }
        }

        // reduce sorted runs
        let reduce_count = snapshot.levels.len() - self.options.num_tiers + 2;
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(reduce_count)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: reduce_count == snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        compacted_sst_ids: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut ssts_need_remove: Vec<usize> = Vec::new();

        for (_, tier) in task.tiers.iter() {
            ssts_need_remove.extend(tier.clone());
        }

        snapshot.levels.retain(|tier| !task.tiers.contains(tier));

        snapshot
            .levels
            .insert(0, (compacted_sst_ids[0], compacted_sst_ids.to_vec()));

        (snapshot, ssts_need_remove)
    }
}
