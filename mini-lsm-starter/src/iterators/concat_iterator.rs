use std::sync::Arc;

use anyhow::{bail, Result};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    current_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                current_sst_idx: 0,
                sstables,
            });
        }

        let current = SsTableIterator::create_and_seek_to_first(Arc::clone(&sstables[0]))?;

        Ok(Self {
            current: Some(current),
            current_sst_idx: 0,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        for (idx, sst) in sstables.iter().enumerate() {
            if idx == 0 && key.raw_ref() <= sst.first_key().raw_ref() {
                let current = SsTableIterator::create_and_seek_to_first(Arc::clone(sst))?;
                return Ok(Self {
                    current: Some(current),
                    current_sst_idx: idx,
                    sstables,
                });
            }

            if sst.may_contain_key(key.raw_ref()) {
                let current = SsTableIterator::create_and_seek_to_key(Arc::clone(sst), key)?;
                return Ok(Self {
                    current: Some(current),
                    current_sst_idx: idx,
                    sstables,
                });
            }
        }

        Ok(Self {
            current: None,
            current_sst_idx: 0,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        if let Some(current) = &self.current {
            return current.value();
        }

        &[]
    }

    fn key(&self) -> KeySlice {
        if let Some(current) = &self.current {
            return current.key();
        }

        KeySlice::default()
    }

    fn is_valid(&self) -> bool {
        if let Some(current) = &self.current {
            return current.is_valid();
        }

        false
    }

    fn next(&mut self) -> Result<()> {
        if self.current_sst_idx >= self.sstables.len() {
            bail!("SstConcatIterator already iterate to the end");
        }

        if let Some(current) = &mut self.current {
            current.next()?;

            if !current.is_valid() {
                self.current_sst_idx += 1;
                if self.current_sst_idx >= self.sstables.len() {
                    self.current = None;
                    return Ok(());
                }

                let next_iterator = SsTableIterator::create_and_seek_to_first(Arc::clone(
                    &self.sstables[self.current_sst_idx],
                ))?;
                self.current = Some(next_iterator);
            }

            return Ok(());
        }

        bail!("SstConcatIterator already iterate to the end");
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
