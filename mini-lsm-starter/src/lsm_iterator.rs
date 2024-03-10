use anyhow::{bail, Result};
use bytes::Bytes;
use std::ops::Bound;

use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::mem_table::map_bound;
use crate::table::SsTableIterator;
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner =
TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<&[u8]>) -> Result<Self> {
        let mut iter = iter;
        while iter.is_valid() && iter.value().is_empty() {
            iter.next()?;
        }
        Ok(Self {
            inner: iter,
            upper: map_bound(upper),
        })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn is_valid(&self) -> bool {
        if !self.inner.is_valid() {
            return false;
        }

        match &self.upper {
            Bound::Included(bound) => self.key() <= bound.as_ref(),
            Bound::Excluded(bound) => self.key() < bound.as_ref(),
            Bound::Unbounded => true,
        }
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn value(&self) -> &[u8] {
        if self.has_errored || !self.is_valid() {
            panic!("inner iterator has error");
        }

        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_errored || !self.is_valid() {
            panic!("inner iterator has error");
        }

        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        if !self.has_errored {
            self.iter.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("inner iterator has error");
        }

        if self.is_valid() {
            if let Err(err) = self.iter.next() {
                self.has_errored = true;
                return Err(err);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
