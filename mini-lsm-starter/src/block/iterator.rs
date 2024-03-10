use bytes::Buf;
use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();

        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_key(key);

        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let entry_start = 0;
        let data = &self.block.data;

        // decode entry key
        let key_len = (&data[entry_start..entry_start + 2]).get_u16_le();
        let key_start = entry_start + 2;
        self.key = KeyVec::from_vec(data[key_start..key_start + key_len as usize].to_vec());

        // decode entry value
        let value_len_start = key_start + key_len as usize;
        let value_len = (&data[value_len_start..value_len_start + 2]).get_u16_le();
        let value_start = value_len_start + 2;
        self.value_range = (value_start, value_start + value_len as usize);

        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if !self.is_valid() {
            return;
        }

        let entry_start = self.value_range.1;
        let data = &self.block.data;

        // already iterate to the end
        if entry_start >= data.len() {
            self.key.clear();
            return;
        }

        let key_len = (&data[entry_start..entry_start + 2]).get_u16_le();
        let key_start = entry_start + 2;
        self.key = KeyVec::from_vec(data[key_start..key_start + key_len as usize].to_vec());

        let value_len_start = key_start + key_len as usize;
        let value_len = (&data[value_len_start..value_len_start + 2]).get_u16_le();
        let value_start = value_len_start + 2;
        self.value_range = (value_start, value_start + value_len as usize);

        // update current entry's idx
        self.idx += 1;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    /// TODO(merryituxz): may can use binary search?
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();

        while self.is_valid() && self.key.as_key_slice() < key {
            self.next();
        }
    }
}
