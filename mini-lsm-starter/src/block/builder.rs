use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::default(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // current size + key_len(2) + key(key.len()) + value_len(2) + value(value.len()) + offset record
        let size_after_add = self.current_size() + 2 + key.len() + 2 + value.len() + 2;

        // reach limit
        if !self.is_empty() && size_after_add >= self.block_size {
            return false;
        }

        let offset = self.data.len();

        // build entry
        let key_len = key.len() as u16;
        let value_len = value.len() as u16;
        let entry_size = 2 + key_len as usize + 2 + value_len as usize;

        let mut entry: Vec<u8> = Vec::with_capacity(entry_size);
        entry.extend_from_slice(&key_len.to_le_bytes()[..]);
        entry.extend_from_slice(key.raw_ref());
        entry.extend_from_slice(&value_len.to_le_bytes()[..]);
        entry.extend_from_slice(value);

        // add entry
        self.data.append(&mut entry);

        // add offset
        self.offsets.push(offset as u16);

        // init first key
        if self.first_key.is_empty() {
            self.first_key.append(key.raw_ref());
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.offsets.is_empty() && self.first_key.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    fn current_size(&self) -> usize {
        // total entry size + offset size + extra size(num of elements)
        self.data.len() + 2 * self.offsets.len() + 2
    }
}
