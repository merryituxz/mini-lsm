use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?;
        let block_iterator = BlockIterator::create_and_seek_to_first(block);

        Ok(Self {
            table,
            blk_iter: block_iterator,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block = self.table.read_block_cached(0)?;
        let block_iterator = BlockIterator::create_and_seek_to_first(block);

        self.blk_iter = block_iterator;
        self.blk_idx = 0;

        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let block_idx = table.find_block_idx(key);
        let block = table.read_block_cached(block_idx)?;
        let block_iterator = BlockIterator::create_and_seek_to_key(block, key);

        Ok(Self {
            table,
            blk_iter: block_iterator,
            blk_idx: block_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let block_idx = self.table.find_block_idx(key);
        let block = self.table.read_block_cached(block_idx)?;

        // try seek in current block
        self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        self.blk_idx = block_idx;

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        let block_count = self.table.block_meta.len();

        // already iterate all blocks
        if self.blk_idx >= block_count {
            return Ok(());
        }

        self.blk_iter.next();

        // current block iterate to the next, switch next block
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;

            // check is last block
            if self.blk_idx >= block_count {
                return Ok(());
            }

            // switch to iterate next block
            let block = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
