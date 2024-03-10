use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyBytes;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::default(),
            last_key: Vec::default(),
            data: Vec::default(),
            meta: Vec::default(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.builder.add(key, value) {
            self.update_first_and_last_key(key);
            return;
        }

        self.save_block();

        assert!(self.builder.add(key, value));
        self.update_first_and_last_key(key);
    }

    fn update_first_and_last_key(&mut self, key: KeySlice) {
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec().into_inner();
        }

        self.last_key = key.to_key_vec().into_inner();
    }

    /// store current block and create new block builder
    fn save_block(&mut self) {
        let block_builder = BlockBuilder::new(self.block_size);
        let old_builder = std::mem::replace(&mut self.builder, block_builder);

        let block = old_builder.build();
        let block_meta = BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::from(self.first_key.clone())),
            last_key: KeyBytes::from_bytes(Bytes::from(self.last_key.clone())),
        };

        self.data.extend_from_slice(block.encode().as_ref());
        self.meta.push(block_meta);
        self.first_key.clear();
        self.last_key.clear();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // flush release data to a block
        if !self.builder.is_empty() {
            self.save_block();
        }

        // build SSTable in memory
        let mut sst_buf = self.data;
        let block_meta_offset = sst_buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut sst_buf);
        sst_buf.extend_from_slice(&(block_meta_offset as u32).to_le_bytes()[..]);

        // write memory SSTable to disk
        let file = FileObject::create(path.as_ref(), sst_buf)?;

        let ss_table = SsTable {
            file,
            block_meta_offset,
            id,
            block_cache,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            bloom: None,
            max_ts: 0,
        };

        Ok(ss_table)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
