pub(crate) mod bloom;
mod builder;
mod iterator;

use std::collections::Bound;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_metas: &[BlockMeta], buf: &mut Vec<u8>) {
        // -------------------------------------------------------------------------------
        // | first key len(u16) | last key len(u16) | offset(u32) | first key | last key |
        // -------------------------------------------------------------------------------
        for block_meta in block_metas.iter() {
            buf.extend_from_slice(&(block_meta.first_key.len() as u16).to_le_bytes()[..]);
            buf.extend_from_slice(&(block_meta.last_key.len() as u16).to_le_bytes()[..]);
            buf.extend_from_slice(&(block_meta.offset as u32).to_le_bytes()[..]);
            buf.extend_from_slice(block_meta.first_key.raw_ref());
            buf.extend_from_slice(block_meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: &[u8]) -> Vec<BlockMeta> {
        let mut block_metas = Vec::new();

        let mut i = 0;
        while i < buf.len() {
            let first_key_len = (&buf[i..i + 2]).get_u16_le();
            i += 2;
            let last_key_len = (&buf[i..i + 2]).get_u16_le();
            i += 2;
            let offset = (&buf[i..i + 4]).get_u32_le();
            i += 4;
            let first_key = &buf[i..i + first_key_len as usize];
            i += first_key_len as usize;
            let last_key = &buf[i..i + last_key_len as usize];
            i += last_key_len as usize;

            let first_key = KeyBytes::from_bytes(Bytes::from(first_key.to_owned()));
            let last_key = KeyBytes::from_bytes(Bytes::from(last_key.to_owned()));

            let block_meta = BlockMeta {
                offset: offset as usize,
                first_key,
                last_key,
            };
            block_metas.push(block_meta);
        }

        block_metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let sst_size = file.1;

        // decode block metas
        let block_meta_offset = file.read(sst_size - 4, 4)?;
        let block_meta_offset = (&block_meta_offset[..]).get_u32_le();
        let block_meta_bytes = file.read(
            block_meta_offset as u64,
            sst_size - 4 - block_meta_offset as u64,
        )?;
        let block_metas = BlockMeta::decode_block_meta(&block_meta_bytes);

        Ok(Self {
            file,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key: block_metas.first().unwrap().first_key.clone(),
            last_key: block_metas.last().unwrap().last_key.clone(),
            block_meta: block_metas,
            bloom: None,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if self.block_meta.is_empty() {
            bail!("no block in SSTable");
        }

        let block_count = self.block_meta.len();
        if block_idx >= block_count {
            bail!(
                "block index out of range, block_idx = {}, block_count = {}",
                block_idx,
                block_count
            );
        }

        let block_begin = self.block_meta[block_idx].offset;
        let block_len = if block_idx == block_count - 1 {
            self.block_meta_offset - block_begin
        } else {
            self.block_meta[block_idx + 1].offset - block_begin
        };

        let block_raw = self.file.read(block_begin as u64, block_len as u64)?;

        let block = Block::decode(&block_raw);

        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let cache_key = (self.id, block_idx);
            let block = block_cache
                .try_get_with(cache_key, || self.read_block(block_idx))
                .map_err(|e| anyhow!("read block cache failed, err: {}", e))?;

            Ok(block)
        } else {
            println!("block cache not enabled, read block directly");
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let pos = self
            .block_meta
            .binary_search_by(|meta| meta.first_key.as_key_slice().cmp(&key));

        match pos {
            Ok(idx) => idx,
            Err(idx) => {
                // idx out of range
                if idx == 0 {
                    return idx;
                }

                let prev_idx = idx - 1;
                if self.block_meta[prev_idx].last_key.as_key_slice() < key {
                    if idx == self.block_meta.len() {
                        idx - 1
                    } else {
                        idx
                    }
                } else {
                    prev_idx
                }
            }
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub fn range_overlap(&self, lower: Bound<Bytes>, upper: Bound<Bytes>) -> bool {
        match lower {
            Bound::Included(bound) => {
                if self.last_key.raw_ref() < bound.as_ref() {
                    return false;
                }
            }
            Bound::Excluded(bound) => {
                if self.last_key.raw_ref() <= bound.as_ref() {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }

        match upper {
            Bound::Included(bound) => {
                if self.first_key.raw_ref() > bound.as_ref() {
                    return false;
                }
            }
            Bound::Excluded(bound) => {
                if self.first_key.raw_ref() >= bound.as_ref() {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }

        true
    }

    pub fn may_contain_key(&self, key: &[u8]) -> bool {
        self.first_key.raw_ref() <= key && key <= self.last_key.raw_ref()
    }
}
