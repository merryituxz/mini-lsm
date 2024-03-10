mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let block_size = self.data.len() + 2 * self.offsets.len() + 2;
        let mut block: Vec<u8> = Vec::with_capacity(block_size);

        self.data.iter().for_each(|v| block.push(*v));
        self.offsets
            .iter()
            .for_each(|v| block.extend_from_slice(&v.to_le_bytes()[..]));
        block.extend_from_slice(&(self.offsets.len() as u16).to_le_bytes()[..]);

        Bytes::from(block)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        assert!(data.len() >= 2, "invalid block format");

        let num_of_elements = (&data[data.len() - 2..]).get_u16_le();

        let (offset_begin, offset_end) = (
            data.len() - 2 - 2 * num_of_elements as usize,
            data.len() - 2,
        );
        let mut offsets: Vec<u16> = Vec::with_capacity(num_of_elements as usize);
        for i in (offset_begin..offset_end).step_by(2) {
            let offset = (&data[i..i + 2]).get_u16_le();
            offsets.push(offset);
        }

        let data = Vec::from(&data[..offset_begin]);

        Self { data, offsets }
    }
}
