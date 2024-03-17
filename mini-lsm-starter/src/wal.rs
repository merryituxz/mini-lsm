#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::mini_lsm_debug;
use anyhow::Result;
use bytes::{Buf, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use serde::{Deserializer, Serialize, Serializer};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(path)?;

        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(path)?;

        let mut buf = Vec::with_capacity(file.metadata()?.len() as usize);
        file.read_to_end(&mut buf)?;

        let mut i = 0;
        while i < buf.len() {
            let key_len = (&buf[i..i + 4]).get_u32_le();
            i += 4;
            let key = &buf[i..i + key_len as usize];
            i += key_len as usize;

            let value_len = (&buf[i..i + 4]).get_u32_le();
            i += 4;
            let value = &buf[i..i + value_len as usize];
            i += value_len as usize;

            skiplist.insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
            mini_lsm_debug!(
                "WAL recover key: {:?}, value: {:?}",
                Bytes::copy_from_slice(key),
                Bytes::copy_from_slice(value)
            );
        }

        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut writer = self.file.lock();

        // key_len(u32) | key | value_len(u32) | value
        let record_len = 4 + key.len() + 4 + value.len();
        let mut buf: Vec<u8> = Vec::with_capacity(record_len);

        buf.extend_from_slice(&(key.len() as u32).to_le_bytes()[..]);
        buf.extend_from_slice(key);
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes()[..]);
        buf.extend_from_slice(value);

        writer.write_all(&buf)?;
        mini_lsm_debug!(
            "WAL put key: {:?}, value: {:?}",
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value)
        );
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut writer = self.file.lock();
        writer.get_mut().sync_all()?;

        Ok(())
    }
}
