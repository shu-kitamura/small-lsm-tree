use std::{collections::BTreeMap, io::Error, path::PathBuf};
use crate::sstable::SSTable;

struct LsmTree {
    data_dir: PathBuf,
    memtable: BTreeMap<String, String>,
    limit: usize,
    sstable_list: Vec<SSTable>,
}

impl LsmTree {
    pub fn new(limit: usize) -> Self {
        LsmTree {
            data_dir: PathBuf::from(".data"),
            memtable: BTreeMap::new(),
            limit,
            sstable_list: Vec::new()
        }
    }

    pub fn put(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.memtable.insert(key.to_string(), value.to_string());

        if self.limit < self.memtable.len() {
            self.flush()?;
        }

        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.put(key, "__delete flag__")
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>, Error> {
        // Memtable から get する
        if let Some(value) = self.memtable.get(key) {
            return Ok(Some(value.to_string()))
        }

        // SSTable から get する
        for sstable in self.sstable_list.iter().rev() {
            if let Some(value) = sstable.get(key)? {
                return Ok(Some(value))
            }
        }

        Ok(None)
    }        

    pub fn flush(&mut self) -> Result<(), Error> {
        let timestamp: i64 = chrono::Local::now().timestamp();
        match SSTable::create(&self.data_dir, &self.memtable, &timestamp.to_string()) {
            Ok(sst) => self.sstable_list.push(sst),
            Err(e) => return Err(e)
        };

        self.memtable.clear();
        Ok(())
    }
}