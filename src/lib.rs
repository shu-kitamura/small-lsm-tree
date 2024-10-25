mod sstable;

use std::{collections::BTreeMap, fs, io::Error, path::PathBuf};
use crate::sstable::SSTable;

pub struct LsmTree {
    pub data_dir: PathBuf,
    pub memtable: BTreeMap<String, String>,
    pub limit: usize,
    pub sstable_list: Vec<SSTable>,
}

impl LsmTree {
    pub fn new(limit: usize) -> Result<Self, Error> {
        let data_dir: PathBuf = PathBuf::from("./data");
        let sstable_list: Vec<SSTable> = load_sstable_files(&data_dir)?;
        Ok(LsmTree {
            data_dir,
            memtable: BTreeMap::new(),
            limit,
            sstable_list
        })
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

fn load_sstable_files(data_dir: &PathBuf) -> Result<Vec<SSTable>, Error> {
    let mut sstable_list: Vec<SSTable> = Vec::new();
    
    let files: fs::ReadDir = fs::read_dir(data_dir)?;
    for result in files {
        let data_file: PathBuf = match result {
            Ok(dir_entry) => dir_entry.path(),
            Err(e) => return Err(e)
        };
        let sstable: SSTable = SSTable::from_file(data_file)?;

        sstable_list.push(sstable);
    }
    Ok(sstable_list)
}