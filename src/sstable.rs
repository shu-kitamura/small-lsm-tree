use std::{collections::{BTreeMap, HashMap}, fs::File, io::{BufReader, BufWriter, Error, Read, Seek, Write}, path::PathBuf};


pub struct SSTable {
    path: PathBuf,
    index: HashMap<String, usize>
}

impl SSTable {
    pub fn create(data_dir: &PathBuf, memtable: &BTreeMap<String, String>, timestamp: &str) -> Result<Self, Error> {
        let mut filepath: PathBuf = data_dir.clone();
        filepath.push(format!("{}.dat", timestamp));

        let mut writer: BufWriter<File> = match File::create(&filepath) {
            Ok(f) => BufWriter::new(f),
            Err(e) => return Err(e)
        };

        let mut pointer: usize = 0;
        let mut index: HashMap<String, usize> = HashMap::new();
        for (k, v) in memtable.iter() {
            index.insert(k.to_string(), pointer);
            pointer = write_key_value(&mut writer, k, v)?;
        }

        Ok(SSTable {
            path: filepath,
            index
        })
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, Error>{
        let pointer: usize = match self.index.get(key) {
            Some(p) => *p,
            None => return Ok(None)
        };

        let mut reader: BufReader<File> = match File::open(&self.path) {
            Ok(f) => BufReader::new(f),
            Err(e) => return Err(e)
        };

        let (_, value) = read_key_value(&mut reader, pointer)?;
        Ok(Some(value))
    }
}

pub fn write_key_value(buf_writer: &mut BufWriter<File>, key: &str, value: &str) -> Result<usize, Error>{
    // key, value を byte 列に変換する  
    let key_bytes: Vec<u8> = [&key.len().to_be_bytes(), key.as_bytes()].concat();
    let value_bytes: Vec<u8> = [&value.len().to_be_bytes(), value.as_bytes()].concat();
    let bytes: Vec<u8> = [key_bytes, value_bytes].concat();

    buf_writer.write(&bytes)
}

pub fn read_key_value(buf_reader: &mut BufReader<File>, offset: usize) -> Result<(String, String), Error> {
    buf_reader.seek(std::io::SeekFrom::Start(offset as u64))?;

    // key の長さを read する
    let mut bytes:[u8; 8]  = [0; 8];
    let key_length: usize = match buf_reader.read(&mut bytes) {
        Ok(_) => usize::from_be_bytes(bytes),
        Err(e) => return Err(e)
    };

    // key を read する
    let mut key_bytes: Vec<u8> = vec![0; key_length];
    let key: String = match buf_reader.read(&mut key_bytes) {
        Ok(_) => String::from_utf8(key_bytes).unwrap(),
        Err(e) => return Err(e)
    };

    // value の長さを read する
    let mut bytes:[u8; 8]  = [0; 8];
    let value_length: usize = match buf_reader.read(&mut bytes) {
        Ok(_) => usize::from_be_bytes(bytes),
        Err(e) => return Err(e)
    };

    // value を read する  
    let mut value_bytes: Vec<u8> = vec![0; value_length];
    let value: String = match buf_reader.read(&mut value_bytes) {
        Ok(_) => String::from_utf8(value_bytes).unwrap(),
        Err(e) => return Err(e)
    };

    Ok((key, value))
}