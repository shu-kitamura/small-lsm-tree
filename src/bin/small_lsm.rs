use std::{fs, path::PathBuf};

use small_lsm_tree::LsmTree;

fn main() {
    // limit=10 で LsmTree を生成
    let mut lsm: LsmTree = match LsmTree::new(10) {
        Ok(lsm) => lsm,
        Err(e) => return eprintln!("{e}"),
    };

    println!("--- put, delete, getの確認 ---");

    let key: &str = "test_key";
    let value: String = format!("test_value_{}", chrono::Local::now().timestamp());

    // put 実行
    if let Err(e) = lsm.put(key, &value) {
        eprintln!("{e}")
    };

    // get 実行 (put の結果を確認)
    match lsm.get(key) {
        Ok(opt) => println!("put後: {:?}", opt),
        Err(e) => eprintln!("{e}")
    };


    // delete 実行
    if let Err(e) = lsm.delete(key) {
        eprintln!("{e}")
    };

    // get 実行 (delete の結果を確認)
    match lsm.get(key) {
        Ok(opt) => println!("delete後: {:?}", opt),
        Err(e) => eprintln!("{e}")
    }

    println!("--- flush の確認 ---");
    let path: PathBuf = PathBuf::from("./data");

    println!("flush 前のファイル一覧");
    let files: fs::ReadDir = fs::read_dir(&path).unwrap();
    for result in files {
        println!("{:?}", result.unwrap());
    }

    // flush 実行
    if let Err(e) = lsm.flush() {
        eprintln!("{e}")
    };

    println!("flush 後のファイル一覧");
    let files: fs::ReadDir = fs::read_dir(&path).unwrap();
    for result in files {
        println!("{:?}", result.unwrap());
    }

    // flush 後、memtable が空になっていることを確認
    println!("{:?}", lsm.memtable);

    // SSTable から get できることを確認
    match lsm.get(key) {
        Ok(opt) => println!("{:?}", opt),
        Err(e) => eprintln!("{e}")
    }
}