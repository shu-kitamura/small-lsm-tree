use small_lsm_tree::LsmTree;

fn main() {
    // limit=10 で LsmTree を生成
    let mut lsm: LsmTree = match LsmTree::new(10) {
        Ok(lsm) => lsm,
        Err(e) => return eprintln!("{e}"),
    };

    // k1~11 を put する
    // limit を超えるので、メモリのデータがディスクに書き込まれる
    for i in 1..12 {
        let timestamp = chrono::Local::now().timestamp();
        let key: String = format!("key{i}");
        let value: String = format!("value{i}_{timestamp}");
        if let Err(e) = lsm.put(&key, &value) {
            eprintln!("{e}");
        };
    }

    // k1~11 を get する
    for i in 1..12 {
        let key: String = format!("key{i}");
        println!("{:?}", lsm.get(&key));
    }
}