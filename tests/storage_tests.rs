use minidist_rs::storage_init::init_table;
use minidist_rs::storage_load::load_table;
use minidist_rs::storage_schema::{ColumnType, parse_schema_file};
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

fn tmp_dir(prefix: &str) -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut path = std::env::temp_dir();
    path.push(format!("minidist-test-{}-{}", prefix, n));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn parses_sales_schema_file() {
    let contents = SALES_SSF;
    let cols = parse_schema_file(&contents).expect("schema should parse");
    assert_eq!(cols.len(), 3);
    assert!(cols[0].is_key);
    assert_eq!(cols[0].name, "id");
    assert!(matches!(cols[0].col_type, ColumnType::Int64));
    assert_eq!(cols[1].name, "region");
    assert!(matches!(cols[1].col_type, ColumnType::String));
    assert_eq!(cols[2].name, "amount");
    assert!(matches!(cols[2].col_type, ColumnType::Float64));
}

#[test]
fn storage_init_writes_layout() {
    let tmp = tmp_dir("init");
    let schema_path = tmp.join("sales.ssf");
    fs::write(&schema_path, SALES_SSF).unwrap();
    init_table(&tmp, &schema_path).expect("init succeeds");

    let schema_path = tmp.join("_schema.ssf");
    let table_txt_path = tmp.join("_table.txt");
    assert!(schema_path.exists());
    assert!(table_txt_path.exists());

    let copied = fs::read_to_string(schema_path).unwrap();
    assert_eq!(SALES_SSF, copied);

    let expected_table_txt = "\
version=1
block_rows=65536
segment_target_rows=1000000
endianness=little
";
    let table_txt = fs::read_to_string(table_txt_path).unwrap();
    assert_eq!(expected_table_txt, table_txt);
}

#[test]
fn storage_load_writes_segments() {
    let tmp = tmp_dir("load");
    let schema_path = tmp.join("sales.ssf");
    let csv_path = tmp.join("sales.csv");
    fs::write(&schema_path, SALES_SSF).unwrap();
    fs::write(&csv_path, SALES_CSV).unwrap();
    let schema = parse_schema_file(SALES_SSF).unwrap();
    load_table(&tmp, &csv_path, &"id".to_string(), 2, &schema).expect("load succeeds");

    // ids are sorted, so first two rows go to seg-000000
    let seg0 = tmp.join("seg-000000");
    let seg1 = tmp.join("seg-000001");
    assert!(seg0.is_dir());
    assert!(seg1.is_dir());

    let ids0 = read_int64s(seg0.join("id.bin"));
    let ids1 = read_int64s(seg1.join("id.bin"));
    assert_eq!(ids0, vec![1, 2]);
    assert_eq!(ids1, vec![3, 4]);

    let regions0 = read_strings(seg0.join("region.bin"));
    let regions1 = read_strings(seg1.join("region.bin"));
    assert_eq!(regions0, vec!["EU".to_string(), "US".to_string()]);
    assert_eq!(regions1, vec!["EU".to_string(), "APAC".to_string()]);

    let amounts0 = read_f64s(seg0.join("amount.bin"));
    let amounts1 = read_f64s(seg1.join("amount.bin"));
    assert_eq!(amounts0, vec![100.0, 200.0]);
    assert_eq!(amounts1, vec![50.0, 300.0]);
}

fn read_int64s(path: PathBuf) -> Vec<i64> {
    let mut f = fs::File::open(path).unwrap();
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).unwrap();
    let mut res = Vec::new();
    let mut i = 0;
    while i + 9 <= buf.len() {
        if buf[i] == 0 {
            i += 1;
            continue;
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&buf[i + 1..i + 9]);
        res.push(i64::from_le_bytes(bytes));
        i += 9;
    }
    res
}

fn read_f64s(path: PathBuf) -> Vec<f64> {
    let mut f = fs::File::open(path).unwrap();
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).unwrap();
    let mut res = Vec::new();
    let mut i = 0;
    while i + 9 <= buf.len() {
        if buf[i] == 0 {
            i += 1;
            continue;
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&buf[i + 1..i + 9]);
        res.push(f64::from_le_bytes(bytes));
        i += 9;
    }
    res
}

fn read_strings(path: PathBuf) -> Vec<String> {
    let mut f = fs::File::open(path).unwrap();
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).unwrap();
    let mut res = Vec::new();
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == 0 {
            i += 1;
            continue;
        }
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&buf[i + 1..i + 5]);
        let len = u32::from_le_bytes(len_bytes) as usize;
        let start = i + 5;
        let end = start + len;
        let s = std::str::from_utf8(&buf[start..end]).unwrap().to_string();
        res.push(s);
        i = end;
    }
    res
}

const SALES_SSF: &str = r#"id: int64 key
region: string
amount: float64
"#;

const SALES_CSV: &str = r#"id,region,amount
1,EU,100
2,US,200
3,EU,50
4,APAC,300
"#;
