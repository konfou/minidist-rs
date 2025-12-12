use minidist_rs::coordinator_merge::merge_partials;
use minidist_rs::minisql_parse::parse_sql;
use minidist_rs::storage_init::init_table;
use minidist_rs::storage_load::load_table;
use minidist_rs::storage_schema::parse_schema_file;
use minidist_rs::worker_exec::{WorkerContext, execute_query};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

fn tmp_dir(prefix: &str) -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut path = std::env::temp_dir();
    path.push(format!("minidist-query-{}-{}", prefix, n));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

fn build_sales_table() -> PathBuf {
    let dir = tmp_dir("table");
    let schema_path = dir.join("sales.ssf");
    let csv_path = dir.join("sales.csv");
    fs::write(&schema_path, SALES_SSF).unwrap();
    fs::write(&csv_path, SALES_CSV).unwrap();
    init_table(&dir, &schema_path).unwrap();
    let schema = parse_schema_file(SALES_SSF).unwrap();
    load_table(&dir, &csv_path, &"id".to_string(), 2, &schema).unwrap();
    dir
}

#[test]
fn query_group_by_region() {
    let table_dir = build_sales_table();
    let mut req =
        parse_sql("SELECT region, SUM(amount) FROM sales GROUP BY region;").expect("parse");
    req.table = table_dir.to_string_lossy().to_string();

    let partials = run_on_all_segments(&table_dir, 2, &req);
    let (merged, _, _, _) = merge_partials(&partials);

    let eu = merged.get("EU").expect("EU group");
    let us = merged.get("US").expect("US group");
    let apac = merged.get("APAC").expect("APAC group");

    assert_eq!(eu["SUM(amount)"].sum, 150.0);
    assert_eq!(eu["SUM(amount)"].count, 2);
    assert_eq!(us["SUM(amount)"].sum, 200.0);
    assert_eq!(apac["SUM(amount)"].sum, 300.0);
}

#[test]
fn query_filter_and_count() {
    let table_dir = build_sales_table();
    let mut req =
        parse_sql("SELECT COUNT(*), SUM(amount) FROM sales WHERE amount > 100;").expect("parse");
    req.table = table_dir.to_string_lossy().to_string();

    let partials = run_on_all_segments(&table_dir, 2, &req);
    let (merged, _, _, _) = merge_partials(&partials);
    let agg = merged.get("all").expect("all group");

    let count = agg.get("COUNT(*)").expect("count agg");
    let sum = agg.get("SUM(amount)").expect("sum agg");
    assert_eq!(count.count, 2);
    assert_eq!(sum.sum, 500.0);
}

#[test]
fn query_select_star_yields_group() {
    let table_dir = build_sales_table();
    let mut req = parse_sql("SELECT * FROM sales;").expect("parse");
    req.table = table_dir.to_string_lossy().to_string();

    let partials = run_on_all_segments(&table_dir, 2, &req);
    let (merged, _, _, _) = merge_partials(&partials);
    assert!(
        merged.contains_key("all"),
        "expected default group 'all' even without aggregates"
    );
    let state = merged["all"].get("COUNT(*)").expect("implicit count");
    assert_eq!(state.count, 4);
}

fn run_on_all_segments(
    table_dir: &PathBuf,
    segments: u32,
    req: &minidist_rs::rpc::QueryRequest,
) -> Vec<minidist_rs::rpc::PartialAggregate> {
    let mut partials = Vec::new();
    for segment in 0..segments {
        let ctx = WorkerContext {
            port: 0,
            table: table_dir.to_string_lossy().to_string(),
            segment,
        };
        partials.push(execute_query(&ctx, req.clone(), Instant::now()));
    }
    partials
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
