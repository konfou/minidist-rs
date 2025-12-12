use minidist_rs::minisql_parse::parse_sql;
use minidist_rs::rpc::{AggregateFn, Predicate};

#[test]
fn parses_example_query() {
    let sql = "SELECT region, SUM(amount)\nFROM sales\nWHERE amount > 100\nGROUP BY region;";
    let req = parse_sql(sql).expect("should parse");
    assert_eq!(req.table, "sales");
    assert_eq!(req.projections, vec!["region"]);
    assert_eq!(req.group_by, vec!["region"]);
    assert_eq!(req.aggregates.len(), 1);
    let agg = &req.aggregates[0];
    assert!(matches!(agg.func, AggregateFn::Sum));
    assert_eq!(agg.column.as_deref(), Some("amount"));
    assert_eq!(req.filters.len(), 1);
    let filt = &req.filters[0];
    assert_eq!(filt.column, "amount");
    assert!(matches!(filt.pred, Predicate::Gt));
}
