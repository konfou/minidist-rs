use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::minisql_eval::{
    ReadError, ReaderState, apply_agg, format_scalar, init_reader, read_value, row_matches,
};
use crate::rpc::{AggregateState, GroupMap, PartialAggregate, QueryRequest, ScalarValue};
use crate::storage_schema::ColumnDef;

#[derive(Debug, Clone)]
pub struct WorkerContext {
    pub port: u16,
    pub table: String,
    pub segment: u32,
}

pub fn execute_query(
    ctx: &WorkerContext,
    req: QueryRequest,
    started: std::time::Instant,
) -> PartialAggregate {
    // Guard against missing/invalid schema to avoid spinning forever.
    let mut rows_scanned = 0u64;
    let mut groups: GroupMap = HashMap::new();

    let schema = load_schema(&ctx.table);
    let def_map: HashMap<String, ColumnDef> =
        schema.iter().map(|c| (c.name.clone(), c.clone())).collect();
    if schema.is_empty() {
        return PartialAggregate {
            worker_port: ctx.port,
            segment: ctx.segment,
            rows_scanned: 0,
            segments_skipped: 1,
            exec_ms: started.elapsed().as_millis() as u64,
            groups,
        };
    }
    let segment_dir = segment_path(&ctx.table, ctx.segment);

    let mut needed_cols: HashSet<String> = needed_columns(&req);
    if needed_cols.is_empty() {
        if let Some(first) = schema.first() {
            needed_cols.insert(first.name.clone());
        }
    }
    let mut readers = match open_readers(&segment_dir, &def_map, &needed_cols) {
        Ok(r) => r,
        Err(_) => {
            return PartialAggregate {
                worker_port: ctx.port,
                segment: ctx.segment,
                rows_scanned: 0,
                segments_skipped: 1,
                exec_ms: started.elapsed().as_millis() as u64,
                groups,
            };
        }
    };
    // Zone map pruning: if filters cannot match based on min/max, skip segment
    if let Some(min_max) = compute_min_max(&segment_dir, &def_map, &req.filters) {
        if should_skip(&req.filters, &min_max) {
            return PartialAggregate {
                worker_port: ctx.port,
                segment: ctx.segment,
                rows_scanned: 0,
                segments_skipped: 1,
                exec_ms: started.elapsed().as_millis() as u64,
                groups,
            };
        }
    }
    if readers.is_empty() {
        return PartialAggregate {
            worker_port: ctx.port,
            segment: ctx.segment,
            rows_scanned: 0,
            segments_skipped: 1,
            exec_ms: started.elapsed().as_millis() as u64,
            groups,
        };
    }

    loop {
        let mut row_values: HashMap<String, Option<ScalarValue>> = HashMap::new();

        let mut eof = false;
        for (name, reader_state) in readers.iter_mut() {
            if !needed_cols.contains(name) {
                continue;
            }
            let def = def_map.get(name).unwrap();
            match read_value(reader_state, def) {
                Ok(v) => {
                    row_values.insert(name.clone(), v);
                }
                Err(ReadError::Eof) | Err(ReadError::Io) => {
                    eof = true;
                    break;
                }
            }
        }

        if eof {
            break;
        }

        rows_scanned += 1;

        if !row_matches(&req.filters, &row_values) {
            continue;
        }

        let gkey = if req.group_by.is_empty() {
            "all".to_string()
        } else {
            let mut parts = Vec::new();
            for gcol in &req.group_by {
                let val = row_values.get(gcol).and_then(|v| v.clone());
                parts.push(format_scalar(&val));
            }
            parts.join("|")
        };

        let agg_map = groups.entry(gkey).or_default();
        if req.aggregates.is_empty() {
            // Implicit COUNT(*) for projection-only queries so users get a visible result.
            let state = agg_map
                .entry("COUNT(*)".to_string())
                .or_insert_with(AggregateState::default);
            state.count += 1;
        } else {
            for agg in &req.aggregates {
                let state = agg_map
                    .entry(agg.output_name.clone())
                    .or_insert_with(AggregateState::default);
                apply_agg(state, agg, &row_values);
            }
        }
    }

    PartialAggregate {
        worker_port: ctx.port,
        segment: ctx.segment,
        rows_scanned,
        segments_skipped: 0,
        exec_ms: started.elapsed().as_millis() as u64,
        groups,
    }
}

fn load_schema(table_dir: &str) -> Vec<ColumnDef> {
    let path = PathBuf::from(table_dir).join("_schema.ssf");
    let contents = std::fs::read_to_string(&path).unwrap_or_default();
    crate::storage_schema::parse_schema_file(&contents).unwrap_or_default()
}

fn segment_path(table_dir: &str, segment: u32) -> PathBuf {
    PathBuf::from(table_dir).join(format!("seg-{:06}", segment))
}

fn needed_columns(req: &QueryRequest) -> HashSet<String> {
    let mut set = HashSet::new();
    for g in &req.group_by {
        set.insert(g.clone());
    }
    for agg in &req.aggregates {
        if let Some(c) = &agg.column {
            set.insert(c.clone());
        }
    }
    for f in &req.filters {
        set.insert(f.column.clone());
    }
    // projections unused in aggregation path
    set
}

fn open_readers(
    segment_dir: &PathBuf,
    defs: &HashMap<String, ColumnDef>,
    needed: &HashSet<String>,
) -> Result<HashMap<String, ReaderState>, ()> {
    let mut map = HashMap::new();
    for name in needed {
        if let Some(col) = defs.get(name) {
            let path = segment_dir.join(format!("{}.bin", col.name));
            let reader = init_reader(&path, col).ok_or(())?;
            map.insert(col.name.clone(), reader);
        }
    }
    Ok(map)
}

fn compute_min_max(
    segment_dir: &PathBuf,
    defs: &HashMap<String, ColumnDef>,
    filters: &[crate::rpc::FilterExpr],
) -> Option<HashMap<String, (Option<ScalarValue>, Option<ScalarValue>)>> {
    let mut stats = HashMap::new();
    for f in filters {
        if stats.contains_key(&f.column) {
            continue;
        }
        let def = defs.get(&f.column)?;
        let path = segment_dir.join(format!("{}.bin", def.name));
        let mut reader = init_reader(&path, def)?;
        let mut min_val: Option<ScalarValue> = None;
        let mut max_val: Option<ScalarValue> = None;
        loop {
            match read_value(&mut reader, def) {
                Ok(Some(v)) => {
                    min_val = match min_val {
                        None => Some(v.clone()),
                        Some(ref cur) => Some(
                            if compare_scalar(&v, cur).map(|o| o.is_lt()).unwrap_or(false) {
                                v.clone()
                            } else {
                                cur.clone()
                            },
                        ),
                    };
                    max_val = match max_val {
                        None => Some(v.clone()),
                        Some(ref cur) => Some(
                            if compare_scalar(&v, cur).map(|o| o.is_gt()).unwrap_or(false) {
                                v
                            } else {
                                cur.clone()
                            },
                        ),
                    };
                }
                Ok(None) => {}
                Err(ReadError::Eof) => break,
                Err(ReadError::Io) => break,
            }
        }
        stats.insert(f.column.clone(), (min_val, max_val));
    }
    Some(stats)
}

fn should_skip(
    filters: &[crate::rpc::FilterExpr],
    stats: &HashMap<String, (Option<ScalarValue>, Option<ScalarValue>)>,
) -> bool {
    for f in filters {
        let Some((min_v, max_v)) = stats.get(&f.column) else {
            continue;
        };
        let target = &f.value;
        match f.pred {
            crate::rpc::Predicate::Eq => {
                if let (Some(minv), Some(maxv)) = (min_v, max_v) {
                    if compare_scalar(target, minv)
                        .map(|o| o.is_lt())
                        .unwrap_or(false)
                        || compare_scalar(target, maxv)
                            .map(|o| o.is_gt())
                            .unwrap_or(false)
                    {
                        return true;
                    }
                }
            }
            crate::rpc::Predicate::Lt => {
                if let Some(minv) = min_v {
                    if compare_scalar(minv, target)
                        .map(|o| o.is_ge())
                        .unwrap_or(false)
                    {
                        return true;
                    }
                }
            }
            crate::rpc::Predicate::Le => {
                if let Some(minv) = min_v {
                    if compare_scalar(minv, target)
                        .map(|o| o.is_gt())
                        .unwrap_or(false)
                    {
                        return true;
                    }
                }
            }
            crate::rpc::Predicate::Gt => {
                if let Some(maxv) = max_v {
                    if compare_scalar(maxv, target)
                        .map(|o| o.is_le())
                        .unwrap_or(false)
                    {
                        return true;
                    }
                }
            }
            crate::rpc::Predicate::Ge => {
                if let Some(maxv) = max_v {
                    if compare_scalar(maxv, target)
                        .map(|o| o.is_lt())
                        .unwrap_or(false)
                    {
                        return true;
                    }
                }
            }
            crate::rpc::Predicate::Between => {
                if let Some(hi) = &f.value_hi {
                    if let (Some(minv), Some(maxv)) = (min_v, max_v) {
                        if compare_scalar(maxv, target)
                            .map(|o| o.is_lt())
                            .unwrap_or(false)
                            || compare_scalar(minv, hi).map(|o| o.is_gt()).unwrap_or(false)
                        {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

fn compare_scalar(a: &ScalarValue, b: &ScalarValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (ScalarValue::Int(x), ScalarValue::Int(y)) => Some(x.cmp(y)),
        (ScalarValue::Float(x), ScalarValue::Float(y)) => x.partial_cmp(y),
        (ScalarValue::Int(x), ScalarValue::Float(y)) => (*x as f64).partial_cmp(y),
        (ScalarValue::Float(x), ScalarValue::Int(y)) => x.partial_cmp(&(*y as f64)),
        (ScalarValue::String(x), ScalarValue::String(y)) => Some(x.cmp(y)),
        (ScalarValue::Bool(x), ScalarValue::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}
