use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use crate::minisql_eval::{ReadError, apply_agg, format_scalar, read_value, row_matches};
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
    let mut readers = match open_readers(&segment_dir, &schema, &needed_cols) {
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
            match read_value(&mut reader_state.reader, &reader_state.def) {
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

struct ReaderState {
    def: ColumnDef,
    reader: BufReader<File>,
}

fn open_readers(
    segment_dir: &PathBuf,
    schema: &[ColumnDef],
    needed: &HashSet<String>,
) -> Result<HashMap<String, ReaderState>, ()> {
    let mut map = HashMap::new();
    for col in schema {
        if needed.contains(&col.name) {
            let path = segment_dir.join(format!("{}.bin", col.name));
            let f = File::open(&path).map_err(|_| ())?;
            map.insert(
                col.name.clone(),
                ReaderState {
                    def: col.clone(),
                    reader: BufReader::new(f),
                },
            );
        }
    }
    Ok(map)
}
