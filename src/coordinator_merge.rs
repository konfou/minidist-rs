use crate::rpc::{AggregateState, GroupMap, PartialAggregate};
use std::collections::HashMap;

pub fn merge_partials(partials: &[PartialAggregate]) -> (GroupMap, u64, u64, u64) {
    let mut cuml: GroupMap = HashMap::new();
    let mut rows_scanned = 0u64;
    let mut segments_skipped = 0u64;
    let mut exec_ms = 0u64;

    for p in partials {
        rows_scanned += p.rows_scanned;
        segments_skipped += p.segments_skipped;
        exec_ms += p.exec_ms;

        for (g_key, g_agg) in &p.groups {
            let entry = cuml.entry(g_key.clone()).or_default();
            for (name, state) in g_agg {
                let agg = entry
                    .entry(name.clone())
                    .or_insert_with(AggregateState::default);
                merge_state(agg, state);
            }
        }
    }

    (cuml, rows_scanned, segments_skipped, exec_ms)
}

fn merge_state(dst: &mut AggregateState, src: &AggregateState) {
    dst.sum += src.sum;
    dst.count += src.count;

    dst.min = match (dst.min, src.min) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (None, Some(b)) => Some(b),
        (Some(a), None) => Some(a),
        (None, None) => None,
    };

    dst.max = match (dst.max, src.max) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (None, Some(b)) => Some(b),
        (Some(a), None) => Some(a),
        (None, None) => None,
    };
}

pub fn format_results(
    cuml: GroupMap,
    rows_scanned: u64,
    segments_skipped: u64,
    exec_ms: u64,
) -> String {
    let mut out = String::new();
    //TODO: impl
    out.push_str("<Response>");
    out.push_str(&format!(
        "Execution Details:\n\
         Rows scanned:       {}\n\
         Segments skipped:   {}\n\
         Execution time:     {} ms",
        rows_scanned, segments_skipped, exec_ms
    ));
    out
}
