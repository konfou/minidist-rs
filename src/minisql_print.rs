use crate::rpc::{AggregateState, GroupMap};

pub fn format_results(
    cuml: GroupMap,
    rows_scanned: u64,
    segments_skipped: u64,
    exec_ms: u64,
    group_by: &[String],
) -> String {
    let mut out = String::new();

    if cuml.is_empty() {
        out.push_str("empty result\n");
    } else {
        let mut group_keys: Vec<_> = cuml.keys().cloned().collect();
        group_keys.sort();

        // Determine headers
        let mut agg_headers: Vec<String> = Vec::new();
        if let Some(first_key) = group_keys.first() {
            if let Some(first_map) = cuml.get(first_key) {
                let mut keys: Vec<_> = first_map.keys().cloned().collect();
                keys.sort();
                agg_headers = keys;
            }
        }

        let mut headers = Vec::new();
        let include_group = !(group_keys.len() == 1 && group_keys[0] == "all");
        if include_group {
            let label = if group_by.is_empty() {
                "group".to_string()
            } else {
                group_by.join(",")
            };
            headers.push(label);
        }
        headers.extend(
            agg_headers
                .iter()
                .map(|h| normalize_header(h))
                .collect::<Vec<_>>(),
        );

        // Build rows
        let mut rows: Vec<Vec<String>> = Vec::new();
        for gk in group_keys {
            let agg_map = cuml.get(&gk).unwrap();
            let mut row_vals: Vec<String> = Vec::new();
            if include_group {
                row_vals.push(gk.clone());
            }
            for raw_name in &agg_headers {
                if let Some(state) = agg_map.get(raw_name) {
                    row_vals.push(render_state_value(raw_name, state));
                } else {
                    row_vals.push(String::new());
                }
            }
            rows.push(row_vals);
        }

        // Column widths
        let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
        for row in &rows {
            for (i, val) in row.iter().enumerate() {
                widths[i] = widths[i].max(val.len());
            }
        }

        // Header row
        out.push_str(&format_row(&headers, &widths));
        // Separator
        let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w.max(&3))).collect();
        out.push_str(&sep.join("-+-"));
        out.push('\n');
        // Data rows
        for row in rows {
            out.push_str(&format_row(&row, &widths));
        }
    }

    out.push('\n');
    out.push_str(&format!(
        "Execution Details:\n\
         Rows scanned:       {}\n\
         Segments skipped:   {}\n\
         Execution time:     {} ms",
        rows_scanned, segments_skipped, exec_ms
    ));
    out
}

fn render_state_value(name: &str, state: &AggregateState) -> String {
    let upper = name.to_ascii_uppercase();
    if upper.starts_with("COUNT") {
        return state.count.to_string();
    }
    if upper.starts_with("SUM") {
        return format!("{:.3}", state.sum);
    }
    if upper.starts_with("AVG") {
        if state.count == 0 {
            return "NULL".into();
        }
        return format!("{:.3}", state.sum / state.count as f64);
    }
    if upper.starts_with("MIN") {
        return match state.min {
            Some(v) => format!("{:.3}", v),
            None => "NULL".into(),
        };
    }
    if upper.starts_with("MAX") {
        return match state.max {
            Some(v) => format!("{:.3}", v),
            None => "NULL".into(),
        };
    }
    // Fallback: show count
    state.count.to_string()
}

fn normalize_header(raw: &str) -> String {
    let lower = raw.to_ascii_lowercase();
    if lower.contains('(') {
        lower
            .replace('(', "_")
            .replace(')', "")
            .replace('*', "star")
    } else {
        lower
    }
}

fn format_row(cols: &[String], widths: &[usize]) -> String {
    let mut parts = Vec::new();
    for (i, col) in cols.iter().enumerate() {
        parts.push(format!("{:<width$}", col, width = widths[i]));
    }
    parts.join(" | ") + "\n"
}
