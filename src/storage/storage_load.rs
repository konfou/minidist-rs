use crate::storage::storage_schema::{ColumnDef, ColumnType};
use csv::ReaderBuilder;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

pub fn load_table(
    table_dir: &Path,
    csv_path: &Path,
    sort_key: &String,
    segments: usize,
    schema: &[ColumnDef],
) -> Result<(), String> {
    let key_col = schema
        .iter()
        .find(|c| c.is_key)
        .ok_or("Schema has no column marked as `key`")?;

    if sort_key != &key_col.name {
        return Err(format!(
            "Sort key '{}' does not match schema key '{}'",
            sort_key, key_col.name
        ));
    }

    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(csv_path)
        .map_err(|e| format!("Failed to read CSV: {}", e))?;

    // XXX: Unspecified existence of headers.
    // TODO: Check how big DBs handle this.
    let headers = reader
        .headers()
        .map_err(|e| format!("CSV header error: {}", e))?
        .clone();

    let mut col_index = Vec::new();
    for col in schema {
        let idx = headers
            .iter()
            .position(|h| h == &col.name)
            .ok_or_else(|| format!("CSV missing required column: '{}'", col.name))?;
        col_index.push(idx);
    }

    let mut rows = Vec::new();
    for r in reader.records() {
        let rec = r.map_err(|e| format!("CSV read error: {}", e))?;
        rows.push(rec);
    }

    if rows.is_empty() {
        return Err("CSV contains no data rows".into());
    }

    let key_idx = headers
        .iter()
        .position(|h| h == &key_col.name)
        .ok_or_else(|| format!("CSV missing key column '{}'", key_col.name))?;

    rows.sort_by(|a, b| a[key_idx].cmp(&b[key_idx]));

    for seg in 0..segments {
        let seg_dir = table_dir.join(format!("seg-{:06}", seg));
        fs::create_dir_all(&seg_dir)
            .map_err(|e| format!("Failed to create segment dir {:?}: {}", seg_dir, e))?;
    }

    struct ColWriter {
        writers: Vec<BufWriter<File>>,
    }

    let mut col_writers: Vec<ColWriter> = Vec::new();

    for col in schema {
        let mut writers = Vec::new();
        for seg in 0..segments {
            let seg_dir = table_dir.join(format!("seg-{:06}", seg));
            let path = seg_dir.join(format!("{}.bin", col.name));

            let f = File::create(&path)
                .map_err(|e| format!("Failed to create file {:?}: {}", path, e))?;

            writers.push(BufWriter::new(f));
        }

        col_writers.push(ColWriter { writers });
    }

    let total_rows = rows.len();
    let rows_per_seg = (total_rows + segments - 1) / segments;

    for (i, record) in rows.into_iter().enumerate() {
        let seg = (i / rows_per_seg).min(segments - 1);

        for (col_idx, col) in schema.iter().enumerate() {
            let csv_field = record[col_index[col_idx]].trim();
            let writer = &mut col_writers[col_idx].writers[seg];
            write_value(writer, csv_field, col)?;
        }
    }

    Ok(())
}

fn write_value(w: &mut BufWriter<File>, field: &str, col: &ColumnDef) -> Result<(), String> {
    if field.is_empty() {
        if col.nullable {
            w.write_all(&[0u8]).map_err(|e| format!("{}", e))?;
            return Ok(());
        } else {
            return Err(format!(
                "Column '{}' is NOT NULL, but encountered empty value",
                col.name
            ));
        }
    }

    // TODO: Fill up error messages.
    w.write_all(&[1u8]).map_err(|e| format!("{}", e))?;

    match col.col_type {
        ColumnType::Int32 => {
            let v: i32 = field.parse().map_err(|e| format!("{}", e))?;
            w.write_all(&v.to_le_bytes()).map_err(|e| format!("{}", e))
        }

        ColumnType::Int64 => {
            let v: i64 = field.parse().map_err(|e| format!("{}", e))?;
            w.write_all(&v.to_le_bytes()).map_err(|e| format!("{}", e))
        }

        ColumnType::Float64 => {
            let v: f64 = field.parse().map_err(|e| format!("{}", e))?;
            w.write_all(&v.to_le_bytes()).map_err(|e| format!("{}", e))
        }

        ColumnType::Bool => {
            // TODO: Also consider if rather true/false, CSV uses 1/0.
            let byte = if field.eq_ignore_ascii_case("true") {
                1u8
            } else {
                0u8
            };
            w.write_all(&[byte]).map_err(|e| format!("{}", e))
        }

        ColumnType::String => {
            let bytes = field.as_bytes();
            let len = bytes.len() as u32;
            w.write_all(&len.to_le_bytes())
                .map_err(|e| format!("{}", e))?;
            w.write_all(bytes).map_err(|e| format!("{}", e))
        }

        ColumnType::Date => {
            // XXX: Unspecified expected date format. ISO 8601 is used.
            // TODO: Check how big DBs handle this.
            let date = chrono::NaiveDate::parse_from_str(field, "%Y-%m-%d")
                .map_err(|e| format!("{}", e))?;
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = date.signed_duration_since(epoch).num_days() as i32;
            w.write_all(&days.to_le_bytes())
                .map_err(|e| format!("{}", e))
        }

        ColumnType::TimestampMs => {
            let v: i64 = field.parse().map_err(|e| format!("{}", e))?;
            w.write_all(&v.to_le_bytes()).map_err(|e| format!("{}", e))
        }
    }
}
