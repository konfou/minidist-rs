use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::storage::storage_schema;

pub fn inspect_schema(dir: &Path) -> Result<String, String> {
    let path = dir.join("_schema.ssf");
    let contents = fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read schema file {:?}: {}", path, e))?;

    storage_schema::parse_schema_file(&contents).map_err(|e| format!("Invalid schema: {}", e))?;

    Ok(contents)
}

pub fn inspect_metadata(dir: &Path) -> Result<String, String> {
    let path = dir.join("_table.txt");
    let contents = fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read table metadata {:?}: {}", path, e))?;

    let metadata = parse_table_metadata(&contents)?;

    let mut out = String::new();
    out.push_str(&format!("version: {}\n", metadata.version));
    out.push_str(&format!("block_rows: {}\n", metadata.block_rows));
    out.push_str(&format!(
        "segment_target_rows: {}\n",
        metadata.segment_target_rows
    ));
    out.push_str(&format!("endianness: {}\n", metadata.endianness));

    Ok(out)
}

struct TableMetadata {
    version: u32,
    block_rows: usize,
    segment_target_rows: usize,
    endianness: String,
}

fn parse_table_metadata(contents: &str) -> Result<TableMetadata, String> {
    let mut kv = HashMap::new();

    for (i, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let Some((k, v)) = line.split_once('=') else {
            return Err(format!("Line {}: expected key=value", i + 1));
        };

        if kv.contains_key(k) {
            return Err(format!("Line {}: duplicate key '{}'", i + 1, k));
        }

        kv.insert(k.to_string(), v.to_string());
    }

    let version = kv
        .get("version")
        .ok_or("Missing 'version'")?
        .parse::<u32>()
        .map_err(|_| "Invalid numeric value for 'version'")?;

    let block_rows = kv
        .get("block_rows")
        .ok_or("Missing 'block_rows'")?
        .parse::<usize>()
        .map_err(|_| "Invalid numeric value for 'block_rows'")?;

    if block_rows == 0 {
        return Err("block_rows must be > 0".into());
    }

    let segment_target_rows = kv
        .get("segment_target_rows")
        .ok_or("Missing 'segment_target_rows'")?
        .parse::<usize>()
        .map_err(|_| "Invalid numeric value for 'segment_target_rows'")?;

    if segment_target_rows == 0 {
        return Err("segment_target_rows must be > 0".into());
    }

    let endianness = kv
        .get("endianness")
        .ok_or("Missing 'endianness'")?
        .to_string();

    if endianness != "little" && endianness != "big" {
        return Err("endianness must be 'little' or 'big'".into());
    }

    Ok(TableMetadata {
        version,
        block_rows,
        segment_target_rows,
        endianness,
    })
}
