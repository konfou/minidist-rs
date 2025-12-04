use std::fs;
use std::path::Path;

pub fn inspect_schema(dir: &Path) -> Result<String, String> {
    // TODO: Validate schema file rather simply reading it.
    let path = dir.join("_schema.ssf");
    fs::read_to_string(&path).map_err(|e| format!("Failed to read schema file {:?}: {}", path, e))
}

pub fn inspect_metadata(dir: &Path) -> Result<String, String> {
    // TODO: Validate metadata file rather simply reading it.
    let path = dir.join("_table.txt");
    fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read table metadata {:?}: {}", path, e))
}
