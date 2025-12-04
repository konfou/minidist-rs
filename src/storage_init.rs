use std::fs;
use std::path::Path;

pub fn init_table(dir: &Path, schema_file: &Path) -> Result<(), String> {
    fs::create_dir_all(dir).map_err(|e| format!("Failed to create directory: {}", e))?;

    // TODO: Validate schema rather simply copying it.
    let target_schema = dir.join("_schema.ssf");
    fs::copy(schema_file, &target_schema).map_err(|e| format!("Failed to copy schema: {}", e))?;

    // XXX: Unspecified what key-values in this file represent.
    //      Copying example verbatim for now.
    let table_txt = "\
version=1
block_rows=65536
segment_target_rows=1000000
endianness=little
";

    let target_table = dir.join("_table.txt");
    fs::write(&target_table, table_txt)
        .map_err(|e| format!("Failed to write _table.txt: {}", e))?;

    Ok(())
}
