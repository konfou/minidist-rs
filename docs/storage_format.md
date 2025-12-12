# Storage format

This notes the on-disk layout used by minidist-rs.

## Tables
- Each table is a directory.
- Schema file: `_schema.ssf` (text). See `storage_schema.rs` parser; one
  column per line: `name: type [nullable] [key]`.
- Table metadata: `_table.txt` (key/value pairs).

## Segments
- Data is split into segment subdirectories named `seg-000000`,
  `seg-000001`, etc.
- Each segment contains one binary file per column: `<column>.bin`.

## Column binary encoding
- Each row's column value is stored in order; rows are distributed
  evenly across segments.
- Every value is prefixed with a 1-byte null flag:
  - `0` => NULL (only allowed if column is nullable)
  - `1` => value follows

Type encodings (little-endian):
- `int32`: 4 bytes (i32)
- `int64`: 8 bytes (i64)
- `float64`: 8 bytes (f64)
- `bool`: 1 byte (0 or nonzero)
- `string`: 4-byte length (u32) + UTF-8 bytes
- `date`: 4 bytes (i32) days since 1970-01-01
- `timestamp(ms)`: 8 bytes (i64) milliseconds since epoch

## Ingestion
- CSV is read, sorted by the key column, split into N segments.
- Columns are written independently into their respective segment files
  using the encoding above.
