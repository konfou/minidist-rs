# Data validation

- Schema file `_schema.ssf` is parsed but not deeply validated beyond
  syntax and a single `key` column requirement.
- Table metadata `_table.txt` is parsed for required keys: `version`,
  `block_rows`, `segment_target_rows`, `endianness` (must be `little` or
  `big`).
- CSV ingestion:
  - Validates headers contain required columns.
  - Enforces non-nullable columns are not empty.
  - Parses types per column; errors on parse failures.
  - Sorts rows by key column before segmenting.
- Runtime query validation is minimal; assumes stored binaries match the
  schema.
