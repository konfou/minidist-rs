# MiniSQL def/parsing

- Grammar (`src/grammar/minisql.pest`):
  - `SELECT` projections (columns, `*`, aggregates
    COUNT/SUM/AVG/MIN/MAX).
  - `FROM <table>`.
  - Optional `WHERE` with `AND`-combined predicates: `=`, `<`, `>`,
    `<=`, `>=`, `BETWEEN`.
  - Optional `GROUP BY` with column list.
  - Required trailing semicolon; optional BOM and whitespace around.
  - Case-insensitive keywords.
- Parser (`src/minisql_parse.rs`):
  - Parses SQL into `QueryRequest` (projections, group_by, aggregates,
    filters, table).
  - Basic error propagation from pest; no semantic validation beyond
    grammar.
- Coordinator:
  - Accepts raw SQL via HTTP `/query`.
  - Converts to `QueryRequest` and dispatches to workers.
- Worker:
  - Receives serialized `QueryRequest` (MessagePack) and executes
    against its segment.

# Execution/printing

- Aggregates track their value type: SUM/MIN/MAX over integer/bool
  columns render as ints; floats stay floats. AVG is still emitted as
  float.
- Result formatting lives in `src/minisql_print.rs` and returns a text
  table:
  - Includes group column only when grouping (labelled with the group-by
    column name(s), otherwise omitted).
  - Normalizes aggregate headers (`SUM(amount)` -> `sum_amount`,
    `COUNT(*)` -> `count_star`).
  - Appends execution details (rows scanned, segments skipped, exec
    time) after a blank line.
- Workers perform simple per-segment zone-map pruning: compute min/max
  for columns referenced in filters and skip a segment when predicates
  cannot match, reporting skipped segments in execution details.
- Column files may be RLE-compressed (`RLE1` magic) or raw; readers
  auto-detect and decode runs.
