# Architecture

Core binaries:

- **minidist** (storage CLI):
  - `init`: create a table directory with schema/metadata.
  - `load`: ingest CSV into segmented columnar binaries.
  - `schema` / `info`: inspect stored schema/metadata.

- **coordinator**:
  - Spawns worker processes (one per segment) via `coordinator_cluster`.
  - Exposes an HTTP endpoint `POST /query`:
    - Body: SQL string.
    - Parses SQL (minisql) into a `QueryRequest`, dispatches to workers,
      merges partial aggregates, returns formatted result text.

- **worker**:
  - Starts a TCP listener per segment.
  - Receives length-prefixed MessagePack `QueryRequest` and returns a
    length-prefixed MessagePack `PartialAggregate`.
  - Executes scans/filters/aggregations against its segment’s columnar
    files.

Auxiliary:

- **netrepl**: client utility to send SQL over HTTP to the coordinator;
  not part of the formal system.
- **minilocal**: local REPL that parses and executes against a single
  segment (also uses the same result formatter as coordinator/netrepl);
  debug-only.

Data flow:

1. `coordinator` starts workers for each segment.
2. Client (e.g., `netrepl`) `POST /query` to coordinator with SQL.
3. Coordinator parses SQL, sends query to each worker over
   MessagePack/TCP.
4. Workers scan their segment, produce `PartialAggregate`.
5. Coordinator merges partials (SUM/COUNT add, MIN/MAX global, AVG via
   sum/count); failed workers are retried once, then treated as skipped
   segments in the merged stats; results are returned to the client.

Optimizations:

- Workers apply simple zone-map pruning per segment (min/max per filter
  column) and may skip their segment entirely; skipped segments are
  reported in execution details. Column readers auto-detect raw vs `RLE`
  run-length encoding.
