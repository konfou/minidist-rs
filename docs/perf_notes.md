# Performance notes

- Columnar layout: each column stored separately allows scanning only
  needed columns.
- Segmentation: rows are split evenly across segment directories;
  coordinator can dispatch segments independently.
- Binary encoding: fixed-width types use little-endian binary; strings
  are length-prefixed to avoid parsing overhead.
- Sorting: ingestion sorts by key column to improve locality for range
  filters and merges.
- Execution: workers stream rows from column files; simple zone-map
  pruning (min/max per filter column) can skip a segment; column readers
  auto-detect raw vs `RLE1` run-length encoded files.
- Merging: coordinator merges aggregates by simple arithmetic; no
  distributed shuffle or repartitioning.

## Parser

The PEG engine uses backtracking, which introduces overhead compared to
deterministic parser generators or zero-copy combinator libraries.
However, for the expected usage (human-written MiniSQL queries of
limited size), Pest's performance is acceptable. The parsing
cost is predictable enough for interactive or analytical workloads.
