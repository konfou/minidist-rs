# minidist-rs

Simple distributed SQL query engine over a column store written in Rust.

Data lives in a simple columnar-on-disk layout sorted by a key column;
`minidist` ingests CSV into segmented binaries; `coordinator` parses a
basic SQL subset (SELECT/WHERE/GROUP BY with COUNT/SUM/AVG/MIN/MAX),
divides queries to per-segment `worker` processes over MessagePack/TCP,
merges partials, and formats results as a text table; `netrepl` and
`minilocal` are lightweight clients (HTTP to coordinator or
single-segment local) for quick querying.

This project models the core ideas behind analytical data engines in a
simplified, educational form.

The libraries (crates) used are:

  * anyhow for easy idiomatic error handling,
  * axum for HTTP API (client-coordinator),
  * chrono for datetime operations,
  * clap for CLI,
  * csv/rmp/serde for CSV/MP serialization,
  * pest for SQL grammar/parsing,
  * tokio for async & RPC (coordinator-workers),
