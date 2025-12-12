# Unspecified behavior

## Load sort-key vs schema key
According to schema definition

>The column marked key defines the sort order for all data in the table.

Also the ingest rules state that data must be **sorted by the key
column**. Yet the `load` command accepts a `--sort-key`
argument. Specification does not define which takes precedence.

### Currently:
- The schema key is treated as authoritative.
- `--sort-key` must match the schema key or produce an error.

### Open Questions:
- Should `--sort-key` be removed entirely?
- Should composite keys be allowed?
