# MiniSQL

## Definition

- Grammar (`src/grammar/minisql.pest`):
  - `SELECT` projections (columns, `*`, aggregates
    COUNT/SUM/AVG/MIN/MAX).
  - `FROM <table>`.
  - Optional `WHERE` with `AND`-combined predicates: `=`, `<`, `>`,
    `<=`, `>=`, `BETWEEN`.
  - Optional `GROUP BY` with column list.
  - Required trailing semicolon; optional BOM and whitespace around.
  - Case-insensitive keywords.

## Flow

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

## Execution/printing

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

## Rationales

Given the assignment's educational focus, Pest was chosen for
implementing the parser because it provides a clear, declarative grammar
that closely mirrors MiniSQL's structure. The grammar is easy to read,
reason about, and maintain. That said, since the project is built around
a small-to-medium SQL subset, the additional growth/complexity of other
frameworks may be irrelevant and their simplicity/performance gains may
be worth the trade-off.

All, algorithm-wise, choices considered, with information on
implementation complexity, performance, and growth:

- Ad-hoc regex "parser":
  - Complexity: low at start; quickly becomes fragile.
  - Performance: good at simple queries; degrades with complexity.
  - Growth: poor; hard to extend safely.
- Recursive descent:
  - Complexity: moderate-to-high; more code, more tests.
  - Performance: excellent; direct AST build, minimal overhead.
  - Growth: strong; explicit control over grammar and errors.
  - Libraries: none required (standard library only).
- Grammar-driven LL/LR parser generator:
  - Complexity: high; grammar constraints and tooling overhead.
  - Performance: excellent; deterministic, low overhead.
  - Growth: strong; explicit precedence and unambiguous grammar.
- Grammar-driven PEG parser generator:
  - Complexity: moderate; grammar is concise and readable.
  - Performance: good enough; manageable backtracking overhead.
  - Growth: strong; grammar scales cleanly.
- Parser combinators:
  - Complexity: moderate; expressive but less readable.
  - Performance: good-to-excellent; low overhead.
  - Growth: strong; composable but can be harder to debug.

All, except recursive descent needing none and ad-hoc regex only needing
`regex`, have few libraries that could use for the implementation. No
exhaustive market research was made.
