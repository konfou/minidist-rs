use clap::Parser;
use minidist::coordinator::coordinator_merge::merge_partials;
use minidist::minisql::minisql_eval::{
    ReadError, ReaderState, format_scalar, init_reader, read_value,
};
use minidist::minisql::minisql_parse;
use minidist::minisql::minisql_print::format_results;
use minidist::rpc::ScalarValue;
use minidist::storage::storage_schema::ColumnDef;
use minidist::worker::worker_exec::{WorkerContext, execute_query};
use std::io::{self, Write};
use std::path::Path;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(name = "minilocal")]
#[command(about = "minidist-rs local REPL (debug-oriented)")]
struct Args {
    #[arg(long)]
    table: String,

    #[arg(long, default_value_t = 0)]
    segment: u32,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let table_path = Path::new(&args.table);
    let schema_path = table_path.join("_schema.ssf");
    if !schema_path.exists() {
        anyhow::bail!("Table not found or missing _schema.ssf: {}", args.table);
    }

    println!(
        "Starting minilocal against table={} segment={}",
        args.table, args.segment
    );

    let mut query_buf = String::new();
    let mut prompt = "minilocal> ";

    loop {
        print!("{}", prompt);
        io::stdout().flush().ok();

        let mut line = String::new();
        if io::stdin().read_line(&mut line)? == 0 {
            break;
        }
        query_buf.push_str(&line);

        if !query_buf.trim_end().ends_with(';') {
            prompt = "... ";
            continue;
        }

        match minisql_parse::parse_sql(&query_buf) {
            Ok(mut req) => {
                req.table = args.table.clone();
                if req.aggregates.is_empty() {
                    match scan_projections(&req.table, args.segment, &req.projections) {
                        Ok((rows, scanned)) => {
                            for r in rows {
                                println!("{}", r.join("\t"));
                            }
                            println!(
                                "Execution Details:\n\
                                 Rows scanned:       {}\n\
                                 Segments skipped:   0\n\
                                 Execution time:     0 ms",
                                scanned
                            );
                        }
                        Err(e) => eprintln!("scan error: {}", e),
                    }
                } else {
                    let ctx = WorkerContext {
                        port: 0,
                        table: args.table.clone(),
                        segment: args.segment,
                    };
                    let partial = execute_query(&ctx, req.clone(), Instant::now());
                    let (merged, rows_scanned, segments_skipped, exec_ms) =
                        merge_partials(&[partial]);
                    let output = format_results(
                        merged,
                        rows_scanned,
                        segments_skipped,
                        exec_ms,
                        &req.group_by,
                    );
                    println!("{}", output);
                }
            }
            Err(e) => {
                eprintln!("parse error: {}", e);
            }
        }

        query_buf.clear();
        prompt = "minilocal> ";
    }

    Ok(())
}

fn scan_projections(
    table: &str,
    segment: u32,
    projections: &[String],
) -> Result<(Vec<Vec<String>>, u64), String> {
    let schema_path = Path::new(table).join("_schema.ssf");
    let schema_str =
        std::fs::read_to_string(&schema_path).map_err(|e| format!("read schema: {}", e))?;
    let schema = minidist::storage::storage_schema::parse_schema_file(&schema_str)
        .map_err(|e| format!("parse schema: {}", e))?;
    let segment_dir = Path::new(table).join(format!("seg-{:06}", segment));

    let mut needed = Vec::new();
    for name in projections {
        if name == "*" {
            needed = schema.iter().map(|c| c.name.clone()).collect();
            break;
        }
        needed.push(name.clone());
    }

    let mut readers: Vec<(String, ColumnDef, ReaderState)> = Vec::new();
    for col in &schema {
        if needed.contains(&col.name) {
            let path = segment_dir.join(format!("{}.bin", col.name));
            let reader =
                init_reader(&path, col).ok_or_else(|| format!("open {:?}: failed", path))?;
            readers.push((col.name.clone(), col.clone(), reader));
        }
    }

    let mut rows = Vec::new();
    let mut rows_scanned = 0u64;
    loop {
        let mut row_vals: Vec<Option<ScalarValue>> = Vec::new();
        let mut eof = false;
        for (_, def, rdr) in readers.iter_mut() {
            match read_value(rdr, def) {
                Ok(v) => row_vals.push(v),
                Err(ReadError::Eof) => {
                    eof = true;
                    break;
                }
                Err(ReadError::Io) => {
                    eof = true;
                    break;
                }
            }
        }
        if eof {
            break;
        }
        rows_scanned += 1;
        let rendered: Vec<String> = row_vals.iter().map(|v| format_scalar(v)).collect();
        rows.push(rendered);
    }

    Ok((rows, rows_scanned))
}
