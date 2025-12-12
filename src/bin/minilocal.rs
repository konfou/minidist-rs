use clap::Parser;
use minidist_rs::minisql_parse;
use std::io::{self, Write};

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
                println!("parsed correctly: {:?}", req);
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
