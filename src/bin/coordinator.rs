use clap::Parser;
use minidist_rs::coordinator_cluster;
use minidist_rs::coordinator_query::ping_worker;
use tokio::time::{Duration, sleep};

#[derive(Parser)]
#[command(name = "coordinator")]
#[command(about = "minidist-rs coordinator", long_about = None)]
struct Args {
    #[arg(long)]
    port: u16,

    // XXX: Needs `required` to appear in error msg on start up.
    #[arg(long, required = true, value_parser = parse_workers)]
    workers: (u16, u16),

    #[arg(long)]
    table: String,
}

fn parse_workers(s: &str) -> Result<(u16, u16), String> {
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 2 {
        return Err("Expected format <start>,<end>".into());
    }
    let start: u16 = parts[0].parse().map_err(|_| "Invalid start worker value")?;
    let end: u16 = parts[1].parse().map_err(|_| "Invalid end worker value")?;

    if start > end {
        return Err("start must be <= end".into());
    }

    Ok((start, end))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _ = coordinator_cluster::run(args.workers).await;

    loop {
        for port in args.workers.0..=args.workers.1 {
            match ping_worker(port).await {
                Ok(_) => println!("Worker {port} is ALIVE"),
                Err(_) => println!("Worker {port} is DOWN"),
            }
        }

        println!("---");
        sleep(Duration::from_secs(2)).await;
    }
}
