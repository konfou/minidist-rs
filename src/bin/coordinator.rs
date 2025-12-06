use clap::Parser;
use minidist_rs::coordinator_cluster;
use minidist_rs::coordinator_query::get_worker_info;
use tokio::time::{Duration, sleep};

#[derive(Parser)]
#[command(name = "coordinator")]
#[command(about = "minidist-rs coordinator", long_about = None)]
struct Args {
    #[arg(long)]
    port: u16,

    // XXX: Needs `required` to appear in error msg on start up.
    #[arg(long, required = true, value_delimiter = ',')]
    workers: Vec<u16>,

    #[arg(long)]
    table: String,
}

fn count_segments(table_dir: &std::path::Path) -> anyhow::Result<usize> {
    let mut count = 0usize;
    let entries = std::fs::read_dir(table_dir)
        .map_err(|e| anyhow::anyhow!("Failed to read table dir {:?}: {}", table_dir, e))?;

    for entry in entries {
        let entry = entry.map_err(|e| anyhow::anyhow!("Failed to read entry: {}", e))?;
        let file_type = entry
            .file_type()
            .map_err(|e| anyhow::anyhow!("Failed to read entry type: {}", e))?;
        if file_type.is_dir() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with("seg-") {
                count += 1;
            }
        }
    }

    if count == 0 {
        return Err(anyhow::anyhow!(
            "No segment directories (seg-*) found under table dir"
        ));
    }

    Ok(count)
}

fn resolve_worker_ports(spec: &[u16], segments: usize) -> anyhow::Result<Vec<u16>> {
    if spec.len() == 1 {
        let start = spec[0];
        let mut ports = Vec::with_capacity(segments);
        for offset in 0..segments {
            let port = start.checked_add(offset as u16).ok_or_else(|| {
                anyhow::anyhow!("Worker port overflow when deriving from start value")
            })?;
            ports.push(port);
        }
        Ok(ports)
    } else if spec.len() == segments {
        Ok(spec.to_vec())
    } else {
        Err(anyhow::anyhow!(
            "Expected either one worker port (start) or {} ports, got {}",
            segments,
            spec.len()
        ))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let segments = count_segments(std::path::Path::new(&args.table))?;
    let worker_ports = resolve_worker_ports(&args.workers, segments)?;
    let _ = coordinator_cluster::run(&worker_ports).await;

    loop {
        for port in &worker_ports {
            match get_worker_info(*port).await {
                Ok(info) => {
                    println!(
                        "Worker {port}: pid={} host={} port={}",
                        info.pid, info.hostname, info.port
                    );
                }
                Err(e) => println!("Worker {port} is DOWN ({})", e),
            }
        }

        println!("---");
        sleep(Duration::from_secs(2)).await;
    }
}
