use clap::Parser;
use minidist::coordinator::coordinator_cluster;
use minidist::coordinator::coordinator_server;

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let segments = coordinator_cluster::count_segments(std::path::Path::new(&args.table))?;
    let worker_ports = coordinator_cluster::resolve_worker_ports(&args.workers, segments)?;
    let _cluster =
        coordinator_cluster::WorkerCluster::spawn(&worker_ports, std::path::Path::new(&args.table))
            .await?;

    coordinator_server::serve(args.port, worker_ports, &args.table).await
}
