use clap::Parser;
use minidist_rs::worker::worker_exec::WorkerContext;
use minidist_rs::worker::worker_server;

#[derive(Parser, Debug)]
#[command(name = "worker")]
#[command(about = "minidist-rs worker node")]
struct Args {
    #[arg(long)]
    port: u16,

    #[arg(long)]
    table: String,

    #[arg(long)]
    segment: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let ctx = WorkerContext {
        port: args.port,
        table: args.table,
        segment: args.segment,
    };

    worker_server::serve(ctx).await
}
