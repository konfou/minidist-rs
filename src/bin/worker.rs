use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use minidist_rs::rpc::WorkerInfo;

#[derive(Parser, Debug)]
#[command(name = "worker")]
#[command(about = "minidist-rs worker node")]
struct Args {
    #[arg(long)]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let addr = format!("127.0.0.1:{}", args.port);
    let listener = TcpListener::bind(&addr).await?;
    println!("Worker listening on {}", addr);

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf = [0u8; 4];
            if socket.read_exact(&mut buf).await.is_err() {
                return;
            }

            if &buf != b"PING" {
                return;
            }

            let info = WorkerInfo {
                pid: std::process::id(),
                port: args.port,
                hostname: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
            };

            let payload = match rmp_serde::to_vec_named(&info) {
                Ok(p) => p,
                Err(_) => return,
            };

            let len_bytes = (payload.len() as u32).to_le_bytes();
            let _ = socket.write_all(&len_bytes).await;
            let _ = socket.write_all(&payload).await;
        });
    }
}
