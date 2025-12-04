use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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
            if socket.read_exact(&mut buf).await.is_ok() && &buf == b"PING" {
                let _ = socket.write_all(b"PONG").await;
            }
        });
    }
}
