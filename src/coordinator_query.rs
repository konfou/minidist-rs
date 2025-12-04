use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn ping_worker(port: u16) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;

    stream.write_all(b"PING").await?;

    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await?;

    if &buf == b"PONG" {
        Ok(())
    } else {
        anyhow::bail!("Invalid reply");
    }
}
