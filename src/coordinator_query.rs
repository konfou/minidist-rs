use crate::rpc::WorkerInfo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn get_worker_info(port: u16) -> anyhow::Result<WorkerInfo> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;

    stream.write_all(b"PING").await?;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;

    let resp_len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; resp_len];
    stream.read_exact(&mut buf).await?;

    let info: WorkerInfo = rmp_serde::from_slice(&buf)?;
    Ok(info)
}
