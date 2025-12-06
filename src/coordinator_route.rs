use crate::coordinator_merge::{format_results, merge_partials};
use crate::rpc::{PartialAggregate, QueryRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn run_query(worker_ports: &[u16], request: QueryRequest) -> anyhow::Result<String> {
    let mut partials = Vec::new();
    for port in worker_ports {
        match run_query_on_worker(*port, &request).await {
            Ok(partial) => partials.push(partial),
            Err(e) => println!("Worker {port} query failed: {}", e),
        }
    }

    let (merged, rows_scanned, segments_skipped, exec_ms) = merge_partials(&partials);
    Ok(format_results(
        merged,
        rows_scanned,
        segments_skipped,
        exec_ms,
    ))
}

async fn run_query_on_worker(port: u16, req: &QueryRequest) -> anyhow::Result<PartialAggregate> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;

    let payload = rmp_serde::to_vec_named(req)?;
    let len = (payload.len() as u32).to_le_bytes();

    stream.write_all(&len).await?;
    stream.write_all(&payload).await?;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;

    let resp_len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; resp_len];
    stream.read_exact(&mut buf).await?;

    let partial: PartialAggregate = rmp_serde::from_slice(&buf)?;
    Ok(partial)
}
