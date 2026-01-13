use super::coordinator_merge::merge_partials;
use crate::minisql::minisql_print::format_results;
use crate::rpc::{PartialAggregate, QueryRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn run_query(worker_ports: &[u16], request: QueryRequest) -> anyhow::Result<String> {
    let mut partials = Vec::new();
    for (idx, port) in worker_ports.iter().enumerate() {
        let attempt = run_query_on_worker(*port, &request).await;
        let result = if attempt.is_err() {
            // one retry
            run_query_on_worker(*port, &request).await
        } else {
            attempt
        };

        match result {
            Ok(partial) => partials.push(partial),
            Err(e) => {
                println!("Worker {port} query failed after retry: {}", e);
                partials.push(crate::rpc::PartialAggregate {
                    worker_port: *port,
                    segment: idx as u32,
                    rows_scanned: 0,
                    segments_skipped: 1,
                    exec_ms: 0,
                    groups: std::collections::HashMap::new(),
                });
            }
        }
    }

    let (merged, rows_scanned, segments_skipped, exec_ms) = merge_partials(&partials);
    let effective_group_by = if request.aggregates.is_empty() && request.group_by.is_empty() {
        if request.projections.len() == 1 && request.projections[0] == "*" {
            Vec::new()
        } else {
            request.projections.clone()
        }
    } else {
        request.group_by.clone()
    };

    Ok(format_results(
        merged,
        rows_scanned,
        segments_skipped,
        exec_ms,
        &effective_group_by,
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
