use super::worker_exec::{WorkerContext, execute_query};
use crate::rpc::QueryRequest;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub async fn serve(ctx: WorkerContext) -> anyhow::Result<()> {
    let addr = format!("127.0.0.1:{}", ctx.port);
    let listener = TcpListener::bind(&addr).await?;
    println!(
        "Worker listening on {} (table={} segment={})",
        addr, ctx.table, ctx.segment
    );

    loop {
        let (mut socket, _) = listener.accept().await?;
        let ctx = ctx.clone();

        tokio::spawn(async move {
            let start = std::time::Instant::now();

            let mut len_buf = [0u8; 4];
            if socket.read_exact(&mut len_buf).await.is_err() {
                return;
            }

            let msg_len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; msg_len];
            if socket.read_exact(&mut buf).await.is_err() {
                return;
            }

            let req: QueryRequest = match rmp_serde::from_slice(&buf) {
                Ok(q) => q,
                Err(_) => return,
            };

            let res = execute_query(&ctx, req, start);

            let payload = match rmp_serde::to_vec_named(&res) {
                Ok(p) => p,
                Err(_) => return,
            };

            let len_bytes = (payload.len() as u32).to_le_bytes();
            let _ = socket.write_all(&len_bytes).await;
            let _ = socket.write_all(&payload).await;
        });
    }
}
