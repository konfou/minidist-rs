use crate::rpc::{PartialAggregate, QueryRequest};

#[derive(Debug, Clone)]
pub struct WorkerContext {
    pub port: u16,
    pub table: String,
    pub segment: u32,
}

pub fn execute_query(
    ctx: &WorkerContext,
    req: QueryRequest,
    started: std::time::Instant,
) -> PartialAggregate {
    let rows_scanned = 0u64;

    PartialAggregate {
        worker_port: ctx.port,
        segment: ctx.segment,
        rows_scanned,
        segments_skipped: 0,
        exec_ms: started.elapsed().as_millis() as u64,
    }
}
