use crate::rpc::{GroupMap, PartialAggregate, QueryRequest};
use std::collections::HashMap;

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
    let mut rows_scanned = 0u64;
    let mut groups: GroupMap = HashMap::new();

    //TOOD: impl `group` when start querying
    PartialAggregate {
        worker_port: ctx.port,
        segment: ctx.segment,
        rows_scanned,
        segments_skipped: 1,
        exec_ms: started.elapsed().as_millis() as u64,
        groups,
    }
}
