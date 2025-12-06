use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub pid: u32,
    pub port: u16,
    pub hostname: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
    pub query: String,
    pub table: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartialAggregate {
    pub worker_port: u16,
    pub segment: u32,
    pub rows_scanned: u64,
    pub segments_skipped: u64,
    pub exec_ms: u64,
}
