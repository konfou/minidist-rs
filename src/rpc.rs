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

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct AggregateState {
    pub sum: f64,
    pub count: u64,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

pub type GroupAggregate = std::collections::HashMap<String, AggregateState>;
pub type GroupMap = std::collections::HashMap<String, GroupAggregate>;

#[derive(Debug, Serialize, Deserialize)]
pub struct PartialAggregate {
    pub worker_port: u16,
    pub segment: u32,
    pub rows_scanned: u64,
    pub segments_skipped: u64,
    pub exec_ms: u64,
    pub groups: GroupMap,
}
