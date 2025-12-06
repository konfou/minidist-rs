use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub pid: u32,
    pub port: u16,
    pub hostname: String,
}
