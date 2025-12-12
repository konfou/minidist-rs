use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub pid: u32,
    pub port: u16,
    pub hostname: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ScalarValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Predicate {
    Eq,
    Lt,
    Gt,
    Le,
    Ge,
    Between,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub enum AggregateFn {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilterExpr {
    pub column: String,
    pub pred: Predicate,
    pub value: ScalarValue,
    pub value_hi: Option<ScalarValue>, // used for BETWEEN
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AggregateExpr {
    pub func: AggregateFn,
    pub column: Option<String>, // None for COUNT(*)
    pub output_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryRequest {
    pub query: String,
    pub projections: Vec<String>,
    pub aggregates: Vec<AggregateExpr>,
    pub table: String,
    pub filters: Vec<FilterExpr>,
    pub group_by: Vec<String>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct AggregateState {
    pub sum: f64,
    pub count: u64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub value_type: ValueType,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum ValueType {
    Int,
    Float,
}

impl Default for ValueType {
    fn default() -> Self {
        ValueType::Float
    }
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
