use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use crate::rpc::{
    AggregateExpr, AggregateFn, AggregateState, FilterExpr, Predicate, ScalarValue, ValueType,
};
use crate::storage::storage_schema::{ColumnDef, ColumnType};

pub fn row_matches(filters: &[FilterExpr], row: &HashMap<String, Option<ScalarValue>>) -> bool {
    for f in filters {
        let val = match row.get(&f.column) {
            Some(v) => v.clone(),
            None => None,
        };

        if !eval_filter(val, f) {
            return false;
        }
    }
    true
}

pub fn eval_filter(val: Option<ScalarValue>, f: &FilterExpr) -> bool {
    let Some(v) = val else {
        return false;
    };
    match f.pred {
        Predicate::Eq => cmp_eq(&v, &f.value),
        Predicate::Lt => cmp_order(&v, &f.value, |o| o.is_lt()),
        Predicate::Gt => cmp_order(&v, &f.value, |o| o.is_gt()),
        Predicate::Le => cmp_order(&v, &f.value, |o| o.is_le()),
        Predicate::Ge => cmp_order(&v, &f.value, |o| o.is_ge()),
        Predicate::Between => {
            if let Some(hi) = &f.value_hi {
                cmp_order(&v, &f.value, |o| o.is_ge()) && cmp_order(&v, hi, |o| o.is_le())
            } else {
                false
            }
        }
    }
}

pub fn apply_agg(
    state: &mut AggregateState,
    expr: &AggregateExpr,
    row: &HashMap<String, Option<ScalarValue>>,
) {
    match expr.func {
        AggregateFn::Count => {
            state.count += 1;
        }
        AggregateFn::Sum | AggregateFn::Avg => {
            if let Some(val) = expr
                .column
                .as_ref()
                .and_then(|c| row.get(c))
                .and_then(|v| v.clone())
            {
                set_value_type(state, &val);
                accumulate_numeric(state, &val);
            }
        }
        AggregateFn::Min => {
            if let Some(val) = expr
                .column
                .as_ref()
                .and_then(|c| row.get(c))
                .and_then(|v| v.clone())
            {
                set_value_type(state, &val);
                if let Some(f) = as_f64(&val) {
                    state.min = Some(match state.min {
                        Some(m) => m.min(f),
                        None => f,
                    });
                }
            }
        }
        AggregateFn::Max => {
            if let Some(val) = expr
                .column
                .as_ref()
                .and_then(|c| row.get(c))
                .and_then(|v| v.clone())
            {
                set_value_type(state, &val);
                if let Some(f) = as_f64(&val) {
                    state.max = Some(match state.max {
                        Some(m) => m.max(f),
                        None => f,
                    });
                }
            }
        }
    }
}

fn set_value_type(state: &mut AggregateState, val: &ScalarValue) {
    match val {
        ScalarValue::Int(_) | ScalarValue::Bool(_) => state.value_type = ValueType::Int,
        ScalarValue::Float(_) => state.value_type = ValueType::Float,
        ScalarValue::String(_) => {}
    }
}

fn accumulate_numeric(state: &mut AggregateState, val: &ScalarValue) {
    match val {
        ScalarValue::Int(i) => {
            state.sum += *i as f64;
            state.count += 1;
        }
        ScalarValue::Float(f) => {
            state.sum += *f;
            state.count += 1;
        }
        ScalarValue::Bool(b) => {
            state.sum += if *b { 1.0 } else { 0.0 };
            state.count += 1;
        }
        ScalarValue::String(_) => {}
    }
}

pub fn cmp_eq(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Int(x), ScalarValue::Int(y)) => x == y,
        (ScalarValue::Float(x), ScalarValue::Float(y)) => x == y,
        (ScalarValue::Int(x), ScalarValue::Float(y)) => (*x as f64) == *y,
        (ScalarValue::Float(x), ScalarValue::Int(y)) => *x == *y as f64,
        (ScalarValue::String(x), ScalarValue::String(y)) => x == y,
        (ScalarValue::Bool(x), ScalarValue::Bool(y)) => x == y,
        _ => false,
    }
}

pub fn cmp_order<F>(a: &ScalarValue, b: &ScalarValue, pred: F) -> bool
where
    F: Fn(Ordering) -> bool,
{
    match (a, b) {
        (ScalarValue::Int(x), ScalarValue::Int(y)) => pred(x.cmp(y)),
        (ScalarValue::Float(x), ScalarValue::Float(y)) => {
            pred(x.partial_cmp(y).unwrap_or(Ordering::Equal))
        }
        (ScalarValue::Int(x), ScalarValue::Float(y)) => {
            pred((*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal))
        }
        (ScalarValue::Float(x), ScalarValue::Int(y)) => {
            pred(x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal))
        }
        (ScalarValue::String(x), ScalarValue::String(y)) => pred(x.cmp(y)),
        _ => false,
    }
}

pub fn as_f64(v: &ScalarValue) -> Option<f64> {
    match v {
        ScalarValue::Int(i) => Some(*i as f64),
        ScalarValue::Float(f) => Some(*f),
        ScalarValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        ScalarValue::String(_) => None,
    }
}

pub fn format_scalar(v: &Option<ScalarValue>) -> String {
    match v {
        Some(ScalarValue::Int(i)) => i.to_string(),
        Some(ScalarValue::Float(f)) => f.to_string(),
        Some(ScalarValue::String(s)) => s.clone(),
        Some(ScalarValue::Bool(b)) => b.to_string(),
        None => "NULL".into(),
    }
}

pub enum ReadError {
    Eof,
    Io,
}

pub enum ReaderState {
    Raw(BufReader<File>),
    Rle {
        reader: BufReader<File>,
        remaining: u32,
        current: Option<ScalarValue>,
    },
}

pub fn init_reader(path: &std::path::Path, _def: &ColumnDef) -> Option<ReaderState> {
    let mut file = File::open(path).ok()?;
    let mut magic = [0u8; 4];
    if file.read_exact(&mut magic).is_ok() && &magic == b"RLE1" {
        let reader = BufReader::new(file);
        Some(ReaderState::Rle {
            reader,
            remaining: 0,
            current: None,
        })
    } else {
        let _ = file.seek(SeekFrom::Start(0));
        Some(ReaderState::Raw(BufReader::new(file)))
    }
}

pub fn read_value(
    state: &mut ReaderState,
    col: &ColumnDef,
) -> Result<Option<ScalarValue>, ReadError> {
    match state {
        ReaderState::Raw(reader) => read_value_raw(reader, col),
        ReaderState::Rle {
            reader,
            remaining,
            current,
        } => {
            if *remaining == 0 {
                // load next run
                let mut len_buf = [0u8; 4];
                if reader.read_exact(&mut len_buf).is_err() {
                    return Err(ReadError::Eof);
                }
                *remaining = u32::from_le_bytes(len_buf);
                let mut null_flag = [0u8; 1];
                if reader.read_exact(&mut null_flag).is_err() {
                    return Err(ReadError::Eof);
                }
                if null_flag[0] == 0 {
                    *current = None;
                } else {
                    *current = read_scalar(reader, col)?;
                }
            }
            if *remaining == 0 {
                return Err(ReadError::Eof);
            }
            *remaining -= 1;
            Ok(current.clone())
        }
    }
}

fn read_value_raw(
    reader: &mut BufReader<File>,
    col: &ColumnDef,
) -> Result<Option<ScalarValue>, ReadError> {
    let mut null_flag = [0u8; 1];
    if reader.read_exact(&mut null_flag).is_err() {
        return Err(ReadError::Eof);
    }
    if null_flag[0] == 0 {
        return Ok(None);
    }
    read_scalar(reader, col)
}

fn read_scalar(
    reader: &mut BufReader<File>,
    col: &ColumnDef,
) -> Result<Option<ScalarValue>, ReadError> {
    match col.col_type {
        ColumnType::Int32 => {
            let mut buf = [0u8; 4];
            if reader.read_exact(&mut buf).is_err() {
                return Err(ReadError::Io);
            }
            Ok(Some(ScalarValue::Int(i32::from_le_bytes(buf) as i64)))
        }
        ColumnType::Int64 => {
            let mut buf = [0u8; 8];
            if reader.read_exact(&mut buf).is_err() {
                return Err(ReadError::Io);
            }
            Ok(Some(ScalarValue::Int(i64::from_le_bytes(buf))))
        }
        ColumnType::Float64 => {
            let mut buf = [0u8; 8];
            if reader.read_exact(&mut buf).is_err() {
                return Err(ReadError::Io);
            }
            Ok(Some(ScalarValue::Float(f64::from_le_bytes(buf))))
        }
        ColumnType::Bool => {
            let mut buf = [0u8; 1];
            if reader.read_exact(&mut buf).is_err() {
                return Err(ReadError::Io);
            }
            Ok(Some(ScalarValue::Bool(buf[0] != 0)))
        }
        ColumnType::String => {
            let mut len_buf = [0u8; 4];
            if reader.read_exact(&mut len_buf).is_err() {
                return Err(ReadError::Io);
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            if reader.read_exact(&mut buf).is_err() {
                return Err(ReadError::Io);
            }
            let s = String::from_utf8_lossy(&buf).to_string();
            Ok(Some(ScalarValue::String(s)))
        }
        ColumnType::Date => {
            let mut buf = [0u8; 4];
            if reader.read_exact(&mut buf).is_err() {
                return Err(ReadError::Io);
            }
            Ok(Some(ScalarValue::Int(i32::from_le_bytes(buf) as i64)))
        }
        ColumnType::TimestampMs => {
            let mut buf = [0u8; 8];
            if reader.read_exact(&mut buf).is_err() {
                return Err(ReadError::Io);
            }
            Ok(Some(ScalarValue::Int(i64::from_le_bytes(buf))))
        }
    }
}
