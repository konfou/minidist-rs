#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ColumnType {
    Int32,
    Int64,
    Float64,
    Bool,
    String,
    Date,
    TimestampMs,
}

impl std::str::FromStr for ColumnType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "int32" => Ok(ColumnType::Int32),
            "int64" => Ok(ColumnType::Int64),
            "float64" => Ok(ColumnType::Float64),
            "bool" => Ok(ColumnType::Bool),
            "string" => Ok(ColumnType::String),
            "date" => Ok(ColumnType::Date),
            "timestamp(ms)" => Ok(ColumnType::TimestampMs),
            _ => Err(format!("Unknown type: {}", s)),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    pub nullable: bool,
    pub is_key: bool,
}

pub fn parse_schema_line(line: &str) -> Result<ColumnDef, String> {
    let line = line.trim();

    if line.is_empty() {
        return Err("Empty schema line".into());
    }

    let Some((name, rest)) = line.split_once(':') else {
        return Err("Missing ':' in schema line".into());
    };

    let name = name.trim().to_string();

    if name.is_empty() {
        return Err("Column name is empty".into());
    }

    let mut parts = rest.split_whitespace();

    let type_token = parts.next().ok_or("Missing column type")?;
    let col_type: ColumnType = type_token.parse()?;

    let mut nullable = false;
    let mut is_key = false;

    for p in parts {
        match p {
            "nullable" => nullable = true,
            "key" => {
                if is_key {
                    return Err("Duplicate 'key' flag".into());
                }
                is_key = true;
            }
            _ => return Err(format!("Unknown flag: {}", p)),
        }
    }

    Ok(ColumnDef {
        name,
        col_type,
        nullable,
        is_key,
    })
}

pub fn parse_schema_file(contents: &str) -> Result<Vec<ColumnDef>, String> {
    let mut columns = Vec::new();
    let mut key_count = 0;

    for (i, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }

        let col = parse_schema_line(line).map_err(|e| format!("Line {}: {}", i + 1, e))?;

        if col.is_key {
            key_count += 1;
        }

        columns.push(col);
    }

    if key_count == 0 {
        return Err("Schema must contain exactly one 'key' column".into());
    }

    if key_count > 1 {
        return Err("Schema contains more than one 'key' column".into());
    }

    Ok(columns)
}
