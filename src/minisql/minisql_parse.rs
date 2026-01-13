use crate::rpc::{AggregateExpr, AggregateFn, FilterExpr, Predicate, QueryRequest, ScalarValue};
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "minisql/grammar/minisql.pest"]
struct SqlParser;

pub fn parse_sql(sql: &str) -> Result<QueryRequest, String> {
    let mut pairs = SqlParser::parse(Rule::sql, sql).map_err(|e| e.to_string())?;
    let sql_pair = pairs
        .next()
        .ok_or_else(|| "Expected SQL statement".to_string())?;
    let mut select_pair = None;
    for p in sql_pair.into_inner() {
        if p.as_rule() == Rule::select_stmt {
            select_pair = Some(p);
            break;
        }
    }
    let select = select_pair.ok_or_else(|| "Expected SELECT statement".to_string())?;
    let mut projections = Vec::new();
    let mut aggregates = Vec::new();
    let mut filters = Vec::new();
    let mut group_by = Vec::new();
    let mut table: Option<String> = None;

    for element in select.into_inner() {
        match element.as_rule() {
            Rule::projection => {
                for proj_item in element.into_inner() {
                    if proj_item.as_rule() != Rule::projection_item {
                        continue;
                    }
                    let proj_text = proj_item.as_str().to_string();
                    let mut inner_iter = proj_item.into_inner();
                    if let Some(inner) = inner_iter.next() {
                        match inner.as_rule() {
                            Rule::aggregate_expr => {
                                let (func, column, output_name) = parse_agg(inner)?;
                                aggregates.push(AggregateExpr {
                                    func,
                                    column,
                                    output_name,
                                });
                            }
                            Rule::ident => projections.push(inner.as_str().to_string()),
                            Rule::star => projections.push("*".into()),
                            _ => {}
                        }
                    } else {
                        projections.push(proj_text);
                    }
                }
            }
            Rule::projection_item => {
                let proj_text = element.as_str().to_string();
                let mut inner_iter = element.into_inner();
                if let Some(inner) = inner_iter.next() {
                    match inner.as_rule() {
                        Rule::aggregate_expr => {
                            let (func, column, output_name) = parse_agg(inner)?;
                            aggregates.push(AggregateExpr {
                                func,
                                column,
                                output_name,
                            });
                        }
                        Rule::ident => projections.push(inner.as_str().to_string()),
                        Rule::star => projections.push("*".into()),
                        _ => {}
                    }
                } else {
                    projections.push(proj_text);
                }
            }
            Rule::table_name => {
                table = Some(element.as_str().to_string());
            }
            Rule::where_clause => {
                if let Some(boolean_expr) = element.into_inner().next() {
                    for pred in boolean_expr.into_inner() {
                        if pred.as_rule() == Rule::predicate {
                            filters.push(parse_predicate(pred)?);
                        }
                    }
                }
            }
            Rule::group_by_clause => {
                for ident in element.into_inner() {
                    if ident.as_rule() == Rule::group_item {
                        group_by.push(ident.as_str().to_string());
                    }
                }
            }
            Rule::group_item => {
                group_by.push(element.as_str().to_string());
            }
            _ => {}
        }
    }

    let table = table.ok_or_else(|| "Table name missing".to_string())?;

    Ok(QueryRequest {
        query: sql.to_string(),
        projections,
        aggregates,
        table,
        filters,
        group_by,
    })
}

fn parse_agg(
    pair: pest::iterators::Pair<Rule>,
) -> Result<(AggregateFn, Option<String>, String), String> {
    let mut func = None;
    let mut column: Option<String> = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::aggregate_fn => {
                func = Some(match p.as_str().to_uppercase().as_str() {
                    "COUNT" => AggregateFn::Count,
                    "SUM" => AggregateFn::Sum,
                    "AVG" => AggregateFn::Avg,
                    "MIN" => AggregateFn::Min,
                    "MAX" => AggregateFn::Max,
                    _ => return Err("Unsupported aggregate".into()),
                });
            }
            Rule::ident => {
                column = Some(p.as_str().to_string());
            }
            Rule::star => {
                column = None;
            }
            _ => {}
        }
    }

    let func = func.ok_or_else(|| "Aggregate function missing".to_string())?;
    let output = match &column {
        Some(col) => format!("{}({})", format!("{:?}", func).to_uppercase(), col),
        None => format!("{}(*)", format!("{:?}", func).to_uppercase()),
    };

    Ok((func, column, output))
}

fn parse_predicate(pair: pest::iterators::Pair<Rule>) -> Result<FilterExpr, String> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or_else(|| "Empty predicate".to_string())?;

    match first.as_rule() {
        Rule::comparison_expr => parse_comparison(first),
        Rule::between_expr => parse_between(first),
        _ => Err("Unsupported predicate".into()),
    }
}

fn parse_comparison(pair: pest::iterators::Pair<Rule>) -> Result<FilterExpr, String> {
    let mut inner = pair.into_inner();
    let column = inner
        .next()
        .ok_or_else(|| "Missing column".to_string())?
        .as_str()
        .to_string();

    let pred_pair = inner.next().ok_or_else(|| "Missing operator".to_string())?;
    let pred = match pred_pair.as_str() {
        "=" => Predicate::Eq,
        "<" => Predicate::Lt,
        ">" => Predicate::Gt,
        "<=" => Predicate::Le,
        ">=" => Predicate::Ge,
        _ => return Err("Unsupported operator".into()),
    };

    let value_pair = inner.next().ok_or_else(|| "Missing literal".to_string())?;
    let value = parse_literal(value_pair)?;

    Ok(FilterExpr {
        column,
        pred,
        value,
        value_hi: None,
    })
}

fn parse_between(pair: pest::iterators::Pair<Rule>) -> Result<FilterExpr, String> {
    let mut inner = pair.into_inner();
    let column = inner
        .next()
        .ok_or_else(|| "Missing column".to_string())?
        .as_str()
        .to_string();

    let low = inner
        .next()
        .ok_or_else(|| "Missing low bound".to_string())?;
    let high = inner
        .next()
        .ok_or_else(|| "Missing high bound".to_string())?;

    Ok(FilterExpr {
        column,
        pred: Predicate::Between,
        value: parse_literal(low)?,
        value_hi: Some(parse_literal(high)?),
    })
}

fn parse_literal(pair: pest::iterators::Pair<Rule>) -> Result<ScalarValue, String> {
    let p = pair.into_inner().next().ok_or("Invalid literal")?;
    match p.as_rule() {
        Rule::number => {
            let s = p.as_str();
            if let Ok(i) = s.parse::<i64>() {
                Ok(ScalarValue::Int(i))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok(ScalarValue::Float(f))
            } else {
                Err("Invalid number".into())
            }
        }
        Rule::string_lit => {
            let s = p.as_str();
            let inner = &s[1..s.len().saturating_sub(1)];
            Ok(ScalarValue::String(inner.to_string()))
        }
        _ => Err("Unknown literal type".into()),
    }
}
