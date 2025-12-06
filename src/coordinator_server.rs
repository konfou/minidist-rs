use crate::coordinator_query::run_query_on_worker;
use crate::rpc::QueryRequest;
use axum::{Router, extract::State, http::StatusCode, routing::post};
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    worker_ports: Arc<Vec<u16>>,
    table: String,
}

pub async fn serve(port: u16, worker_ports: Vec<u16>, table: &str) -> anyhow::Result<()> {
    let state = AppState {
        table: table.to_string(),
        worker_ports: Arc::new(worker_ports),
    };

    let app = Router::new()
        .route("/query", post(handle_query))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    println!("Coordinator listening on 127.0.0.1:{}", port);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_query(State(state): State<AppState>, body: String) -> (StatusCode, String) {
    if body.trim().eq_ignore_ascii_case("PING") {
        return (StatusCode::OK, "PONG".into());
    }

    let req = QueryRequest {
        query: body,
        table: state.table.clone(),
    };

    match run_query(&state.worker_ports, req).await {
        Ok(r) => (StatusCode::OK, r),
        Err(e) => (StatusCode::OK, format!("Error: {}", e)),
    }
}

pub async fn run_query(worker_ports: &[u16], request: QueryRequest) -> anyhow::Result<String> {
    let mut partials = Vec::new();
    for port in worker_ports {
        match run_query_on_worker(*port, &request).await {
            Ok(partial) => partials.push(partial),
            Err(e) => println!("Worker {port} query failed: {}", e),
        }
    }

    Ok("XXX: partials gathered".to_string())
}
