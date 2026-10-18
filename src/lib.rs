pub mod config;
pub mod rooms;
pub mod signaling;
pub mod turn;

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderValue, Method},
    routing::get,
};
use serde_json::{Value, json};
use tower_http::{
    cors::{AllowOrigin, Any, CorsLayer},
    trace::TraceLayer,
};

pub use config::Config;

/// Shared by every request handler; cloning is cheap (two `Arc`s).
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub rooms: rooms::Rooms,
}

pub fn app(config: Config) -> Router {
    let cors = if config.allowed_origins.iter().any(|o| o == "*") {
        CorsLayer::new().allow_origin(Any)
    } else {
        let origins: Vec<HeaderValue> = config.allowed_origins.iter().filter_map(|o| o.parse().ok()).collect();
        CorsLayer::new().allow_origin(AllowOrigin::list(origins))
    }
    .allow_methods([Method::GET]);

    let state = AppState { config: Arc::new(config), rooms: Default::default() };

    Router::new()
        .route("/health", get(health))
        .route("/turn-credentials", get(turn::credentials))
        .route("/ws", get(signaling::ws_handler))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    let (rooms, connections) = state.rooms.stats();
    Json(json!({ "status": "ok", "rooms": rooms, "connections": connections }))
}
