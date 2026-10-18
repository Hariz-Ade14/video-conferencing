// use video_call_backend::{Config, app};

// #[tokio::main]
// async fn main() -> anyhow::Result<()> {
//     dotenvy::dotenv().ok();
//     tracing_subscriber::fmt()
//         .with_env_filter(
//             tracing_subscriber::EnvFilter::try_from_default_env()
//                 .unwrap_or_else(|_| "video_call_backend=debug,tower_http=info".into()),
//         )
//         .init();

//     let config = Config::from_env()?;
//     let addr = config.bind_addr.clone();
//     if config.turn.is_none() {
//         tracing::warn!("no TURN server configured: calls between some networks will fail to connect");
//     }
//     if config.allowed_origins.iter().any(|o| o == "*") {
//         tracing::warn!("ALLOWED_ORIGINS=* lets any website use this server; restrict it in production");
//     }

//     let listener = tokio::net::TcpListener::bind(&addr).await?;
//     tracing::info!("listening on http://{addr} (WebSocket at ws://{addr}/ws)");
//     axum::serve(listener, app(config))
//         .with_graceful_shutdown(async {
//             let _ = tokio::signal::ctrl_c().await;
//             tracing::info!("shutting down");
//         })
//         .await?;
//     Ok(())
// }

use video_call_backend::{Config, app};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "video_call_backend=debug,tower_http=info".into()),
        )
        .init();

    let mut config = Config::from_env()?;

    // Render requires the server to bind to 0.0.0.0
    // and listen on the assigned PORT.
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string());

    config.bind_addr = format!("0.0.0.0:{port}");

    let addr = config.bind_addr.clone();

    if config.turn.is_none() {
        tracing::warn!(
            "no TURN server configured: calls between some networks will fail to connect"
        );
    }

    if config.allowed_origins.iter().any(|o| o == "*") {
        tracing::warn!(
            "ALLOWED_ORIGINS=* lets any website use this server; restrict it in production"
        );
    }

    let listener = tokio::net::TcpListener::bind(&addr).await?;

    tracing::info!(
        "listening on http://{addr} (WebSocket at ws://{addr}/ws)"
    );

    axum::serve(listener, app(config))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;

    Ok(())
}