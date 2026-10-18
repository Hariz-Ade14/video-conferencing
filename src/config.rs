use std::time::Duration;

use anyhow::Context;

/// Everything the server reads from the environment (or `.env`).
#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: String,
    /// Origins allowed to open the WebSocket and call the HTTP API.
    /// `*` allows any origin (handy in development, never in production).
    pub allowed_origins: Vec<String>,
    pub stun_urls: Vec<String>,
    /// TURN relay, if one is configured. Without it calls work on most
    /// networks but fail behind strict NATs and corporate firewalls.
    pub turn: Option<TurnConfig>,
    /// Hard cap on sockets per room. The frontend turns away a third person
    /// itself; this just stops a single room from being flooded.
    pub max_room_size: usize,
}

#[derive(Clone, Debug)]
pub struct TurnConfig {
    pub urls: Vec<String>,
    /// Shared with coturn's `static-auth-secret` (`use-auth-secret` mode).
    pub secret: String,
    pub ttl: Duration,
}

fn list(value: &str) -> Vec<String> {
    value.split(',').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect()
}

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let turn = match (var("TURN_URLS"), var("TURN_SECRET")) {
            (Some(urls), Some(secret)) => Some(TurnConfig {
                urls: list(&urls),
                secret,
                ttl: Duration::from_secs(
                    var("TURN_TTL_SECS").map_or(Ok(86_400), |v| v.parse()).context("TURN_TTL_SECS must be a number")?,
                ),
            }),
            (None, None) => None,
            _ => anyhow::bail!("set both TURN_URLS and TURN_SECRET, or neither"),
        };

        Ok(Self {
            bind_addr: var("BIND_ADDR").unwrap_or_else(|| "127.0.0.1:8080".into()),
            allowed_origins: list(&var("ALLOWED_ORIGINS").unwrap_or_else(|| "http://localhost:3000".into())),
            stun_urls: list(&var("STUN_URLS").unwrap_or_else(|| "stun:stun.l.google.com:19302".into())),
            turn,
            max_room_size: var("MAX_ROOM_SIZE").map_or(Ok(10), |v| v.parse()).context("MAX_ROOM_SIZE must be a number")?,
        })
    }

    pub fn origin_allowed(&self, origin: &str) -> bool {
        self.allowed_origins.iter().any(|o| o == "*" || o == origin)
    }
}
