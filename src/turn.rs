use std::time::{SystemTime, UNIX_EPOCH};

use axum::{Json, extract::State, http::header, response::IntoResponse};
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, KeyInit, Mac};
use serde::Serialize;
use sha1::Sha1;

use crate::AppState;

#[derive(Serialize, Debug)]
pub struct IceServer {
    pub urls: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential: Option<String>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct IceConfig {
    /// Same shape as `RTCConfiguration.iceServers`, so the browser can use it as is.
    pub ice_servers: Vec<IceServer>,
    /// Seconds until the TURN credential stops working (0 when there is no TURN).
    pub ttl: u64,
}

/// coturn's "TURN REST API" scheme: the username is the expiry time, and the
/// password is an HMAC of it under the secret coturn also knows. coturn can
/// check it without a database, and a leaked credential expires on its own.
pub fn turn_credential(secret: &str, expires_at: u64) -> (String, String) {
    let username = format!("{expires_at}:guest");
    let mut mac = Hmac::<Sha1>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(username.as_bytes());
    let credential = STANDARD.encode(mac.finalize().into_bytes());
    (username, credential)
}

/// `GET /turn-credentials`: the ICE servers a browser should use for a call.
pub async fn credentials(State(state): State<AppState>) -> impl IntoResponse {
    let config = &state.config;
    let mut ice_servers = vec![IceServer { urls: config.stun_urls.clone(), username: None, credential: None }];
    let mut ttl = 0;

    if let Some(turn) = &config.turn {
        ttl = turn.ttl.as_secs();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let (username, credential) = turn_credential(&turn.secret, now + ttl);
        ice_servers.push(IceServer { urls: turn.urls.clone(), username: Some(username), credential: Some(credential) });
    }

    ([(header::CACHE_CONTROL, "no-store")], Json(IceConfig { ice_servers, ttl }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_coturn_rest_api_scheme() {
        // Reference value from:
        //   printf '1700000000:guest' | openssl dgst -sha1 -hmac 'north-wind' -binary | base64
        let (username, credential) = turn_credential("north-wind", 1_700_000_000);
        assert_eq!(username, "1700000000:guest");
        assert_eq!(credential, "C7fSBqpU8fftvXHuO1iPzZsQoi8=");
    }
}
