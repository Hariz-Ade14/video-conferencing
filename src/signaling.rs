use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::Bytes,
    extract::{
        Query, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::{
    AppState,
    rooms::{ConnId, OUTBOX_CAPACITY},
};

/// An SDP offer is a few KB; anything far beyond that isn't signaling.
const MAX_MESSAGE_BYTES: usize = 64 * 1024;
const PING_EVERY: Duration = Duration::from_secs(20);
/// A socket that sends nothing (not even a pong) for this long is gone,
/// e.g. a laptop lid closed mid-call.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Per socket. Generous for a burst of ICE candidates, low enough to cut off a flood.
const MAX_MESSAGES_PER_SEC: u32 = 100;

/// Close codes in the 4000-4999 range are free for applications to use.
const CLOSE_ROOM_FULL: u16 = 4001;
const CLOSE_TOO_FAST: u16 = 4002;

#[derive(Deserialize)]
pub struct WsParams {
    room: String,
}

/// Same rule as the frontend's room codes (`src/lib/room.ts`).
pub fn valid_room(room: &str) -> bool {
    (3..=64).contains(&room.len()) && room.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// `GET /ws?room=<code>`: joins the room and relays signaling messages.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(params): Query<WsParams>,
    headers: HeaderMap,
) -> Response {
    // CORS doesn't apply to WebSockets, so a page on any site could open one
    // with the visitor's browser. Checking Origin is how that's prevented.
    let origin = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()).unwrap_or("");
    if !state.config.origin_allowed(origin) {
        return (StatusCode::FORBIDDEN, "origin not allowed").into_response();
    }
    if !valid_room(&params.room) {
        return (StatusCode::BAD_REQUEST, "invalid room code").into_response();
    }
    if state.rooms.is_full(&params.room, state.config.max_room_size) {
        return (StatusCode::CONFLICT, "room is full").into_response();
    }

    ws.max_message_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| handle_socket(socket, state, params.room))
}

/// Just the fields the server checks. Everything else in the message is
/// passed through untouched, so the frontend can add message types freely.
#[derive(Deserialize)]
struct Envelope {
    room: String,
    message: Header,
}

#[derive(Deserialize)]
struct Header {
    from: String,
    to: Option<String>,
}

async fn handle_socket(mut socket: WebSocket, state: AppState, room: String) {
    let (tx, mut outbox) = mpsc::channel::<Arc<str>>(OUTBOX_CAPACITY);
    // Checked again here: someone may have joined since the pre-upgrade check.
    let Ok(conn) = state.rooms.join(&room, tx, state.config.max_room_size) else {
        let _ = socket.send(close(CLOSE_ROOM_FULL, "room is full")).await;
        return;
    };
    tracing::debug!(room, conn, "joined");

    let mut heartbeat = tokio::time::interval(PING_EVERY);
    heartbeat.tick().await; // the first tick fires immediately
    let mut last_seen = Instant::now();
    let mut rate = RateWindow::default();

    loop {
        tokio::select! {
            outgoing = outbox.recv() => {
                // None: the room dropped us (our queue overflowed).
                let Some(text) = outgoing else { break };
                if socket.send(Message::text(text.as_ref())).await.is_err() {
                    break;
                }
            }
            incoming = socket.recv() => {
                last_seen = Instant::now();
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if !rate.allow() {
                            tracing::warn!(room, conn, "too many messages, disconnecting");
                            let _ = socket.send(close(CLOSE_TOO_FAST, "too many messages")).await;
                            break;
                        }
                        relay(&state, &room, conn, text.as_str());
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {} // pong, ping (axum answers it) or binary (ignored)
                }
            }
            _ = heartbeat.tick() => {
                if last_seen.elapsed() > IDLE_TIMEOUT {
                    tracing::debug!(room, conn, "idle, disconnecting");
                    break;
                }
                if socket.send(Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
            }
        }
    }

    state.rooms.leave(&room, conn);
    tracing::debug!(room, conn, "left");
}

fn relay(state: &AppState, room: &str, conn: ConnId, text: &str) {
    let Ok(envelope) = serde_json::from_str::<Envelope>(text) else {
        tracing::debug!(room, conn, "dropping malformed message");
        return;
    };
    let from = envelope.message.from.as_str();
    if envelope.room != room || from.is_empty() || from.len() > 64 {
        tracing::debug!(room, conn, "dropping message with bad room or sender");
        return;
    }
    if !state.rooms.claim_peer_id(room, conn, from) {
        tracing::warn!(room, conn, from, "dropping message sent under another peer's id");
        return;
    }
    state.rooms.relay(room, conn, envelope.message.to.as_deref(), Arc::from(text));
}

fn close(code: u16, reason: &'static str) -> Message {
    Message::Close(Some(CloseFrame { code, reason: reason.into() }))
}

struct RateWindow {
    started: Instant,
    count: u32,
}

impl Default for RateWindow {
    fn default() -> Self {
        Self { started: Instant::now(), count: 0 }
    }
}

impl RateWindow {
    fn allow(&mut self) -> bool {
        if self.started.elapsed() >= Duration::from_secs(1) {
            *self = Self::default();
        }
        self.count += 1;
        self.count <= MAX_MESSAGES_PER_SEC
    }
}

#[cfg(test)]
mod tests {
    use super::valid_room;

    #[test]
    fn room_codes() {
        assert!(valid_room("kqf-xbtw-mzr"));
        assert!(valid_room("abc"));
        assert!(!valid_room("ab"));
        assert!(!valid_room("Has-Caps"));
        assert!(!valid_room("../etc"));
        assert!(!valid_room(&"a".repeat(65)));
    }
}
