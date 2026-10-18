//! Runs the real server on a random port and talks to it over real sockets.

use std::time::Duration;

use axum::{body::Body, http::Request};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};
use tower::ServiceExt;
use video_call_backend::{Config, app, config::TurnConfig};

const ORIGIN: &str = "http://localhost:3000";

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn config() -> Config {
    Config {
        bind_addr: "127.0.0.1:0".into(),
        allowed_origins: vec![ORIGIN.into()],
        stun_urls: vec!["stun:stun.example.com:3478".into()],
        turn: None,
        max_room_size: 3,
    }
}

async fn start(config: Config) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app(config)).await.unwrap() });
    format!("ws://{addr}/ws")
}

async fn try_connect(base: &str, room: &str, origin: &str) -> Result<Socket, u16> {
    let mut request = format!("{base}?room={room}").into_client_request().unwrap();
    request.headers_mut().insert("Origin", HeaderValue::from_str(origin).unwrap());
    match connect_async(request).await {
        Ok((socket, _)) => Ok(socket),
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => Err(response.status().as_u16()),
        Err(e) => panic!("unexpected connect error: {e}"),
    }
}

async fn connect(base: &str, room: &str) -> Socket {
    try_connect(base, room, ORIGIN).await.expect("connect")
}

async fn send(socket: &mut Socket, room: &str, message: Value) {
    let envelope = json!({ "room": room, "message": message });
    socket.send(Message::text(envelope.to_string())).await.unwrap();
}

/// Next text message, skipping pings. Panics after 2s.
async fn recv(socket: &mut Socket) -> Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("timed out waiting for a message")
            .expect("socket closed")
            .unwrap();
        if let Message::Text(text) = frame {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

/// Asserts nothing arrives within 300ms.
async fn assert_silent(socket: &mut Socket) {
    if let Ok(Some(Ok(Message::Text(text)))) = tokio::time::timeout(Duration::from_millis(300), socket.next()).await {
        panic!("expected no message, got {text}");
    }
}

#[tokio::test]
async fn relays_to_others_in_the_same_room_only() {
    let base = start(config()).await;
    let mut alice = connect(&base, "room-one").await;
    let mut bob = connect(&base, "room-one").await;
    let mut stranger = connect(&base, "room-two").await;

    send(&mut alice, "room-one", json!({ "type": "join", "from": "alice", "name": "Alice" })).await;

    let got = recv(&mut bob).await;
    assert_eq!(got["room"], "room-one");
    assert_eq!(got["message"], json!({ "type": "join", "from": "alice", "name": "Alice" }));
    assert_silent(&mut alice).await; // no echo to the sender
    assert_silent(&mut stranger).await; // nothing leaks across rooms
}

#[tokio::test]
async fn messages_with_to_reach_only_that_peer() {
    let base = start(config()).await;
    let mut alice = connect(&base, "abc").await;
    let mut bob = connect(&base, "abc").await;
    let mut carol = connect(&base, "abc").await;

    // Everyone announces themselves so the server learns their peer ids.
    for (socket, id) in [(&mut alice, "alice"), (&mut bob, "bob"), (&mut carol, "carol")] {
        send(socket, "abc", json!({ "type": "join", "from": id, "name": id })).await;
    }
    for _ in 0..2 {
        recv(&mut alice).await;
        recv(&mut bob).await;
        recv(&mut carol).await;
    }

    let offer = json!({ "type": "description", "from": "alice", "to": "bob", "description": { "type": "offer", "sdp": "v=0" } });
    send(&mut alice, "abc", offer.clone()).await;
    assert_eq!(recv(&mut bob).await["message"], offer);
    assert_silent(&mut carol).await; // carol never sees alice and bob's SDP
}

#[tokio::test]
async fn disconnect_tells_the_room_who_left() {
    let base = start(config()).await;
    let mut alice = connect(&base, "abc").await;
    let mut bob = connect(&base, "abc").await;
    send(&mut bob, "abc", json!({ "type": "join", "from": "bob", "name": "Bob" })).await;
    recv(&mut alice).await;

    drop(bob); // tab closed without saying goodbye

    let got = recv(&mut alice).await;
    assert_eq!(got, json!({ "room": "abc", "message": { "type": "leave", "from": "bob" } }));
}

#[tokio::test]
async fn cannot_send_as_someone_else() {
    let base = start(config()).await;
    let mut alice = connect(&base, "abc").await;
    let mut mallory = connect(&base, "abc").await;
    let mut bob = connect(&base, "abc").await;

    send(&mut alice, "abc", json!({ "type": "join", "from": "alice", "name": "Alice" })).await;
    recv(&mut mallory).await;
    recv(&mut bob).await;

    // Mallory tries to hang up on Bob's behalf as Alice.
    send(&mut mallory, "abc", json!({ "type": "leave", "from": "alice" })).await;
    assert_silent(&mut bob).await;
}

#[tokio::test]
async fn drops_malformed_and_wrong_room_messages() {
    let base = start(config()).await;
    let mut alice = connect(&base, "abc").await;
    let mut bob = connect(&base, "abc").await;

    alice.send(Message::text("not json")).await.unwrap();
    alice.send(Message::text(r#"{"room":"abc","message":{"type":"join"}}"#)).await.unwrap(); // no `from`
    send(&mut alice, "other-room", json!({ "type": "join", "from": "alice", "name": "A" })).await;
    assert_silent(&mut bob).await;

    // The socket is still usable afterwards.
    send(&mut alice, "abc", json!({ "type": "join", "from": "alice", "name": "A" })).await;
    assert_eq!(recv(&mut bob).await["message"]["from"], "alice");
}

#[tokio::test]
async fn rejects_bad_origin_bad_room_and_full_room() {
    let base = start(config()).await;
    assert_eq!(try_connect(&base, "abc", "https://evil.example").await.err(), Some(403));
    assert_eq!(try_connect(&base, "Bad_Room", ORIGIN).await.err(), Some(400));

    let _a = connect(&base, "abc").await;
    let _b = connect(&base, "abc").await;
    let _c = connect(&base, "abc").await;
    assert_eq!(try_connect(&base, "abc", ORIGIN).await.err(), Some(409), "max_room_size is 3");
}

#[tokio::test]
async fn flooding_gets_disconnected() {
    let base = start(config()).await;
    let mut flooder = connect(&base, "abc").await;
    for i in 0..150 {
        let msg = json!({ "room": "abc", "message": { "type": "candidate", "from": "f", "n": i } });
        if flooder.send(Message::text(msg.to_string())).await.is_err() {
            break;
        }
    }
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(Ok(frame)) = flooder.next().await {
            if let Message::Close(Some(frame)) = frame {
                return Some(u16::from(frame.code));
            }
        }
        None
    })
    .await
    .expect("socket should close");
    assert_eq!(closed, Some(4002));
}

async fn get_json(config: Config, path: &str) -> (u16, Value, Option<String>) {
    let response = app(config)
        .oneshot(Request::get(path).header("Origin", ORIGIN).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let cors = response
        .headers()
        .get("access-control-allow-origin")
        .map(|v| v.to_str().unwrap().to_string());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap(), cors)
}

#[tokio::test]
async fn health() {
    let (status, body, _) = get_json(config(), "/health").await;
    assert_eq!(status, 200);
    assert_eq!(body, json!({ "status": "ok", "rooms": 0, "connections": 0 }));
}

#[tokio::test]
async fn ice_servers_without_turn() {
    let (status, body, cors) = get_json(config(), "/turn-credentials").await;
    assert_eq!(status, 200);
    assert_eq!(cors.as_deref(), Some(ORIGIN));
    assert_eq!(body, json!({ "iceServers": [{ "urls": ["stun:stun.example.com:3478"] }], "ttl": 0 }));
}

#[tokio::test]
async fn ice_servers_with_turn() {
    let mut config = config();
    config.turn = Some(TurnConfig {
        urls: vec!["turn:turn.example.com:3478".into()],
        secret: "north-wind".into(),
        ttl: Duration::from_secs(3600),
    });
    let (_, body, _) = get_json(config, "/turn-credentials").await;

    assert_eq!(body["ttl"], 3600);
    let turn = &body["iceServers"][1];
    assert_eq!(turn["urls"], json!(["turn:turn.example.com:3478"]));
    let username = turn["username"].as_str().unwrap();
    let expires: u64 = username.split(':').next().unwrap().parse().unwrap();
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    assert!((now + 3590..=now + 3610).contains(&expires), "expires about an hour from now");
    assert_eq!(turn["credential"].as_str().unwrap().len(), 28, "base64 of a 20-byte SHA-1 HMAC");
}
