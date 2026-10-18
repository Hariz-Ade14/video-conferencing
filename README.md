# Video Call — backend

Rust (axum + tokio) signaling server for the Next.js app in `../web`. It
introduces the two browsers in a call to each other; the video itself goes
directly between them over WebRTC and never touches this server.

## Run

```bash
cp .env.example .env
cargo run
```

Then start the frontend pointed at it:

```bash
cd ../web
NEXT_PUBLIC_SIGNALING_URL=ws://localhost:8080/ws NEXT_PUBLIC_API_URL=http://localhost:8080 npm run dev
```

`ALLOWED_ORIGINS` must match the frontend's address exactly, or the WebSocket
gets a 403.

## Endpoints

| Route | What it does |
|---|---|
| `GET /ws?room=<code>` | WebSocket. Relays `{ "room", "message" }` envelopes to the others in the room. |
| `GET /turn-credentials` | ICE servers for `RTCPeerConnection`, with short-lived TURN credentials if TURN is configured. |
| `GET /health` | `{"status":"ok","rooms":N,"connections":N}` |

## What the signaling server enforces

- **Origin check** on the WebSocket (CORS doesn't cover WebSockets).
- **Room codes** must match the frontend's format (`a-z`, `0-9`, `-`, 3–64 chars).
- **No impersonation:** the first `from` a socket uses becomes its identity;
  later messages under another id, or an id already in use, are dropped.
- **Private routing:** messages with `to` reach only that peer, so a third
  person never sees the others' SDP or network addresses.
- **Goodbye on disconnect:** when a socket drops, the room gets a `leave` for it,
  so a crashed tab is noticed at once.
- **Limits:** 64 KB per message, 100 messages/sec (close code 4002), room size
  (HTTP 409 / close code 4001), a 60 s idle timeout with pings every 20 s, and
  sockets that can't keep up are dropped.

Message contents are otherwise passed through untouched, so new message types
in `web/src/lib/signaling/protocol.ts` need no backend change.

## Code

| File | |
|---|---|
| `src/main.rs` | Reads config, starts the server, graceful shutdown on Ctrl-C |
| `src/lib.rs` | Routes, CORS, shared state |
| `src/config.rs` | Environment variables (see `.env.example`) |
| `src/rooms.rs` | Who's in which room; relaying, `to` routing, identity, cleanup |
| `src/signaling.rs` | The WebSocket handler: checks, heartbeat, rate limit |
| `src/turn.rs` | coturn REST-API credentials (HMAC-SHA1 of the expiry time) |
| `tests/server.rs` | End-to-end tests over real sockets |

```bash
cargo test
```

## Before going to production

- **TURN:** run coturn (`turnserver.conf.example`) and set `TURN_URLS` + `TURN_SECRET`.
- **HTTPS/WSS:** browsers only allow camera access on HTTPS, so the frontend
  must use `wss://`. Put this server behind a TLS proxy (Caddy, nginx).
- **Auth:** anyone who can reach `/turn-credentials` can use your TURN
  bandwidth. Once accounts exist, require a login for it and for `/ws`.
- **One instance only:** rooms live in memory. Running several instances needs
  sticky routing by room, or Redis pub/sub between them.
