//! Full end-to-end coverage over a real bound axum server, using
//! `tokio-tungstenite` as the client. Exercises the actual wire protocol
//! (JSON over a real WebSocket, real TCP) rather than calling into
//! `room.rs` directly — this is what would catch a wiring bug between
//! `connection.rs`/`ws.rs`/`registry.rs` that per-module unit tests can't
//! see.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::routing::{get, post};
use axum::Router;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tower_http::limit::RequestBodyLimitLayer;

use relay_rs::client_ip::ClientIpResolver;
use relay_rs::logs::{self, LogStore, MAX_LOG_SIZE};
use relay_rs::quotas::{ConnectionQuota, IpRateLimiter, CONNECT_ATTEMPT_BURST, CONNECT_ATTEMPT_SUSTAINED};
use relay_rs::registry::Registry;
use relay_rs::snapshot;
use relay_rs::{ws, AppState};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn token(byte: u8) -> String {
    URL_SAFE_NO_PAD.encode([byte; 32])
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_state_file() -> PathBuf {
    let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("relay-rs-ws-integration-{}-{n}.json", std::process::id()))
}

/// A real bound server, wired the same way `main.rs` wires one, minus
/// graceful shutdown (the test just aborts the task on drop).
struct TestServer {
    addr: SocketAddr,
    handle: JoinHandle<()>,
    state_file: PathBuf,
}

impl TestServer {
    fn ws_url(&self) -> String {
        format!("ws://{}/relay", self.addr)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
        let _ = std::fs::remove_file(&self.state_file);
    }
}

async fn spawn_server() -> TestServer {
    let state_file = temp_state_file();
    let (snapshot_handle, snapshot_rx) = snapshot::channel();
    let registry = Arc::new(Registry::new(snapshot_handle.clone()));
    tokio::spawn(snapshot::run(snapshot_rx, registry.clone(), state_file.clone()));

    let app = AppState {
        registry,
        logs: Arc::new(LogStore::new()),
        client_ip: Arc::new(ClientIpResolver::new(Vec::new())),
        connections: Arc::new(ConnectionQuota::default()),
        connect_limiter: Arc::new(IpRateLimiter::new(CONNECT_ATTEMPT_BURST, CONNECT_ATTEMPT_SUSTAINED)),
        snapshot: snapshot_handle,
    };

    let router = Router::new()
        .route("/relay", get(ws::ws_handler))
        .route("/logs", post(logs::post_logs).route_layer(RequestBodyLimitLayer::new(MAX_LOG_SIZE)))
        .route("/logs/:id", get(logs::get_logs))
        .route("/health", get(logs::health))
        .with_state(app);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>()).await;
    });

    TestServer { addr, handle, state_file }
}

async fn connect(server: &TestServer) -> WsStream {
    let (ws, _response) = tokio_tungstenite::connect_async(server.ws_url()).await.expect("ws upgrade should succeed");
    ws
}

async fn send_json(ws: &mut WsStream, value: Value) {
    ws.send(Message::Text(value.to_string())).await.expect("send should succeed");
}

/// Reads the next application message, transparently skipping
/// ping/pong control frames.
async fn recv_json(ws: &mut WsStream) -> Value {
    loop {
        let next = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timed out waiting for a message")
            .expect("stream ended unexpectedly")
            .expect("websocket protocol error");
        match next {
            Message::Text(text) => return serde_json::from_str(&text).expect("valid JSON"),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected non-text message: {other:?}"),
        }
    }
}

#[tokio::test]
async fn create_join_broadcast_sendto_leave_end_session_flow() {
    let server = spawn_server().await;

    let mut host = connect(&server).await;
    let host_token = token(1);
    send_json(
        &mut host,
        json!({
            "type": "create",
            "sessionId": "int-room-1",
            "peerId": "host1",
            "reconnectToken": host_token,
            "protocolVersion": 2,
        }),
    )
    .await;
    let created = recv_json(&mut host).await;
    assert_eq!(created["type"], "created");
    assert_eq!(created["hostPeerId"], "host1");

    let mut guest = connect(&server).await;
    let guest_token = token(2);
    send_json(
        &mut guest,
        json!({
            "type": "join",
            "sessionId": "int-room-1",
            "peerId": "g1",
            "reconnectToken": guest_token,
            "protocolVersion": 2,
        }),
    )
    .await;
    let joined = recv_json(&mut guest).await;
    assert_eq!(joined["type"], "joined");
    assert_eq!(joined["hostPeerId"], "host1");

    let peer_joined = recv_json(&mut host).await;
    assert_eq!(peer_joined["type"], "peerJoined");
    assert_eq!(peer_joined["peerId"], "g1");

    // broadcast: host -> every other connected peer (g1).
    send_json(&mut host, json!({"type": "broadcast", "payload": {"hello": "world"}})).await;
    let broadcast = recv_json(&mut guest).await;
    assert_eq!(broadcast["type"], "message");
    assert_eq!(broadcast["from"], "host1");
    assert_eq!(broadcast["payload"]["hello"], "world");

    // sendTo: g1 -> host1 directly, no one else should see it (there's no
    // one else in the room to accidentally see it, but we assert the
    // direct reply shape is right).
    send_json(&mut guest, json!({"type": "sendTo", "to": "host1", "payload": {"ack": true}})).await;
    let direct = recv_json(&mut host).await;
    assert_eq!(direct["type"], "message");
    assert_eq!(direct["from"], "g1");
    assert_eq!(direct["payload"]["ack"], true);

    // g1 leaves; host is told.
    send_json(&mut guest, json!({"type": "leave", "reconnectToken": guest_token, "protocolVersion": 2})).await;
    let left = recv_json(&mut guest).await;
    assert_eq!(left["type"], "left");
    assert_eq!(left["peerId"], "g1");
    let peer_left = recv_json(&mut host).await;
    assert_eq!(peer_left["type"], "peerLeft");
    assert_eq!(peer_left["peerId"], "g1");

    // host ends the session.
    send_json(&mut host, json!({"type": "endSession", "reconnectToken": host_token, "protocolVersion": 2})).await;
    let ended = recv_json(&mut host).await;
    assert_eq!(ended["type"], "ended");
    assert_eq!(ended["sessionId"], "int-room-1");
}

#[tokio::test]
async fn guest_resumes_after_abrupt_disconnect() {
    let server = spawn_server().await;

    let mut host = connect(&server).await;
    let host_token = token(1);
    send_json(
        &mut host,
        json!({"type": "create", "sessionId": "int-room-2", "peerId": "host1", "reconnectToken": host_token, "protocolVersion": 2}),
    )
    .await;
    recv_json(&mut host).await; // created

    let guest_token = token(2);
    {
        let mut guest = connect(&server).await;
        send_json(
            &mut guest,
            json!({"type": "join", "sessionId": "int-room-2", "peerId": "g1", "reconnectToken": guest_token, "protocolVersion": 2}),
        )
        .await;
        let joined = recv_json(&mut guest).await;
        assert_eq!(joined["type"], "joined");
        // Abrupt disconnect: drop the socket without sending `leave`.
    }
    recv_json(&mut host).await; // peerLeft — confirms the server noticed.

    let mut guest2 = connect(&server).await;
    send_json(
        &mut guest2,
        json!({"type": "resume", "sessionId": "int-room-2", "peerId": "g1", "reconnectToken": guest_token, "protocolVersion": 2}),
    )
    .await;
    let resumed = recv_json(&mut guest2).await;
    assert_eq!(resumed["type"], "resumed");
    assert_eq!(resumed["peers"], json!(["host1"]));
}

/// Once `MAX_CONNS_PER_IP` (or the burst on the connect-attempt limiter,
/// whichever trips first — both are real admission gates in `ws.rs`)
/// concurrent connections are held open from one IP, a further upgrade is
/// rejected with 429 rather than accepted.
#[tokio::test]
async fn connection_quota_rejects_beyond_the_per_ip_cap() {
    let server = spawn_server().await;

    let mut held = Vec::new();
    for _ in 0..relay_rs::quotas::MAX_CONNS_PER_IP {
        held.push(connect(&server).await);
    }

    match tokio_tungstenite::connect_async(server.ws_url()).await {
        Ok(_) => panic!("expected the connection to be rejected once the per-IP cap is reached"),
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            assert_eq!(response.status(), 429);
        }
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// Rapidly opening-then-closing connections still exhausts the
/// connect-attempt rate limiter (a `governor` burst+sustained bucket that
/// only refills over time, unaffected by the connection itself closing),
/// independently of the concurrent-connection quota.
#[tokio::test]
async fn connect_attempt_rate_limiter_rejects_rapid_reconnects() {
    let server = spawn_server().await;

    for _ in 0..relay_rs::quotas::CONNECT_ATTEMPT_BURST {
        let ws = connect(&server).await;
        drop(ws); // free the connection-quota slot; rate-limiter tokens don't come back
    }

    match tokio_tungstenite::connect_async(server.ws_url()).await {
        Ok(_) => panic!("expected the connect-attempt burst to be exhausted"),
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            assert_eq!(response.status(), 429);
        }
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}
