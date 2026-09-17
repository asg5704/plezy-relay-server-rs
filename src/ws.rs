//! Axum `GET /ws` handler: resolves the client IP, runs pre-upgrade
//! admission (global/per-IP connection caps, connect-attempt rate
//! limiter), then hands off to `connection::run`.

use std::net::SocketAddr;

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::logs::forwarded_for;
use crate::protocol::MAX_MESSAGE_SIZE;
use crate::{connection, AppState};

pub async fn ws_handler(
    State(app): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let Ok(ip) = app.client_ip.resolve(peer.ip(), &forwarded_for(&headers)) else {
        return (StatusCode::BAD_REQUEST, "Invalid client address").into_response();
    };
    if !app.connect_limiter.allow(ip) {
        return (StatusCode::TOO_MANY_REQUESTS, "Too many connection attempts").into_response();
    }
    if !app.connections.try_connect(ip) {
        return (StatusCode::TOO_MANY_REQUESTS, "Too many connections").into_response();
    }

    let connections = app.connections.clone();
    ws.max_message_size(MAX_MESSAGE_SIZE).on_upgrade(move |socket| async move {
        connection::run(socket, app, ip).await;
        connections.disconnect(ip);
    })
}
