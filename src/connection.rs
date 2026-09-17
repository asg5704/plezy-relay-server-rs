//! Per-WebSocket connection actor: a read loop (this function) plus a
//! spawned write task, communicating over a bounded channel. Mirrors Go's
//! `Client`/`writePump` split — the write task owns the socket's write
//! half and the 30s ping interval so app frames and pings never interleave
//! from two writers; the read loop decodes inbound JSON, enforces the
//! per-connection message rate limiter, and dispatches into
//! `registry.rs`/`room.rs`.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message as WsMessage, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::ClientError;
use crate::ids::valid_id;
use crate::protocol::{self, client_type, server_type, ClientMessage, ServerMessage, MAX_MESSAGE_SIZE, MAX_PEER_ID_LENGTH};
use crate::quotas::{TokenBucket, PER_CONNECTION_MESSAGE_BURST, PER_CONNECTION_MESSAGE_SUSTAINED};
use crate::room::{valid_session_and_peer_id, Admission, Effects, Room};
use crate::AppState;

const OUTBOUND_CHANNEL_CAPACITY: usize = 64;
const PING_INTERVAL: Duration = Duration::from_secs(30);
const PONG_WAIT: Duration = Duration::from_secs(60);

pub async fn run(socket: WebSocket, app: AppState, ip: IpAddr) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(OUTBOUND_CHANNEL_CAPACITY);

    let write_task = tokio::spawn(async move {
        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await; // first tick fires immediately; skip it
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(m) => {
                            let Ok(json) = serde_json::to_string(&m) else { continue };
                            if sink.send(WsMessage::Text(json)).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                _ = ping_interval.tick() => {
                    if sink.send(WsMessage::Ping(Vec::new())).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = sink.close().await;
    });

    let mut current_room: Option<Arc<Room>> = None;
    let mut current_peer_id = String::new();
    let mut current_conn_id: u64 = 0;
    let mut current_cancel: Option<CancellationToken> = None;
    let mut msg_limiter = TokenBucket::new(PER_CONNECTION_MESSAGE_BURST, PER_CONNECTION_MESSAGE_SUSTAINED);
    let mut deadline = Instant::now() + PONG_WAIT;

    'read: loop {
        let cancelled = async {
            match &current_cancel {
                Some(c) => c.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            _ = cancelled => break 'read,
            _ = tokio::time::sleep_until(deadline.into()) => break 'read,
            frame = stream.next() => {
                let Some(frame) = frame else { break 'read };
                let Ok(frame) = frame else { break 'read };
                deadline = Instant::now() + PONG_WAIT;
                match frame {
                    WsMessage::Close(_) => break 'read,
                    WsMessage::Ping(_) | WsMessage::Pong(_) => continue 'read,
                    WsMessage::Binary(_) => continue 'read,
                    WsMessage::Text(text) => {
                        if text.len() > MAX_MESSAGE_SIZE {
                            continue 'read;
                        }
                        if !msg_limiter.allow() {
                            let _ = tx.try_send(ClientError::RateLimited.into_message("Too many messages"));
                            continue 'read;
                        }
                        let Ok(msg) = serde_json::from_str::<ClientMessage>(&text) else {
                            let _ = tx.try_send(ClientError::InvalidMessage.into_message("Invalid JSON"));
                            continue 'read;
                        };
                        handle_message(
                            &app, &tx, ip, msg,
                            &mut current_room, &mut current_peer_id, &mut current_conn_id, &mut current_cancel,
                        ).await;
                    }
                }
            }
        }
    }

    if let Some(room) = current_room.take() {
        if !current_peer_id.is_empty() {
            let effects = room.disconnect(&current_peer_id, current_conn_id).await;
            apply_effects(&app, &tx, effects).await;
        }
    }
    drop(tx);
    write_task.abort();
}

async fn handle_message(
    app: &AppState,
    tx: &mpsc::Sender<ServerMessage>,
    ip: IpAddr,
    msg: ClientMessage,
    current_room: &mut Option<Arc<Room>>,
    current_peer_id: &mut String,
    current_conn_id: &mut u64,
    current_cancel: &mut Option<CancellationToken>,
) {
    if msg.msg_type == client_type::PING {
        let _ = tx.try_send(ServerMessage { msg_type: server_type::PONG.to_string(), ..Default::default() });
        return;
    }

    let already_in_room = current_room.is_some();
    let rejects_transition = matches!(msg.msg_type.as_str(), client_type::CREATE | client_type::JOIN | client_type::RESUME) && already_in_room;
    if rejects_transition {
        let _ = tx.try_send(ClientError::AlreadyInRoom.into_message("Leave the current room before creating or joining another"));
        return;
    }

    match msg.msg_type.as_str() {
        client_type::CREATE => {
            if !valid_session_and_peer_id(&msg.session_id, &msg.peer_id) {
                let _ = tx.try_send(ClientError::InvalidMessage.into_message("Invalid sessionId or peerId"));
                return;
            }
            if !protocol::supported_protocol_version(msg.protocol_version) {
                let _ = tx.try_send(protocol_mismatch());
                return;
            }
            let ip_key = ip.to_string();
            let admission = Admission {
                peer_id: msg.peer_id.clone(),
                tx: tx.clone(),
                sync_protocol_version: msg.sync_protocol_version,
                host_transfer_capable: msg.capabilities.iter().any(|c| c == protocol::capability::HOST_TRANSFER),
            };
            let (room, effects) = app
                .registry
                .create(&msg.session_id, &msg.peer_id, &msg.reconnect_token, &ip_key, admission)
                .await;
            if let (Some(room), Some(conn_id)) = (room, effects.assigned_conn_id) {
                *current_room = Some(room);
                *current_peer_id = msg.peer_id.clone();
                *current_conn_id = conn_id;
                *current_cancel = effects.assigned_cancel.clone();
            }
            apply_effects(app, tx, effects).await;
        }

        client_type::JOIN | client_type::RESUME => {
            let resume_only = msg.msg_type == client_type::RESUME;
            if !valid_session_and_peer_id(&msg.session_id, &msg.peer_id) {
                let _ = tx.try_send(ClientError::InvalidMessage.into_message("Invalid sessionId or peerId"));
                return;
            }
            let version_ok = protocol::supported_protocol_version(msg.protocol_version)
                && (!resume_only || msg.protocol_version == protocol::RELAY_PROTOCOL_VERSION);
            if !version_ok {
                let _ = tx.try_send(protocol_mismatch());
                return;
            }
            let Some(room) = app.registry.get(&msg.session_id).await else {
                let _ = tx.try_send(ClientError::RoomNotFound.into_message("Room does not exist"));
                return;
            };
            let admission = Admission {
                peer_id: msg.peer_id.clone(),
                tx: tx.clone(),
                sync_protocol_version: msg.sync_protocol_version,
                host_transfer_capable: msg.capabilities.iter().any(|c| c == protocol::capability::HOST_TRANSFER),
            };
            let effects = room.join_or_resume(&msg, admission, resume_only).await;
            if let Some(conn_id) = effects.assigned_conn_id {
                *current_peer_id = msg.peer_id.clone();
                *current_conn_id = conn_id;
                *current_cancel = effects.assigned_cancel.clone();
                *current_room = Some(room);
            }
            apply_effects(app, tx, effects).await;
        }

        client_type::LEAVE => {
            let Some(room) = current_room.clone() else {
                let _ = tx.try_send(ClientError::NotInRoom.into_message("Not in a room"));
                return;
            };
            let effects = room.leave(&app.snapshot, current_peer_id, *current_conn_id, &msg.reconnect_token, msg.protocol_version).await;
            let failed = effects.reply.as_ref().is_some_and(|r| r.code.is_some());
            if !failed {
                *current_room = None;
                *current_peer_id = String::new();
                *current_cancel = None;
            }
            apply_effects(app, tx, effects).await;
        }

        client_type::END_SESSION => {
            let Some(room) = current_room.clone() else {
                let _ = tx.try_send(ClientError::NotInRoom.into_message("Not in a room"));
                return;
            };
            let effects = app.registry.end_session(&room, current_peer_id, *current_conn_id, &msg.reconnect_token, msg.protocol_version).await;
            *current_room = None;
            *current_peer_id = String::new();
            *current_cancel = None;
            apply_effects(app, tx, effects).await;
        }

        client_type::TRANSFER_HOST => {
            let Some(room) = current_room.clone() else {
                let _ = tx.try_send(ClientError::NotInRoom.into_message("Not in a room"));
                return;
            };
            if !valid_id(&msg.to, MAX_PEER_ID_LENGTH) {
                let _ = tx.try_send(ClientError::InvalidMessage.into_message("Invalid to field"));
                return;
            }
            let effects = room.transfer_host(current_peer_id, *current_conn_id, msg.protocol_version, &msg.to).await;
            apply_effects(app, tx, effects).await;
        }

        client_type::BROADCAST => {
            let Some(room) = current_room.clone() else {
                let _ = tx.try_send(ClientError::NotInRoom.into_message("Not in a room"));
                return;
            };
            let effects = room.broadcast(current_peer_id, *current_conn_id, msg.payload).await;
            apply_effects(app, tx, effects).await;
        }

        client_type::SEND_TO => {
            let Some(room) = current_room.clone() else {
                let _ = tx.try_send(ClientError::NotInRoom.into_message("Not in a room"));
                return;
            };
            if !valid_id(&msg.to, MAX_PEER_ID_LENGTH) {
                let _ = tx.try_send(ClientError::InvalidMessage.into_message("Invalid to field"));
                return;
            }
            let effects = room.send_to(current_peer_id, *current_conn_id, &msg.to, msg.payload).await;
            apply_effects(app, tx, effects).await;
        }

        _ => {
            let _ = tx.try_send(ClientError::InvalidMessage.into_message("Unknown message type"));
        }
    }
}

/// Delivers a room-layer [`Effects`]: the direct reply via the caller's own
/// `tx`, broadcasts via each target's own sender, evictions by cancelling
/// their tokens, and a non-terminal snapshot notification if anything
/// changed. Terminal mutations (`leave`/`endSession`) already awaited their
/// own durability before this runs.
async fn apply_effects(app: &AppState, tx: &mpsc::Sender<ServerMessage>, effects: Effects) {
    if let Some(reply) = effects.reply {
        let _ = tx.try_send(reply);
    }
    for outbound in effects.broadcast {
        let _ = outbound.tx.try_send(outbound.msg);
    }
    for eviction in effects.evict {
        eviction.cancel.cancel();
    }
    if effects.dirty {
        app.snapshot.record_mutation();
    }
}

fn protocol_mismatch() -> ServerMessage {
    ServerMessage {
        msg_type: server_type::ERROR.to_string(),
        code: Some(ClientError::ProtocolMismatch.code().to_string()),
        message: Some("Unsupported relay protocol version".to_string()),
        protocol_version: protocol::RELAY_PROTOCOL_VERSION,
        ..Default::default()
    }
}
