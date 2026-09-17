//! Wire protocol types and constants, mirroring `relay_protocol.json`
//! (vendored at the repo root) and Go's `server/relay_protocol_gen.go`.
//!
//! `protocolVersion = 2` only — this relay does not speak the legacy v0
//! protocol.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_with::skip_serializing_none;

pub const RELAY_PROTOCOL_VERSION: u32 = 2;

pub const MAX_ROOM_SIZE: usize = 8;
pub const MAX_MESSAGE_SIZE: usize = 65536;
pub const MAX_SESSION_ID_LENGTH: usize = 64;
pub const MAX_PEER_ID_LENGTH: usize = 128;
pub const RECONNECT_TOKEN_BYTES: usize = 32;

// Client -> server message types.
pub mod client_type {
    pub const CREATE: &str = "create";
    pub const JOIN: &str = "join";
    pub const RESUME: &str = "resume";
    pub const BROADCAST: &str = "broadcast";
    pub const SEND_TO: &str = "sendTo";
    pub const PING: &str = "ping";
    pub const LEAVE: &str = "leave";
    pub const END_SESSION: &str = "endSession";
    pub const TRANSFER_HOST: &str = "transferHost";
}

// Server -> client message types.
pub mod server_type {
    pub const CREATED: &str = "created";
    pub const JOINED: &str = "joined";
    pub const RESUMED: &str = "resumed";
    pub const PEER_JOINED: &str = "peerJoined";
    pub const PEER_LEFT: &str = "peerLeft";
    pub const MESSAGE: &str = "message";
    pub const ERROR: &str = "error";
    pub const PONG: &str = "pong";
    pub const LEFT: &str = "left";
    pub const ENDED: &str = "ended";
    pub const HOST_CHANGED: &str = "hostChanged";
    pub const HOST_TRANSFER_ELIGIBILITY: &str = "hostTransferEligibility";
}

pub mod error_code {
    pub const RATE_LIMITED: &str = "rate_limited";
    pub const INVALID_MESSAGE: &str = "invalid_message";
    pub const ROOM_EXISTS: &str = "room_exists";
    pub const ROOM_NOT_FOUND: &str = "room_not_found";
    pub const ROOM_FULL: &str = "room_full";
    pub const NOT_IN_ROOM: &str = "not_in_room";
    pub const ALREADY_IN_ROOM: &str = "already_in_room";
    pub const PEER_ID_UNAVAILABLE: &str = "peer_id_unavailable";
    pub const PROTOCOL_MISMATCH: &str = "protocol_mismatch";
    pub const NOT_HOST: &str = "not_host";
    pub const PEER_NOT_FOUND: &str = "peer_not_found";
    pub const HOST_TRANSFER_UNAVAILABLE: &str = "host_transfer_unavailable";
}

pub mod feature {
    pub const ATOMIC_HOST_TRANSFER: &str = "atomicHostTransfer";
    pub const AUTHENTICATED_RESUME: &str = "authenticatedResume";
}

pub mod capability {
    pub const HOST_TRANSFER: &str = "hostTransfer";
}

pub const RELAY_FEATURES: [&str; 2] = [feature::ATOMIC_HOST_TRANSFER, feature::AUTHENTICATED_RESUME];

pub fn relay_features() -> Vec<String> {
    RELAY_FEATURES.iter().map(|s| s.to_string()).collect()
}

/// A message received from a client. One flat struct covers every message
/// type (mirrors Go's `clientMsg`) since the field set only partially varies
/// per type and there's no discriminated-union-friendly tagging in the wire
/// format.
#[skip_serializing_none]
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ClientMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub peer_id: String,
    #[serde(default)]
    pub reconnect_token: String,
    #[serde(default)]
    pub protocol_version: u32,
    #[serde(default)]
    pub sync_protocol_version: i32,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub to: String,
    pub payload: Option<Box<RawValue>>,
}

/// A message sent to a client. Mirrors Go's `serverMsg`; fields are omitted
/// from the JSON when absent (`skip_serializing_none`), matching Go's
/// `omitempty`.
///
/// `host_transfer_targets` is a plain `Option<Vec<String>>`. Go's field is a
/// `*[]string` with `omitempty`, which in Go only distinguishes "omitted"
/// (nil pointer) from "present array, possibly empty" (non-nil pointer) —
/// Go's own code never actually emits a bare JSON `null` for it, since
/// `publishHostTransferEligibilityLocked` either doesn't construct the
/// message at all (no capability-holding peers to notify) or constructs it
/// with an always-non-nil (possibly empty) slice. So the real contract is
/// two-state, not three: whenever `host_transfer.rs` actually builds and
/// sends a `hostTransferEligibility` message, this field must be
/// `Some(vec![...])` (never `None`) — "should this message be sent at all"
/// is a decision made at the call site, not encoded in this field.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ServerMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub session_id: Option<String>,
    pub peer_id: Option<String>,
    pub host_peer_id: Option<String>,
    pub reconnect_token: Option<String>,
    #[serde(skip_serializing_if = "is_zero_version")]
    pub protocol_version: u32,
    pub from: Option<String>,
    pub peers: Option<Vec<String>>,
    pub features: Option<Vec<String>>,
    pub host_transfer_targets: Option<Vec<String>>,
    pub code: Option<String>,
    pub message: Option<String>,
    pub payload: Option<Box<RawValue>>,
}

fn is_zero_version(v: &u32) -> bool {
    *v == 0
}

impl ServerMessage {
    pub fn error(code: &str, message: impl Into<String>) -> Self {
        Self {
            msg_type: server_type::ERROR.to_string(),
            code: Some(code.to_string()),
            message: Some(message.into()),
            ..Default::default()
        }
    }
}

pub fn supported_protocol_version(version: u32) -> bool {
    version == RELAY_PROTOCOL_VERSION
}
