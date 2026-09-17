//! Asserts `protocol.rs`'s consts/limits match the vendored
//! `relay_protocol.json` verbatim. This is the guard against exactly the
//! kind of drift that let `maxRoomSize` silently diverge (6 in the vendored
//! JSON vs. 8 in both `protocol.rs` and the canonical
//! `~/oss/plezy/relay_protocol.json`) before this test existed.

use std::collections::{HashMap, HashSet};

use relay_rs::error::ClientError;
use relay_rs::ids::valid_id;
use relay_rs::protocol::{
    capability, client_type, error_code, server_type, MAX_MESSAGE_SIZE,
    MAX_PEER_ID_LENGTH, MAX_ROOM_SIZE, MAX_SESSION_ID_LENGTH, RECONNECT_TOKEN_BYTES,
    RELAY_FEATURES, RELAY_PROTOCOL_VERSION,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct RelayProtocolSpec {
    #[serde(rename = "protocolVersion")]
    protocol_version: u32,
    #[serde(rename = "clientMessageTypes")]
    client_message_types: HashMap<String, String>,
    #[serde(rename = "serverMessageTypes")]
    server_message_types: HashMap<String, String>,
    #[serde(rename = "errorCodes")]
    error_codes: HashMap<String, String>,
    features: HashMap<String, String>,
    capabilities: HashMap<String, String>,
    limits: Limits,
    #[serde(rename = "idPattern")]
    id_pattern: String,
    #[serde(rename = "reconnectTokenBytes")]
    reconnect_token_bytes: usize,
}

#[derive(Deserialize)]
struct Limits {
    #[serde(rename = "maxRoomSize")]
    max_room_size: usize,
    #[serde(rename = "maxMessageSize")]
    max_message_size: usize,
    #[serde(rename = "maxSessionIdLength")]
    max_session_id_length: usize,
    #[serde(rename = "maxPeerIdLength")]
    max_peer_id_length: usize,
}

fn load_spec() -> RelayProtocolSpec {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/relay_protocol.json");
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("reading {path}: {e}"));
    serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("parsing {path}: {e}"))
}

fn values(map: &HashMap<String, String>) -> HashSet<&str> {
    map.values().map(String::as_str).collect()
}

#[test]
fn protocol_version_matches() {
    let spec = load_spec();
    assert_eq!(spec.protocol_version, RELAY_PROTOCOL_VERSION);
}

#[test]
fn limits_match() {
    let spec = load_spec();
    assert_eq!(spec.limits.max_room_size, MAX_ROOM_SIZE, "maxRoomSize");
    assert_eq!(spec.limits.max_message_size, MAX_MESSAGE_SIZE, "maxMessageSize");
    assert_eq!(spec.limits.max_session_id_length, MAX_SESSION_ID_LENGTH, "maxSessionIdLength");
    assert_eq!(spec.limits.max_peer_id_length, MAX_PEER_ID_LENGTH, "maxPeerIdLength");
    assert_eq!(spec.reconnect_token_bytes, RECONNECT_TOKEN_BYTES, "reconnectTokenBytes");
}

#[test]
fn client_message_types_match() {
    let spec = load_spec();
    let expected: HashSet<&str> = [
        client_type::CREATE,
        client_type::JOIN,
        client_type::RESUME,
        client_type::BROADCAST,
        client_type::SEND_TO,
        client_type::PING,
        client_type::LEAVE,
        client_type::END_SESSION,
        client_type::TRANSFER_HOST,
    ]
    .into_iter()
    .collect();
    assert_eq!(values(&spec.client_message_types), expected);
}

#[test]
fn server_message_types_match() {
    let spec = load_spec();
    let expected: HashSet<&str> = [
        server_type::CREATED,
        server_type::JOINED,
        server_type::RESUMED,
        server_type::PEER_JOINED,
        server_type::PEER_LEFT,
        server_type::MESSAGE,
        server_type::ERROR,
        server_type::PONG,
        server_type::LEFT,
        server_type::ENDED,
        server_type::HOST_CHANGED,
        server_type::HOST_TRANSFER_ELIGIBILITY,
    ]
    .into_iter()
    .collect();
    assert_eq!(values(&spec.server_message_types), expected);
}

/// Every `ClientError` variant's `code()` must appear in the JSON's
/// `errorCodes` values, and vice versa — a mismatch either way means a
/// client-visible error code drifted from the spec.
#[test]
fn error_codes_match() {
    let spec = load_spec();
    let all_errors = [
        ClientError::RateLimited,
        ClientError::InvalidMessage,
        ClientError::RoomExists,
        ClientError::RoomNotFound,
        ClientError::RoomFull,
        ClientError::NotInRoom,
        ClientError::AlreadyInRoom,
        ClientError::PeerIdUnavailable,
        ClientError::ProtocolMismatch,
        ClientError::NotHost,
        ClientError::PeerNotFound,
        ClientError::HostTransferUnavailable,
    ];
    let expected: HashSet<&str> = all_errors.iter().map(|e| e.code()).collect();
    assert_eq!(values(&spec.error_codes), expected);
    // Sanity check the individual constants used elsewhere still line up
    // with the module they're supposed to mirror.
    assert_eq!(ClientError::RateLimited.code(), error_code::RATE_LIMITED);
    assert_eq!(ClientError::HostTransferUnavailable.code(), error_code::HOST_TRANSFER_UNAVAILABLE);
}

#[test]
fn features_and_capabilities_match() {
    let spec = load_spec();
    let expected_features: HashSet<&str> = RELAY_FEATURES.into_iter().collect();
    assert_eq!(values(&spec.features), expected_features);
    let expected_capabilities: HashSet<&str> = [capability::HOST_TRANSFER].into_iter().collect();
    assert_eq!(values(&spec.capabilities), expected_capabilities);
}

/// Structural check rather than a regex-engine comparison (no `regex`
/// dependency in this crate): `idPattern` is documented as
/// `^[A-Za-z0-9_-]+$`, so `valid_id`'s per-byte predicate must accept
/// exactly the alphanumeric/underscore/hyphen character class, one char at a
/// time, for every ASCII byte.
#[test]
fn id_pattern_matches_valid_id_predicate() {
    let spec = load_spec();
    assert_eq!(spec.id_pattern, "^[A-Za-z0-9_-]+$");

    for byte in 0u8..=127 {
        let s = (byte as char).to_string();
        let matches_pattern = byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-';
        assert_eq!(
            valid_id(&s, 64),
            matches_pattern,
            "byte {byte} ({s:?}) disagreement between valid_id and idPattern"
        );
    }
}
