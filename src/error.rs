//! Protocol-level errors: map straight onto `protocol::error_code` values so
//! every call site that needs to reply with an `error` message can do so via
//! [`ClientError::into_message`].

use crate::protocol::{error_code, ServerMessage};

#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum ClientError {
    #[error("rate limited")]
    RateLimited,
    #[error("invalid message")]
    InvalidMessage,
    #[error("room already exists")]
    RoomExists,
    #[error("room does not exist")]
    RoomNotFound,
    #[error("room is full")]
    RoomFull,
    #[error("not in a room")]
    NotInRoom,
    #[error("already in a room")]
    AlreadyInRoom,
    #[error("peer id unavailable")]
    PeerIdUnavailable,
    #[error("protocol mismatch")]
    ProtocolMismatch,
    #[error("not host")]
    NotHost,
    #[error("peer not found")]
    PeerNotFound,
    #[error("host transfer unavailable")]
    HostTransferUnavailable,
}

impl ClientError {
    pub fn code(self) -> &'static str {
        match self {
            ClientError::RateLimited => error_code::RATE_LIMITED,
            ClientError::InvalidMessage => error_code::INVALID_MESSAGE,
            ClientError::RoomExists => error_code::ROOM_EXISTS,
            ClientError::RoomNotFound => error_code::ROOM_NOT_FOUND,
            ClientError::RoomFull => error_code::ROOM_FULL,
            ClientError::NotInRoom => error_code::NOT_IN_ROOM,
            ClientError::AlreadyInRoom => error_code::ALREADY_IN_ROOM,
            ClientError::PeerIdUnavailable => error_code::PEER_ID_UNAVAILABLE,
            ClientError::ProtocolMismatch => error_code::PROTOCOL_MISMATCH,
            ClientError::NotHost => error_code::NOT_HOST,
            ClientError::PeerNotFound => error_code::PEER_NOT_FOUND,
            ClientError::HostTransferUnavailable => error_code::HOST_TRANSFER_UNAVAILABLE,
        }
    }

    pub fn into_message(self, text: impl Into<String>) -> ServerMessage {
        ServerMessage::error(self.code(), text)
    }
}

/// Failure resolving a client's source IP (malformed/oversized
/// `X-Forwarded-For`, unparsable `RemoteAddr`) — maps to an HTTP 400 at the
/// admission boundary, never a protocol-level `error` frame.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("invalid client address")]
pub struct ClientIpError;

/// Snapshot persistence failure, surfaced to `leave`/`endSession` callers so
/// they can roll back rather than tell the client an unsaved change
/// succeeded.
#[derive(Debug, Clone, thiserror::Error)]
#[error("snapshot persistence failed: {0}")]
pub struct SnapshotError(pub String);
