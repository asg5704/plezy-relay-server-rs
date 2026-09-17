//! Room state machine. `RoomState` is a plain, synchronous struct: every
//! mutating method takes `&mut self`, does no locking and no I/O, and
//! returns an [`Effects`] describing what should happen next (a reply to
//! the calling connection, messages to other connected peers, and stale
//! connections to force-evict) — the caller applies those effects (actual
//! channel sends, cancellations) *after* releasing the room lock.
//!
//! [`Room`] wraps `RoomState` in a `tokio::sync::Mutex` and is what
//! `registry.rs`/`connection.rs` actually touch. `leave` and `endSession`
//! are two-phase (`begin_*` / `finish_*`) because they must wait for
//! snapshot durability between marking a mutation pending and committing or
//! rolling it back — see `snapshot.rs`.

use std::collections::HashMap;
use std::time::{Instant, SystemTime};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::ClientError;
use crate::host_transfer::{self, PeerInfo};
use crate::ids::{valid_id, Verifier};
use crate::protocol::{
    self, relay_features, server_type, ClientMessage, ServerMessage, MAX_PEER_ID_LENGTH,
    MAX_ROOM_SIZE, MAX_SESSION_ID_LENGTH,
};

/// 5-minute grace period a disconnected guest identity's reservation
/// survives before it can be pruned and the peerId reused. Matches Go's
/// `peerReservationGrace` (== `emptyRoomMaxAge`).
pub const PEER_RESERVATION_GRACE: std::time::Duration = std::time::Duration::from_secs(5 * 60);
/// A room with no connected peers is evicted after this much idle time.
pub const EMPTY_ROOM_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(5 * 60);
/// Any room, occupied or not, is evicted once this old.
pub const ROOM_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

pub struct ConnectedPeer {
    pub tx: mpsc::Sender<ServerMessage>,
    pub sync_protocol_version: i32,
    pub host_transfer_capable: bool,
    /// Disambiguates "am I still the current occupant" across replace races
    /// (idempotent host re-`create`, join/resume reclaiming a peerId whose
    /// old socket hasn't noticed it died yet).
    pub conn_id: u64,
    pub cancel: CancellationToken,
}

#[derive(Clone)]
pub struct PeerReservation {
    pub verifier: Verifier,
    /// `None` while the identity is currently connected; set to the
    /// disconnect time otherwise, starting the grace-period clock.
    pub absent_since: Option<Instant>,
    /// Set while a `leave` is durably committing; blocks join/resume for
    /// this identity until the terminal mutation resolves either way.
    pub release_pending: bool,
}

pub struct RoomState {
    pub session_id: String,
    pub host_peer_id: String,
    pub host_verifier: Verifier,
    pub peers: HashMap<String, ConnectedPeer>,
    pub reservations: HashMap<String, PeerReservation>,
    pub owner_ip_key: String,
    pub created_at: SystemTime,
    pub last_activity_at: SystemTime,
    pub closing: bool,
    next_conn_id: u64,
}

/// A message queued for one other currently-connected peer.
pub struct Outbound {
    pub tx: mpsc::Sender<ServerMessage>,
    pub msg: ServerMessage,
}

/// A stale/replaced connection to force-close after the room lock is
/// dropped.
pub struct Eviction {
    pub cancel: CancellationToken,
}

/// What a `RoomState` method decided should happen. `reply` is delivered by
/// the caller directly via its own connection (never routed through the
/// room's peer map, since on failure the caller may not be admitted at
/// all); `broadcast` targets other peers' own senders (already known to the
/// room); `evict` cancels stale connections; `dirty` tells the caller
/// whether to notify the snapshotter of a non-terminal mutation.
#[derive(Default)]
pub struct Effects {
    pub reply: Option<ServerMessage>,
    pub broadcast: Vec<Outbound>,
    pub evict: Vec<Eviction>,
    pub dirty: bool,
    /// Set on a successful `new_room`/`reconnect_host`/`join_or_resume` to
    /// the fresh connection id assigned to the caller — the caller must
    /// remember this locally (alongside its room + peerId) and present it
    /// back on every later operation, so the room can tell "am I still the
    /// authoritative occupant" apart from a stale/replaced connection.
    pub assigned_conn_id: Option<u64>,
    /// The cancellation token for that same fresh connection — the caller
    /// should race it (`select!`) against its read/write loops and treat
    /// cancellation as "shut down, another connection has taken over this
    /// identity."
    pub assigned_cancel: Option<CancellationToken>,
}

impl Effects {
    pub(crate) fn error(err: ClientError, text: &str) -> Self {
        Self { reply: Some(err.into_message(text)), ..Default::default() }
    }
}

/// Everything needed to admit a connection into a room (as host or guest).
pub struct Admission {
    pub peer_id: String,
    pub tx: mpsc::Sender<ServerMessage>,
    pub sync_protocol_version: i32,
    pub host_transfer_capable: bool,
}

impl RoomState {
    pub fn new_room(session_id: String, host_peer_id: String, host_verifier: Verifier, owner_ip_key: String, host: Admission, now: SystemTime) -> (Self, Effects) {
        let conn_id = 1;
        let cancel = CancellationToken::new();
        let mut peers = HashMap::new();
        peers.insert(
            host_peer_id.clone(),
            ConnectedPeer {
                tx: host.tx,
                sync_protocol_version: host.sync_protocol_version,
                host_transfer_capable: host.host_transfer_capable,
                conn_id,
                cancel: cancel.clone(),
            },
        );
        let state = RoomState {
            session_id: session_id.clone(),
            host_peer_id: host_peer_id.clone(),
            host_verifier,
            peers,
            reservations: HashMap::new(),
            owner_ip_key,
            created_at: now,
            last_activity_at: now,
            closing: false,
            next_conn_id: conn_id + 1,
        };
        let mut effects = Effects {
            reply: Some(ServerMessage {
                msg_type: server_type::CREATED.to_string(),
                session_id: Some(session_id),
                host_peer_id: Some(host_peer_id),
                protocol_version: protocol::RELAY_PROTOCOL_VERSION,
                features: Some(relay_features()),
                ..Default::default()
            }),
            dirty: true,
            assigned_conn_id: Some(conn_id),
            assigned_cancel: Some(cancel),
            ..Default::default()
        };
        state.append_host_transfer_eligibility(&mut effects);
        (state, effects)
    }

    /// Reconstructs a room from a validated snapshot entry at startup — no
    /// connected peers (those never survive a restart), reservations
    /// carried over as-is.
    pub fn restore(
        session_id: String,
        host_peer_id: String,
        host_verifier: Verifier,
        owner_ip_key: String,
        reservations: HashMap<String, crate::snapshot::RestoredReservation>,
        created_at: SystemTime,
        last_activity_at: SystemTime,
    ) -> Self {
        let reservations = reservations
            .into_iter()
            .map(|(peer_id, r)| (peer_id, PeerReservation { verifier: r.verifier, absent_since: r.absent_since, release_pending: false }))
            .collect();
        RoomState {
            session_id,
            host_peer_id,
            host_verifier,
            peers: HashMap::new(),
            reservations,
            owner_ip_key,
            created_at,
            last_activity_at,
            closing: false,
            next_conn_id: 1,
        }
    }

    /// Idempotent host re-`create`: same sessionId + hostPeerId + a
    /// presented token whose verifier matches the room's existing host
    /// verifier. Admits this connection as host, replacing any prior live
    /// connection under that peerId. No quota/registry changes — the room
    /// already exists.
    pub fn reconnect_host(&mut self, host: Admission, _now: SystemTime) -> Effects {
        let was_absent = !self.peers.contains_key(&self.host_peer_id);
        let host_peer_id = self.host_peer_id.clone();
        let (conn_id, cancel, old) = self.admit(host_peer_id, host);

        let existing_peers: Vec<String> =
            self.peers.keys().filter(|id| **id != self.host_peer_id).cloned().collect();
        let mut effects = Effects {
            reply: Some(ServerMessage {
                msg_type: server_type::CREATED.to_string(),
                session_id: Some(self.session_id.clone()),
                host_peer_id: Some(self.host_peer_id.clone()),
                protocol_version: protocol::RELAY_PROTOCOL_VERSION,
                peers: Some(existing_peers),
                features: Some(relay_features()),
                ..Default::default()
            }),
            dirty: true,
            assigned_conn_id: Some(conn_id),
            assigned_cancel: Some(cancel),
            evict: old.into_iter().map(|p| Eviction { cancel: p.cancel }).collect(),
            ..Default::default()
        };
        if was_absent {
            self.broadcast_except(&self.host_peer_id.clone(), peer_joined(&self.host_peer_id), &mut effects);
        }
        self.append_host_transfer_eligibility(&mut effects);
        effects
    }

    /// True iff this room is empty (no connected peers) and not already
    /// closing — the condition under which `create` may reclaim an
    /// abandoned sessionId rather than reject with `room_exists`.
    pub fn is_reclaimable(&self) -> bool {
        !self.closing && self.peers.is_empty()
    }

    pub fn join_or_resume(
        &mut self,
        msg: &ClientMessage,
        admission: Admission,
        resume_only: bool,
        now: SystemTime,
    ) -> Effects {
        if self.closing {
            return Effects::error(ClientError::RoomNotFound, "Room does not exist");
        }
        self.prune_expired_reservations();

        let peer_id = admission.peer_id.clone();
        let presented = Verifier::from_token(&msg.reconnect_token);
        let occupied = self.peers.contains_key(&peer_id);
        let reservation = self.reservations.get(&peer_id).cloned();

        let authorized = if peer_id == self.host_peer_id {
            presented.map(|v| self.host_verifier.matches(&v)).unwrap_or(false)
        } else if let Some(res) = &reservation {
            !res.release_pending && presented.map(|v| res.verifier.matches(&v)).unwrap_or(false)
        } else if occupied {
            false
        } else {
            // Brand-new identity: only a plain `join` may allocate one, and
            // only with a structurally valid token (its bytes become the
            // reservation's verifier).
            !resume_only && presented.is_some()
        };
        if !authorized {
            return Effects::error(ClientError::PeerIdUnavailable, "Peer ID is unavailable");
        }

        if !occupied {
            let is_new_identity = peer_id != self.host_peer_id && reservation.is_none();
            if is_new_identity {
                let guest_count = self.reservations.len();
                if guest_count >= MAX_ROOM_SIZE - 1 {
                    return Effects::error(ClientError::RoomFull, "Room is full");
                }
            }
        }

        let verifier = presented.unwrap_or_else(|| {
            // Host/resume paths always had a matching presented token to get
            // this far; this branch is unreachable in practice but keeps
            // the function total without a panic.
            reservation.as_ref().map(|r| r.verifier).unwrap_or(self.host_verifier)
        });

        let (conn_id, cancel, old) = self.admit(peer_id.clone(), admission);
        if peer_id != self.host_peer_id {
            self.reservations.insert(
                peer_id.clone(),
                PeerReservation { verifier, absent_since: None, release_pending: false },
            );
        }
        self.last_activity_at = now;

        let existing_peers: Vec<String> = self.peers.keys().filter(|id| **id != peer_id).cloned().collect();
        let response_type = if resume_only { server_type::RESUMED } else { server_type::JOINED };
        let mut effects = Effects {
            reply: Some(ServerMessage {
                msg_type: response_type.to_string(),
                session_id: Some(self.session_id.clone()),
                host_peer_id: Some(self.host_peer_id.clone()),
                reconnect_token: Some(msg.reconnect_token.clone()),
                protocol_version: protocol::RELAY_PROTOCOL_VERSION,
                peers: Some(existing_peers),
                features: Some(relay_features()),
                ..Default::default()
            }),
            dirty: true,
            assigned_conn_id: Some(conn_id),
            assigned_cancel: Some(cancel),
            evict: old.into_iter().map(|p| Eviction { cancel: p.cancel }).collect(),
            ..Default::default()
        };
        self.broadcast_except(&peer_id, peer_joined(&peer_id), &mut effects);
        self.append_host_transfer_eligibility(&mut effects);
        effects
    }

    /// Phase 1 of `leave`: validates authorization and marks the guest's
    /// reservation release-pending. Returns the peerId whose release is now
    /// pending, or an error effect if unauthorized.
    pub fn begin_leave(&mut self, peer_id: &str, conn_id: u64, token: &str, protocol_version: u32) -> Result<(), Effects> {
        let is_current = self.peers.get(peer_id).map(|p| p.conn_id) == Some(conn_id);
        let is_guest = peer_id != self.host_peer_id;
        if self.closing || !is_current || !is_guest || protocol_version != protocol::RELAY_PROTOCOL_VERSION {
            return Err(Effects::error(ClientError::PeerIdUnavailable, "Unable to release peer identity"));
        }
        let presented = Verifier::from_token(token);
        let authorized = self.reservations.get(peer_id).map_or(false, |res| {
            !res.release_pending && presented.map(|v| res.verifier.matches(&v)).unwrap_or(false)
        });
        if !authorized {
            return Err(Effects::error(ClientError::PeerIdUnavailable, "Unable to release peer identity"));
        }
        if let Some(res) = self.reservations.get_mut(peer_id) {
            res.release_pending = true;
        }
        Ok(())
    }

    /// Phase 2 of `leave`, called after the snapshot write either succeeded
    /// or failed. On success, the identity is fully freed; on failure, the
    /// release-pending flag is rolled back and the client is told the leave
    /// failed rather than silently left in limbo.
    pub fn finish_leave(&mut self, peer_id: &str, conn_id: u64, persisted: bool, now: SystemTime) -> Effects {
        let still_owns = self.peers.get(peer_id).map(|p| p.conn_id) == Some(conn_id)
            && self.reservations.get(peer_id).map_or(false, |r| r.release_pending);
        if !still_owns {
            // Another mutation (e.g. a forced eviction) beat us to it; the
            // room has already moved on, nothing left to commit or roll
            // back for this specific attempt.
            return Effects::default();
        }
        if !persisted {
            if let Some(res) = self.reservations.get_mut(peer_id) {
                res.release_pending = false;
            }
            return Effects::error(ClientError::InvalidMessage, "Unable to persist released peer identity");
        }

        self.peers.remove(peer_id);
        self.reservations.remove(peer_id);
        self.last_activity_at = now;

        let mut effects = Effects {
            reply: Some(ServerMessage {
                msg_type: server_type::LEFT.to_string(),
                session_id: Some(self.session_id.clone()),
                peer_id: Some(peer_id.to_string()),
                protocol_version: protocol::RELAY_PROTOCOL_VERSION,
                ..Default::default()
            }),
            dirty: true,
            ..Default::default()
        };
        self.broadcast_except(peer_id, peer_left(peer_id), &mut effects);
        self.append_host_transfer_eligibility(&mut effects);
        effects
    }

    /// Phase 1 of `endSession`: validates the caller is the connected host,
    /// marks the room closing, and returns the guest connections to notify
    /// once persistence resolves.
    pub fn begin_end_session(&mut self, peer_id: &str, conn_id: u64, token: &str, protocol_version: u32) -> Result<(String, Vec<mpsc::Sender<ServerMessage>>), Effects> {
        let is_current_host =
            peer_id == self.host_peer_id && self.peers.get(peer_id).map(|p| p.conn_id) == Some(conn_id);
        if self.closing || !is_current_host || protocol_version != protocol::RELAY_PROTOCOL_VERSION {
            return Err(Effects::error(ClientError::PeerIdUnavailable, "Unable to end room"));
        }
        let presented = Verifier::from_token(token);
        let authorized = presented.map(|v| self.host_verifier.matches(&v)).unwrap_or(false);
        if !authorized {
            return Err(Effects::error(ClientError::PeerIdUnavailable, "Unable to end room"));
        }
        self.closing = true;
        let guests = self
            .peers
            .iter()
            .filter(|(id, _)| **id != peer_id)
            .map(|(_, p)| p.tx.clone())
            .collect();
        Ok((self.owner_ip_key.clone(), guests))
    }

    /// Phase 2 of `endSession`. On success, sends `ended` to host + guests
    /// and evicts everyone. On persist failure, tells the host it failed
    /// and just tears down guests without an `ended` message (matching
    /// Go's failure path).
    pub fn finish_end_session(&mut self, guests: Vec<mpsc::Sender<ServerMessage>>, persisted: bool) -> Effects {
        let evict: Vec<Eviction> =
            self.peers.values().map(|p| Eviction { cancel: p.cancel.clone() }).collect();
        self.peers.clear();
        self.reservations.clear();

        if !persisted {
            return Effects {
                reply: Some(ClientError::InvalidMessage.into_message("Unable to persist ended room")),
                evict,
                ..Default::default()
            };
        }
        let ended = ServerMessage {
            msg_type: server_type::ENDED.to_string(),
            session_id: Some(self.session_id.clone()),
            protocol_version: protocol::RELAY_PROTOCOL_VERSION,
            ..Default::default()
        };
        Effects {
            reply: Some(ended.clone()),
            broadcast: guests.into_iter().map(|tx| Outbound { tx, msg: ended.clone() }).collect(),
            evict,
            dirty: false,
            assigned_conn_id: None,
            assigned_cancel: None,
        }
    }

    pub fn transfer_host(&mut self, peer_id: &str, conn_id: u64, protocol_version: u32, target_peer_id: &str) -> Effects {
        let is_current = self.peers.get(peer_id).map(|p| p.conn_id) == Some(conn_id);
        if self.closing || !is_current || protocol_version != protocol::RELAY_PROTOCOL_VERSION {
            return Effects::error(ClientError::PeerIdUnavailable, "Unable to transfer host authority");
        }
        if peer_id != self.host_peer_id {
            return Effects::error(ClientError::NotHost, "Only the host can transfer host authority");
        }
        if !valid_id(target_peer_id, MAX_PEER_ID_LENGTH) || target_peer_id == peer_id || !self.peers.contains_key(target_peer_id) {
            return Effects::error(ClientError::PeerNotFound, "Target peer not found");
        }
        let target_reserved_ok = self
            .reservations
            .get(target_peer_id)
            .map_or(false, |r| !r.release_pending);
        if !target_reserved_ok {
            return Effects::error(ClientError::PeerNotFound, "Target peer not found");
        }
        if !self.can_transfer_host_to(target_peer_id) {
            return Effects::error(ClientError::HostTransferUnavailable, "The current room roster cannot follow a host transfer");
        }

        let old_host = self.host_peer_id.clone();
        let target_reservation = self.reservations.remove(target_peer_id).expect("checked above");
        self.reservations.insert(
            old_host.clone(),
            PeerReservation { verifier: self.host_verifier, absent_since: None, release_pending: false },
        );
        self.host_verifier = target_reservation.verifier;
        self.host_peer_id = target_peer_id.to_string();

        let host_changed = ServerMessage {
            msg_type: server_type::HOST_CHANGED.to_string(),
            session_id: Some(self.session_id.clone()),
            host_peer_id: Some(target_peer_id.to_string()),
            from: Some(old_host),
            ..Default::default()
        };
        let mut effects = Effects { dirty: true, ..Default::default() };
        self.broadcast_all(host_changed, &mut effects);
        self.append_host_transfer_eligibility(&mut effects);
        effects
    }

    pub fn broadcast(&mut self, sender_id: &str, conn_id: u64, payload: Option<Box<serde_json::value::RawValue>>) -> Effects {
        if self.peers.get(sender_id).map(|p| p.conn_id) != Some(conn_id) {
            return Effects::default();
        }
        if self.closing {
            return Effects::default();
        }
        self.last_activity_at = SystemTime::now();
        let msg = ServerMessage { msg_type: server_type::MESSAGE.to_string(), from: Some(sender_id.to_string()), payload, ..Default::default() };
        let mut effects = Effects::default();
        self.broadcast_except(sender_id, msg, &mut effects);
        effects
    }

    pub fn send_to(&mut self, sender_id: &str, conn_id: u64, target_id: &str, payload: Option<Box<serde_json::value::RawValue>>) -> Effects {
        if self.peers.get(sender_id).map(|p| p.conn_id) != Some(conn_id) {
            return Effects::default();
        }
        if self.closing {
            return Effects::default();
        }
        let Some(target) = self.peers.get(target_id) else {
            return Effects::error(ClientError::NotInRoom, "Target peer not found");
        };
        self.last_activity_at = SystemTime::now();
        let msg = ServerMessage { msg_type: server_type::MESSAGE.to_string(), from: Some(sender_id.to_string()), payload, ..Default::default() };
        Effects { broadcast: vec![Outbound { tx: target.tx.clone(), msg }], ..Default::default() }
    }

    /// Called on ungraceful disconnect (socket error/close/timeout) for the
    /// connection that was the authoritative occupant of `peer_id`. Not a
    /// terminal mutation — no client is waiting on a reply.
    pub fn handle_disconnect(&mut self, peer_id: &str, conn_id: u64, now: SystemTime) -> Effects {
        if self.closing || self.peers.get(peer_id).map(|p| p.conn_id) != Some(conn_id) {
            return Effects::default();
        }
        self.peers.remove(peer_id);
        if peer_id != self.host_peer_id {
            if let Some(res) = self.reservations.get_mut(peer_id) {
                res.absent_since = Some(Instant::now());
            }
        }
        self.last_activity_at = now;
        let mut effects = Effects { dirty: true, ..Default::default() };
        self.broadcast_except(peer_id, peer_left(peer_id), &mut effects);
        self.append_host_transfer_eligibility(&mut effects);
        effects
    }

    pub fn prune_expired_reservations(&mut self) -> bool {
        let peers = &self.peers;
        let before = self.reservations.len();
        self.reservations.retain(|peer_id, res| {
            if res.release_pending || peers.contains_key(peer_id) {
                return true;
            }
            match res.absent_since {
                None => true,
                Some(absent_since) => {
                    Instant::now().saturating_duration_since(absent_since) < PEER_RESERVATION_GRACE
                }
            }
        });
        before != self.reservations.len()
    }

    pub fn connected_peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn idle_for(&self, now: SystemTime) -> std::time::Duration {
        now.duration_since(self.last_activity_at).unwrap_or_default()
    }

    pub fn age(&self, now: SystemTime) -> std::time::Duration {
        now.duration_since(self.created_at).unwrap_or_default()
    }

    /// Replaces any existing connection under `peer_id` (returning it for
    /// the caller to evict) and installs the new one with a fresh
    /// connection id (also returned, so the caller can remember it).
    fn admit(&mut self, peer_id: String, admission: Admission) -> (u64, CancellationToken, Option<ConnectedPeer>) {
        let conn_id = self.next_conn_id;
        self.next_conn_id += 1;
        let cancel = CancellationToken::new();
        let old = self.peers.insert(
            peer_id,
            ConnectedPeer {
                tx: admission.tx,
                sync_protocol_version: admission.sync_protocol_version,
                host_transfer_capable: admission.host_transfer_capable,
                conn_id,
                cancel: cancel.clone(),
            },
        );
        (conn_id, cancel, old)
    }

    fn broadcast_except(&self, exclude: &str, msg: ServerMessage, effects: &mut Effects) {
        for (id, peer) in &self.peers {
            if id != exclude {
                effects.broadcast.push(Outbound { tx: peer.tx.clone(), msg: msg.clone() });
            }
        }
    }

    fn broadcast_all(&self, msg: ServerMessage, effects: &mut Effects) {
        for peer in self.peers.values() {
            effects.broadcast.push(Outbound { tx: peer.tx.clone(), msg: msg.clone() });
        }
    }

    fn peer_infos(&self) -> Vec<PeerInfo<'_>> {
        self.peers
            .iter()
            .map(|(id, p)| PeerInfo { peer_id: id, sync_protocol_version: p.sync_protocol_version, host_transfer_capable: p.host_transfer_capable })
            .collect()
    }

    fn can_transfer_host_to(&self, target_peer_id: &str) -> bool {
        let peers = self.peer_infos();
        let reservations = &self.reservations;
        host_transfer::is_valid_target(&self.host_peer_id, &peers, |id| {
            reservations.get(id).map_or(false, |r| !r.release_pending)
        }, target_peer_id)
    }

    fn append_host_transfer_eligibility(&self, effects: &mut Effects) {
        let peers = self.peer_infos();
        let reservations = &self.reservations;
        let Some((recipients, targets)) = host_transfer::compute_eligibility(&self.host_peer_id, &peers, |id| {
            reservations.get(id).map_or(false, |r| !r.release_pending)
        }) else {
            return;
        };
        let msg = ServerMessage {
            msg_type: server_type::HOST_TRANSFER_ELIGIBILITY.to_string(),
            session_id: Some(self.session_id.clone()),
            host_peer_id: Some(self.host_peer_id.clone()),
            host_transfer_targets: Some(targets),
            ..Default::default()
        };
        for recipient in recipients {
            if let Some(peer) = self.peers.get(&recipient) {
                effects.broadcast.push(Outbound { tx: peer.tx.clone(), msg: msg.clone() });
            }
        }
    }
}

fn peer_joined(peer_id: &str) -> ServerMessage {
    ServerMessage { msg_type: server_type::PEER_JOINED.to_string(), peer_id: Some(peer_id.to_string()), ..Default::default() }
}

fn peer_left(peer_id: &str) -> ServerMessage {
    ServerMessage { msg_type: server_type::PEER_LEFT.to_string(), peer_id: Some(peer_id.to_string()), ..Default::default() }
}

pub fn valid_session_and_peer_id(session_id: &str, peer_id: &str) -> bool {
    valid_id(session_id, MAX_SESSION_ID_LENGTH) && valid_id(peer_id, MAX_PEER_ID_LENGTH)
}

/// Async wrapper: the actual `Arc<Room>` handle held by the registry and by
/// connections that are members of it. All locking/`.await` orchestration
/// (including the two-phase terminal mutations) lives here, one layer above
/// the synchronous `RoomState`.
pub struct Room {
    pub session_id: String,
    pub state: tokio::sync::Mutex<RoomState>,
}

impl Room {
    pub fn new(state: RoomState) -> Self {
        Self { session_id: state.session_id.clone(), state: tokio::sync::Mutex::new(state) }
    }

    pub async fn join_or_resume(&self, msg: &ClientMessage, admission: Admission, resume_only: bool) -> Effects {
        let mut state = self.state.lock().await;
        state.join_or_resume(msg, admission, resume_only, SystemTime::now())
    }

    pub async fn transfer_host(&self, peer_id: &str, conn_id: u64, protocol_version: u32, target_peer_id: &str) -> Effects {
        let mut state = self.state.lock().await;
        state.transfer_host(peer_id, conn_id, protocol_version, target_peer_id)
    }

    pub async fn broadcast(&self, sender_id: &str, conn_id: u64, payload: Option<Box<serde_json::value::RawValue>>) -> Effects {
        let mut state = self.state.lock().await;
        state.broadcast(sender_id, conn_id, payload)
    }

    pub async fn send_to(&self, sender_id: &str, conn_id: u64, target_id: &str, payload: Option<Box<serde_json::value::RawValue>>) -> Effects {
        let mut state = self.state.lock().await;
        state.send_to(sender_id, conn_id, target_id, payload)
    }

    pub async fn disconnect(&self, peer_id: &str, conn_id: u64) -> Effects {
        let mut state = self.state.lock().await;
        state.handle_disconnect(peer_id, conn_id, SystemTime::now())
    }

    /// Two-phase `leave`: marks the reservation release-pending, waits for
    /// that to be durable via `snap`, then commits (frees the identity) or
    /// rolls back, replying only once the outcome is known. See the
    /// module-level doc comment and `snapshot.rs`.
    pub async fn leave(
        &self,
        snap: &crate::snapshot::SnapshotHandle,
        peer_id: &str,
        conn_id: u64,
        token: &str,
        protocol_version: u32,
    ) -> Effects {
        {
            let mut state = self.state.lock().await;
            if let Err(effects) = state.begin_leave(peer_id, conn_id, token, protocol_version) {
                return effects;
            }
        }
        // Room lock is dropped across this await, as required — the
        // snapshotter runs entirely independently of any room's mutex.
        let persisted = snap.record_terminal_mutation().await.is_ok();
        let mut state = self.state.lock().await;
        state.finish_leave(peer_id, conn_id, persisted, SystemTime::now())
    }
}
