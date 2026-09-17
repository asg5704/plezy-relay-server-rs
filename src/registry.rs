//! Global room table + per-owner-IP room-creation quota. `create()` is the
//! one place the plan's lock-ordering exception lives: the registry's
//! write lock is held for the whole decision (idempotent re-`create` vs.
//! empty-room reclaim vs. `room_exists` vs. fresh insert), nested with a
//! room's own lock for the idempotent/reclaim checks — safe here because
//! `create` is not a hot path and nothing in the critical section
//! `.await`s (RoomState's methods are synchronous).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::RwLock;

use crate::error::ClientError;
use crate::ids::Verifier;
use crate::room::{Admission, Effects, Room, RoomState};
use crate::snapshot::{self, RestoredRoom, RoomDto, SnapshotHandle};

pub const MAX_ROOMS_PER_IP: usize = 3;
pub const MAX_RETAINED_ROOMS: usize = 2000;

pub struct Registry {
    rooms: RwLock<HashMap<String, Arc<Room>>>,
    rooms_per_ip: tokio::sync::Mutex<HashMap<String, usize>>,
    pub snapshot: SnapshotHandle,
}

impl Registry {
    pub fn new(snapshot: SnapshotHandle) -> Self {
        Self { rooms: RwLock::new(HashMap::new()), rooms_per_ip: tokio::sync::Mutex::new(HashMap::new()), snapshot }
    }

    /// Restores rooms loaded from a snapshot file at startup. Must be
    /// called before any client traffic is admitted.
    pub async fn restore(&self, restored: Vec<RestoredRoom>) {
        let mut rooms = self.rooms.write().await;
        let mut quota = self.rooms_per_ip.lock().await;
        for r in restored {
            let owner_ip_key = String::new(); // not persisted (see plan's accepted divergence) — quota re-accrues naturally
            let state = RoomState::restore(
                r.session_id.clone(),
                r.host_peer_id,
                r.host_verifier,
                owner_ip_key.clone(),
                r.reservations,
                r.created_at,
                r.last_activity_at,
            );
            rooms.insert(r.session_id, Arc::new(Room::new(state)));
            *quota.entry(owner_ip_key).or_insert(0) += 1;
        }
    }

    pub async fn get(&self, session_id: &str) -> Option<Arc<Room>> {
        self.rooms.read().await.get(session_id).cloned()
    }

    pub async fn room_count(&self) -> usize {
        self.rooms.read().await.len()
    }

    /// Snapshot of every room's `Arc` handle, for the cleanup sweep to walk
    /// without holding the registry lock while it locks individual rooms.
    pub async fn all_rooms(&self) -> Vec<Arc<Room>> {
        self.rooms.read().await.values().cloned().collect()
    }

    /// Removes `session_id` from the map iff it still points at `room`
    /// (guards against a racing replacement), and releases its quota slot.
    pub async fn remove_if_current(&self, session_id: &str, room: &Arc<Room>) -> bool {
        let removed = {
            let mut rooms = self.rooms.write().await;
            match rooms.get(session_id) {
                Some(current) if Arc::ptr_eq(current, room) => {
                    rooms.remove(session_id);
                    true
                }
                _ => false,
            }
        };
        if removed {
            let owner_ip_key = room.state.lock().await.owner_ip_key.clone();
            self.release_room_quota(&owner_ip_key).await;
        }
        removed
    }

    async fn release_room_quota(&self, owner_ip_key: &str) {
        let mut quota = self.rooms_per_ip.lock().await;
        if let Some(count) = quota.get_mut(owner_ip_key) {
            if *count > 0 {
                *count -= 1;
            }
            if *count == 0 {
                quota.remove(owner_ip_key);
            }
        }
    }

    async fn try_claim_room_quota(&self, owner_ip_key: &str) -> bool {
        let mut quota = self.rooms_per_ip.lock().await;
        let count = quota.entry(owner_ip_key.to_string()).or_insert(0);
        if *count >= MAX_ROOMS_PER_IP {
            return false;
        }
        *count += 1;
        true
    }

    pub async fn create(
        &self,
        session_id: &str,
        host_peer_id: &str,
        presented_token: &str,
        owner_ip_key: &str,
        admission: Admission,
    ) -> (Option<Arc<Room>>, Effects) {
        let Some(host_verifier) = Verifier::from_token(presented_token) else {
            return (None, Effects::error(ClientError::InvalidMessage, "Invalid reconnect token"));
        };

        let mut rooms = self.rooms.write().await;

        if let Some(existing) = rooms.get(session_id).cloned() {
            let idempotent = {
                let state = existing.state.lock().await;
                !state.closing && state.host_peer_id == host_peer_id && state.host_verifier.matches(&host_verifier)
            };
            if idempotent {
                let mut state = existing.state.lock().await;
                let effects = state.reconnect_host(admission, presented_token.to_string(), SystemTime::now());
                drop(state);
                drop(rooms);
                self.snapshot.record_mutation();
                return (Some(existing), effects);
            }

            let reclaimable = existing.state.lock().await.is_reclaimable();
            if !reclaimable {
                return (None, Effects::error(ClientError::RoomExists, "Room already exists"));
            }
            // Reclaim: this happens entirely within the write-lock section
            // already held above, so release-then-claim below is atomic
            // against any concurrent create — no separate "replacing" quota
            // transaction is needed.
            let old_owner = existing.state.lock().await.owner_ip_key.clone();
            existing.state.lock().await.closing = true;
            rooms.remove(session_id);
            self.release_room_quota(&old_owner).await;
        }

        if rooms.len() >= MAX_RETAINED_ROOMS {
            return (None, Effects::error(ClientError::RateLimited, "Too many retained rooms"));
        }
        if !self.try_claim_room_quota(owner_ip_key).await {
            return (None, Effects::error(ClientError::RateLimited, "Too many rooms created"));
        }

        let (state, effects) = RoomState::new_room(
            session_id.to_string(),
            host_peer_id.to_string(),
            host_verifier,
            presented_token.to_string(),
            owner_ip_key.to_string(),
            admission,
            SystemTime::now(),
        );
        let room = Arc::new(Room::new(state));
        rooms.insert(session_id.to_string(), room.clone());
        drop(rooms);
        self.snapshot.record_mutation();
        (Some(room), effects)
    }

    /// Full `endSession` orchestration: marks the room closing, evicts it
    /// from the registry (releasing quota), waits for that removal to be
    /// durable, then commits (or rolls back) the room's own teardown.
    pub async fn end_session(&self, room: &Arc<Room>, peer_id: &str, conn_id: u64, token: &str, protocol_version: u32) -> Effects {
        let (owner_ip_key, guests) = {
            let mut state = room.state.lock().await;
            match state.begin_end_session(peer_id, conn_id, token, protocol_version) {
                Err(effects) => return effects,
                Ok(ok) => ok,
            }
        };
        {
            let mut rooms = self.rooms.write().await;
            if rooms.get(&room.session_id).map(|r| Arc::ptr_eq(r, room)).unwrap_or(false) {
                rooms.remove(&room.session_id);
            }
        }
        self.release_room_quota(&owner_ip_key).await;
        let persisted = self.snapshot.record_terminal_mutation().await.is_ok();
        let mut state = room.state.lock().await;
        state.finish_end_session(guests, persisted)
    }

    pub(crate) async fn build_snapshot(&self) -> Vec<RoomDto> {
        let rooms: Vec<Arc<Room>> = self.rooms.read().await.values().cloned().collect();
        let mut dtos = Vec::with_capacity(rooms.len());
        for room in rooms {
            let state = room.state.lock().await;
            if state.closing {
                continue;
            }
            dtos.push(snapshot::build_room_dto(
                &state.session_id,
                &state.host_peer_id,
                &state.host_verifier,
                &state.reservations,
                state.created_at,
                state.last_activity_at,
            ));
        }
        dtos
    }
}
