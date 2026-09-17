//! Coverage for `room.rs` (the state machine) and the parts of
//! `registry.rs` that decide idempotent-reconnect vs. reclaim vs.
//! `room_exists`. Scenarios ported from
//! `~/oss/plezy/server/main_test.go`'s Go coverage, adapted to this
//! crate's synchronous `RoomState` + two-phase `begin_*`/`finish_*` design.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use tokio::sync::mpsc;

use relay_rs::ids::Verifier;
use relay_rs::protocol::{error_code, server_type, ClientMessage, ServerMessage, RECONNECT_TOKEN_BYTES};
use relay_rs::registry::Registry;
use relay_rs::room::{Admission, RoomState};
use relay_rs::snapshot;

fn token(byte: u8) -> String {
    URL_SAFE_NO_PAD.encode([byte; RECONNECT_TOKEN_BYTES])
}

fn admission(peer_id: &str) -> (Admission, mpsc::Receiver<ServerMessage>) {
    let (tx, rx) = mpsc::channel(8);
    (Admission { peer_id: peer_id.to_string(), tx, sync_protocol_version: 0, host_transfer_capable: false }, rx)
}

fn capable_admission(peer_id: &str, sync_version: i32) -> (Admission, mpsc::Receiver<ServerMessage>) {
    let (tx, rx) = mpsc::channel(8);
    (Admission { peer_id: peer_id.to_string(), tx, sync_protocol_version: sync_version, host_transfer_capable: true }, rx)
}

fn client_msg(msg_type: &str, session_id: &str, peer_id: &str, reconnect_token: &str, protocol_version: u32) -> ClientMessage {
    ClientMessage {
        msg_type: msg_type.to_string(),
        session_id: session_id.to_string(),
        peer_id: peer_id.to_string(),
        reconnect_token: reconnect_token.to_string(),
        protocol_version,
        ..Default::default()
    }
}

/// Builds a fresh room with a connected host, returning the state and the
/// host's assigned connection id.
fn new_room(session_id: &str, host_peer_id: &str, host_token: &str) -> (RoomState, u64) {
    let (host, _rx) = admission(host_peer_id);
    let verifier = Verifier::from_token(host_token).expect("valid token");
    let (state, effects) = RoomState::new_room(
        session_id.to_string(),
        host_peer_id.to_string(),
        verifier,
        host_token.to_string(),
        "owner-ip".to_string(),
        host,
        SystemTime::now(),
    );
    (state, effects.assigned_conn_id.expect("host gets a conn id"))
}

fn error_code_of(msg: &Option<ServerMessage>) -> Option<&str> {
    msg.as_ref().and_then(|m| m.code.as_deref())
}

// ---------------------------------------------------------------------
// Idempotent host re-create (RoomState::reconnect_host)
// ---------------------------------------------------------------------

#[test]
fn reconnect_host_reannounces_previously_absent_host() {
    let (mut state, host_conn_id) = new_room("room1", "host1", &token(1));

    let (g1, mut g1_rx) = admission("g1");
    let join = client_msg("join", "room1", "g1", &token(2), 2);
    let effects = state.join_or_resume(&join, g1, false, SystemTime::now());
    assert_eq!(effects.reply.unwrap().msg_type, server_type::JOINED);
    drop(g1_rx.try_recv()); // nothing broadcast to g1 itself on its own join

    // Host goes away (ungraceful disconnect) — the room stays around.
    let disconnect_effects = state.handle_disconnect("host1", host_conn_id, SystemTime::now());
    assert!(!state.peers.contains_key("host1"));
    // g1 hears about the host leaving.
    assert_eq!(disconnect_effects.broadcast.len(), 1);
    assert_eq!(disconnect_effects.broadcast[0].msg.msg_type, server_type::PEER_LEFT);

    // Idempotent re-create: reconnect_host reannounces since the host was
    // absent, and evicts nothing (there was no live host connection to
    // replace).
    let (host2, _host2_rx) = admission("host1");
    let effects = state.reconnect_host(host2, token(1), SystemTime::now());
    let reply = effects.reply.expect("created reply");
    assert_eq!(reply.msg_type, server_type::CREATED);
    assert_eq!(reply.peers, Some(vec!["g1".to_string()]));
    assert!(effects.evict.is_empty(), "no prior live host connection to evict");
    assert_eq!(effects.broadcast.len(), 1, "g1 should hear the host rejoined");
    assert_eq!(effects.broadcast[0].msg.msg_type, server_type::PEER_JOINED);
    assert_eq!(effects.broadcast[0].msg.peer_id, Some("host1".to_string()));

    let new_conn_id = effects.assigned_conn_id.expect("fresh conn id");
    assert_ne!(new_conn_id, host_conn_id);

    // Try to drain g1's own receiver just to make sure the channel is intact.
    let _ = g1_rx.try_recv();
}

#[test]
fn reconnect_host_while_still_connected_evicts_the_old_connection() {
    let (mut state, host_conn_id) = new_room("room1", "host1", &token(1));

    let (host2, _host2_rx) = admission("host1");
    let effects = state.reconnect_host(host2, token(1), SystemTime::now());
    assert_eq!(effects.reply.unwrap().msg_type, server_type::CREATED);
    // The host never left, so no peerJoined re-announcement...
    assert!(effects.broadcast.is_empty());
    // ...but the stale connection is evicted.
    assert_eq!(effects.evict.len(), 1);
    let new_conn_id = effects.assigned_conn_id.unwrap();
    assert_ne!(new_conn_id, host_conn_id);
}

// ---------------------------------------------------------------------
// Registry::create — idempotent reuse vs. reclaim vs. room_exists
// ---------------------------------------------------------------------

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_state_file() -> PathBuf {
    let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("relay-rs-room-state-test-{}-{n}.json", std::process::id()))
}

/// Spins up a real `Registry` backed by a real (temp-file) snapshot
/// writer, so terminal mutations (`record_terminal_mutation`) actually
/// resolve instead of hanging forever waiting for a reply nobody sends.
async fn new_registry() -> (Arc<Registry>, PathBuf) {
    let (handle, rx) = snapshot::channel();
    let registry = Arc::new(Registry::new(handle));
    let state_file = temp_state_file();
    tokio::spawn(snapshot::run(rx, registry.clone(), state_file.clone()));
    (registry, state_file)
}

#[tokio::test]
async fn create_rejects_duplicate_session_as_room_exists() {
    let (registry, state_file) = new_registry().await;
    let (host1, _rx1) = admission("host1");
    let (room, _effects) = registry.create("room1", "host1", &token(1), "ip-a", host1).await;
    assert!(room.is_some());

    // Different host identity/token for the same still-occupied session.
    let (host2, _rx2) = admission("host2");
    let (room2, effects2) = registry.create("room1", "host2", &token(2), "ip-b", host2).await;
    assert!(room2.is_none());
    assert_eq!(error_code_of(&effects2.reply), Some(error_code::ROOM_EXISTS));

    let _ = std::fs::remove_file(state_file);
}

#[tokio::test]
async fn create_is_idempotent_for_matching_host_and_token() {
    let (registry, state_file) = new_registry().await;
    let (host1, _rx1) = admission("host1");
    let (room, _effects) = registry.create("room1", "host1", &token(1), "ip-a", host1).await;
    let room = room.expect("created");

    // Same session + host + token: reused, not reclaimed — same Arc, old
    // connection evicted.
    let (host1_again, _rx2) = admission("host1");
    let (room2, effects2) = registry.create("room1", "host1", &token(1), "ip-a", host1_again).await;
    let room2 = room2.expect("idempotent recreate succeeds");
    assert!(Arc::ptr_eq(&room, &room2), "idempotent create reuses the same room");
    assert_eq!(effects2.reply.unwrap().msg_type, server_type::CREATED);
    assert_eq!(effects2.evict.len(), 1, "old connection under host1 is evicted");
    assert_eq!(registry.room_count().await, 1);

    let _ = std::fs::remove_file(state_file);
}

#[tokio::test]
async fn create_reclaims_abandoned_empty_room_with_a_fresh_room() {
    let (registry, state_file) = new_registry().await;
    let (host1, _rx1) = admission("host1");
    let (room, effects) = registry.create("room1", "host1", &token(1), "ip-a", host1).await;
    let room = room.expect("created");
    let host_conn_id = effects.assigned_conn_id.expect("conn id");

    // Host disconnects ungracefully — the room is now empty but still
    // registered (nothing proactively evicts it outside the cleanup sweep
    // or a reclaiming create).
    room.disconnect("host1", host_conn_id).await;
    assert_eq!(registry.room_count().await, 1);

    // A brand-new create for the same session id, different host/token,
    // reclaims it instead of erroring room_exists.
    let (host2, _rx2) = admission("host2");
    let (room2, effects2) = registry.create("room1", "host2", &token(2), "ip-b", host2).await;
    let room2 = room2.expect("reclaim succeeds");
    assert!(!Arc::ptr_eq(&room, &room2), "reclaim allocates a fresh room, not the old Arc");
    assert_eq!(effects2.reply.unwrap().msg_type, server_type::CREATED);
    assert_eq!(registry.room_count().await, 1, "old room was evicted, new one took its slot");

    let _ = std::fs::remove_file(state_file);
}

// ---------------------------------------------------------------------
// Guest reservation resume: within vs. past the 5-minute grace window
// ---------------------------------------------------------------------

fn backdated_instant(ago: Duration) -> Instant {
    Instant::now().checked_sub(ago).unwrap_or_else(Instant::now)
}

#[test]
fn resume_within_grace_window_succeeds() {
    let (mut state, _host_conn_id) = new_room("room1", "host1", &token(1));

    let (g1, _g1_rx) = admission("g1");
    let join = client_msg("join", "room1", "g1", &token(2), 2);
    let effects = state.join_or_resume(&join, g1, false, SystemTime::now());
    let g1_conn_id = effects.assigned_conn_id.expect("conn id");

    state.handle_disconnect("g1", g1_conn_id, SystemTime::now());
    // Backdate well within the 5-minute grace period.
    state.reservations.get_mut("g1").unwrap().absent_since = Some(backdated_instant(Duration::from_secs(60)));

    let (g1_again, _rx) = admission("g1");
    let resume = client_msg("resume", "room1", "g1", &token(2), 2);
    let effects = state.join_or_resume(&resume, g1_again, true, SystemTime::now());
    let reply = effects.reply.expect("resumed reply");
    assert_eq!(reply.msg_type, server_type::RESUMED);
    assert_eq!(reply.peers, Some(vec!["host1".to_string()]));
}

#[test]
fn resume_past_grace_window_is_rejected() {
    let (mut state, _host_conn_id) = new_room("room1", "host1", &token(1));

    let (g1, _g1_rx) = admission("g1");
    let join = client_msg("join", "room1", "g1", &token(2), 2);
    let effects = state.join_or_resume(&join, g1, false, SystemTime::now());
    let g1_conn_id = effects.assigned_conn_id.expect("conn id");

    state.handle_disconnect("g1", g1_conn_id, SystemTime::now());
    // Backdate past the 5-minute grace period (PEER_RESERVATION_GRACE).
    state.reservations.get_mut("g1").unwrap().absent_since = Some(backdated_instant(Duration::from_secs(6 * 60)));

    let (g1_again, _rx) = admission("g1");
    let resume = client_msg("resume", "room1", "g1", &token(2), 2);
    let effects = state.join_or_resume(&resume, g1_again, true, SystemTime::now());
    assert_eq!(error_code_of(&effects.reply), Some(error_code::PEER_ID_UNAVAILABLE));
    // The expired reservation should actually be gone, not just rejected.
    assert!(!state.reservations.contains_key("g1"));
}

// ---------------------------------------------------------------------
// Host transfer eligibility integration (room.rs + host_transfer.rs)
// ---------------------------------------------------------------------

#[test]
fn transfer_host_moves_authority_and_rebroadcasts_eligibility() {
    let (host, _host_rx) = capable_admission("host1", 5);
    let verifier = Verifier::from_token(&token(1)).unwrap();
    let (mut state, effects) =
        RoomState::new_room("room1".to_string(), "host1".to_string(), verifier, token(1), "owner-ip".to_string(), host, SystemTime::now());
    let host_conn_id = effects.assigned_conn_id.unwrap();

    let (g1, _g1_rx) = capable_admission("g1", 5);
    let join = client_msg("join", "room1", "g1", &token(2), 2);
    state.join_or_resume(&join, g1, false, SystemTime::now());

    let effects = state.transfer_host("host1", host_conn_id, 2, "g1");
    assert!(effects.dirty);

    let host_changed: Vec<_> = effects.broadcast.iter().filter(|o| o.msg.msg_type == server_type::HOST_CHANGED).collect();
    assert_eq!(host_changed.len(), 2, "both connected peers hear hostChanged");
    for outbound in &host_changed {
        assert_eq!(outbound.msg.host_peer_id, Some("g1".to_string()));
        assert_eq!(outbound.msg.from, Some("host1".to_string()));
    }

    let eligibility: Vec<_> =
        effects.broadcast.iter().filter(|o| o.msg.msg_type == server_type::HOST_TRANSFER_ELIGIBILITY).collect();
    assert_eq!(eligibility.len(), 2, "both capable peers get the recomputed eligibility");
    for outbound in &eligibility {
        assert_eq!(outbound.msg.host_peer_id, Some("g1".to_string()));
        assert_eq!(outbound.msg.host_transfer_targets, Some(vec!["host1".to_string()]));
    }

    assert_eq!(state.host_peer_id, "g1");
    assert!(state.host_verifier.matches(&Verifier::from_token(&token(2)).unwrap()));
    // The old host becomes a reserved (transferable-back) identity.
    let old_host_reservation = state.reservations.get("host1").expect("old host reservation retained");
    assert!(old_host_reservation.verifier.matches(&Verifier::from_token(&token(1)).unwrap()));
}

// ---------------------------------------------------------------------
// leave: two-phase commit + rollback-on-persist-failure
// ---------------------------------------------------------------------

#[test]
fn leave_commits_on_persist_success() {
    let (mut state, _host_conn_id) = new_room("room1", "host1", &token(1));
    let (g1, _g1_rx) = admission("g1");
    let join = client_msg("join", "room1", "g1", &token(2), 2);
    let effects = state.join_or_resume(&join, g1, false, SystemTime::now());
    let g1_conn_id = effects.assigned_conn_id.unwrap();

    state.begin_leave("g1", g1_conn_id, &token(2), 2).unwrap_or_else(|_| panic!("not authorized"));
    assert!(state.reservations["g1"].release_pending);

    let effects = state.finish_leave("g1", g1_conn_id, true, SystemTime::now());
    let reply = effects.reply.expect("left reply");
    assert_eq!(reply.msg_type, server_type::LEFT);
    assert_eq!(reply.peer_id, Some("g1".to_string()));
    assert!(!state.peers.contains_key("g1"));
    assert!(!state.reservations.contains_key("g1"), "identity fully freed");
    assert_eq!(effects.broadcast.len(), 1);
    assert_eq!(effects.broadcast[0].msg.msg_type, server_type::PEER_LEFT);
}

#[test]
fn leave_rolls_back_on_persist_failure() {
    let (mut state, _host_conn_id) = new_room("room1", "host1", &token(1));
    let (g1, _g1_rx) = admission("g1");
    let join = client_msg("join", "room1", "g1", &token(2), 2);
    let effects = state.join_or_resume(&join, g1, false, SystemTime::now());
    let g1_conn_id = effects.assigned_conn_id.unwrap();

    state.begin_leave("g1", g1_conn_id, &token(2), 2).unwrap_or_else(|_| panic!("not authorized"));

    let effects = state.finish_leave("g1", g1_conn_id, false, SystemTime::now());
    assert_eq!(error_code_of(&effects.reply), Some(error_code::INVALID_MESSAGE));
    // Rolled back: g1 is still a fully-fledged member, not half-freed.
    assert!(!state.reservations["g1"].release_pending);
    assert!(state.peers.contains_key("g1"));
    assert!(state.reservations.contains_key("g1"));
}

#[test]
fn begin_leave_rejects_host_and_stale_connections() {
    let (mut state, host_conn_id) = new_room("room1", "host1", &token(1));
    // The host itself may never "leave" this way (only endSession applies).
    assert!(state.begin_leave("host1", host_conn_id, &token(1), 2).is_err());

    let (g1, _g1_rx) = admission("g1");
    let join = client_msg("join", "room1", "g1", &token(2), 2);
    let effects = state.join_or_resume(&join, g1, false, SystemTime::now());
    let g1_conn_id = effects.assigned_conn_id.unwrap();

    // Stale conn id (as if a replaced/old connection tried to leave).
    assert!(state.begin_leave("g1", g1_conn_id + 1, &token(2), 2).is_err());
    // Wrong token.
    assert!(state.begin_leave("g1", g1_conn_id, &token(99), 2).is_err());
}

// ---------------------------------------------------------------------
// endSession: two-phase commit + rollback-on-persist-failure
// ---------------------------------------------------------------------

#[test]
fn end_session_commits_and_notifies_guests_on_persist_success() {
    let (mut state, host_conn_id) = new_room("room1", "host1", &token(1));
    let (g1, _g1_rx) = admission("g1");
    let join = client_msg("join", "room1", "g1", &token(2), 2);
    state.join_or_resume(&join, g1, false, SystemTime::now());

    let (owner_ip, guests) = state.begin_end_session("host1", host_conn_id, &token(1), 2).unwrap_or_else(|_| panic!("not authorized"));
    assert_eq!(owner_ip, "owner-ip");
    assert_eq!(guests.len(), 1, "only g1 is a guest to notify");
    assert!(state.closing);

    let effects = state.finish_end_session(guests, true);
    let reply = effects.reply.expect("ended reply");
    assert_eq!(reply.msg_type, server_type::ENDED);
    assert_eq!(effects.broadcast.len(), 1);
    assert_eq!(effects.broadcast[0].msg.msg_type, server_type::ENDED);
    assert_eq!(effects.evict.len(), 2, "both host and guest connections are evicted");
    assert!(state.peers.is_empty());
    assert!(state.reservations.is_empty());
}

#[test]
fn end_session_persist_failure_tears_down_without_ended_message() {
    let (mut state, host_conn_id) = new_room("room1", "host1", &token(1));
    let (g1, _g1_rx) = admission("g1");
    let join = client_msg("join", "room1", "g1", &token(2), 2);
    state.join_or_resume(&join, g1, false, SystemTime::now());

    let (_owner_ip, guests) = state.begin_end_session("host1", host_conn_id, &token(1), 2).unwrap_or_else(|_| panic!("not authorized"));
    let effects = state.finish_end_session(guests, false);

    assert_eq!(error_code_of(&effects.reply), Some(error_code::INVALID_MESSAGE));
    assert!(effects.broadcast.is_empty(), "no ended message goes out on persist failure");
    assert_eq!(effects.evict.len(), 2, "still torn down even though persistence failed");
    assert!(state.peers.is_empty());
    assert!(state.reservations.is_empty());
}

#[test]
fn begin_end_session_rejects_non_host() {
    let (mut state, _host_conn_id) = new_room("room1", "host1", &token(1));
    let (g1, _g1_rx) = admission("g1");
    let join = client_msg("join", "room1", "g1", &token(2), 2);
    let effects = state.join_or_resume(&join, g1, false, SystemTime::now());
    let g1_conn_id = effects.assigned_conn_id.unwrap();

    assert!(state.begin_end_session("g1", g1_conn_id, &token(2), 2).is_err());
}
