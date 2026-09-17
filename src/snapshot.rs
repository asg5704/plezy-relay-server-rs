//! Room-state persistence: synchronous durability for terminal mutations
//! (`leave`/`endSession`), debounced best-effort persistence for everything
//! else. See the plan's "Persistence (synchronous durability)" section.
//!
//! Simplified relative to Go's `recordTerminalMutation`/ticket model: rather
//! than the writer invoking a caller-supplied rollback closure from inside
//! its own task, [`SnapshotHandle::record_terminal_mutation`] just resolves
//! once the mutation's generation is durably persisted (or the write
//! failed) — callers (`room.rs`'s `Room::leave`/`Room::end_session`)
//! re-acquire the room lock themselves afterward to commit or roll back.
//! Same guarantee (a client is never told `left`/`ended` succeeded before
//! it's durable), simpler wiring.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::error::SnapshotError;
use crate::ids::Verifier;
use crate::protocol::{supported_protocol_version, MAX_PEER_ID_LENGTH, MAX_SESSION_ID_LENGTH};
use crate::registry::Registry;
use crate::room::{valid_session_and_peer_id, EMPTY_ROOM_MAX_AGE, ROOM_MAX_AGE};

const SNAPSHOT_FORMAT_VERSION: u32 = 1;
const DEBOUNCE: Duration = Duration::from_millis(100);
pub const SNAPSHOT_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Serialize, Deserialize)]
struct ReservationDto {
    verifier: String,
    /// Unix-epoch milliseconds the identity became absent; `None` means
    /// "was connected at save time" (never persisted as currently-absent
    /// for a peer that's still occupying a live connection).
    absent_since_millis: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RoomDto {
    session_id: String,
    host_peer_id: String,
    host_verifier: String,
    protocol_version: u32,
    reservations: HashMap<String, ReservationDto>,
    created_at_millis: u64,
    last_activity_at_millis: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StateSnapshotDto {
    version: u32,
    rooms: Vec<RoomDto>,
}

impl StateSnapshotDto {
    pub(crate) fn new(rooms: Vec<RoomDto>) -> Self {
        Self { version: SNAPSHOT_FORMAT_VERSION, rooms }
    }
}

/// A room restored from a snapshot file at startup, defensively validated —
/// never trust the file blindly.
pub struct RestoredRoom {
    pub session_id: String,
    pub host_peer_id: String,
    pub host_verifier: Verifier,
    pub reservations: HashMap<String, RestoredReservation>,
    pub created_at: SystemTime,
    pub last_activity_at: SystemTime,
}

pub struct RestoredReservation {
    pub verifier: Verifier,
    pub absent_since: Option<Instant>,
}

fn to_millis(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn from_millis(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

/// Loads and defensively validates `path`, dropping (not erroring on) any
/// room whose id/verifier shape is invalid or that's aged/idled past the
/// same limits the cleanup sweep enforces. A missing or corrupt file yields
/// an empty result — startup always proceeds with a fresh, empty room set
/// rather than failing.
pub fn load_snapshot(path: &Path, now: SystemTime) -> Vec<RestoredRoom> {
    let Ok(bytes) = std::fs::read(path) else {
        return Vec::new();
    };
    let Ok(dto) = serde_json::from_slice::<StateSnapshotDto>(&bytes) else {
        tracing::warn!("snapshot: corrupt file at {}, starting fresh", path.display());
        return Vec::new();
    };
    if dto.version != SNAPSHOT_FORMAT_VERSION {
        tracing::warn!("snapshot: unknown version {}, starting fresh", dto.version);
        return Vec::new();
    }

    let mut restored = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for room in dto.rooms {
        if !valid_session_and_peer_id(&room.session_id, &room.host_peer_id) {
            continue;
        }
        if room.session_id.len() > MAX_SESSION_ID_LENGTH || room.host_peer_id.len() > MAX_PEER_ID_LENGTH {
            continue;
        }
        if !supported_protocol_version(room.protocol_version) {
            continue;
        }
        let Some(host_verifier) = Verifier::from_encoded(&room.host_verifier) else { continue };
        let created_at = from_millis(room.created_at_millis);
        let last_activity_at = from_millis(room.last_activity_at_millis);
        let age = now.duration_since(created_at).unwrap_or_default();
        let idle = now.duration_since(last_activity_at).unwrap_or_default();
        if age > ROOM_MAX_AGE || idle > EMPTY_ROOM_MAX_AGE {
            continue;
        }
        if !seen.insert(room.session_id.clone()) {
            continue;
        }

        let mut reservations = HashMap::new();
        let mut valid = true;
        for (peer_id, r) in room.reservations {
            if !valid_session_and_peer_id(&room.session_id, &peer_id) || peer_id == room.host_peer_id {
                valid = false;
                break;
            }
            let Some(verifier) = Verifier::from_encoded(&r.verifier) else {
                valid = false;
                break;
            };
            let absent_since = r.absent_since_millis.map(|ms| {
                let absent_at = from_millis(ms);
                let elapsed = now.duration_since(absent_at).unwrap_or_default();
                Instant::now().checked_sub(elapsed).unwrap_or_else(Instant::now)
            });
            reservations.insert(peer_id, RestoredReservation { verifier, absent_since });
        }
        if !valid {
            continue;
        }

        restored.push(RestoredRoom {
            session_id: room.session_id,
            host_peer_id: room.host_peer_id,
            host_verifier,
            reservations,
            created_at,
            last_activity_at,
        });
    }
    tracing::info!("snapshot: loaded {} room(s) from {}", restored.len(), path.display());
    restored
}

pub enum Command {
    Mutate,
    Terminal(oneshot::Sender<Result<(), SnapshotError>>),
    FlushAndStop(oneshot::Sender<Result<(), SnapshotError>>),
}

/// Cheap, cloneable handle `room.rs`/`connection.rs`/`cleanup.rs` hold to
/// notify the snapshotter of mutations.
#[derive(Clone)]
pub struct SnapshotHandle {
    tx: mpsc::UnboundedSender<Command>,
}

impl SnapshotHandle {
    /// Fire-and-forget: the state changed, but nothing is waiting on it
    /// being durable — it'll be picked up by the next debounced write.
    pub fn record_mutation(&self) {
        let _ = self.tx.send(Command::Mutate);
    }

    /// Forces an immediate (debounce-skipping) capture+persist and resolves
    /// once that generation is durable (or the write failed). Used by
    /// `leave`/`endSession` — the caller replies to the client only after
    /// this resolves.
    pub async fn record_terminal_mutation(&self) -> Result<(), SnapshotError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.tx.send(Command::Terminal(reply_tx)).is_err() {
            return Err(SnapshotError("snapshot writer is stopped".to_string()));
        }
        reply_rx.await.unwrap_or_else(|_| Err(SnapshotError("snapshot writer dropped the request".to_string())))
    }

    pub async fn flush_and_stop(&self, timeout: Duration) -> Result<(), SnapshotError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.tx.send(Command::FlushAndStop(reply_tx)).is_err() {
            return Ok(());
        }
        match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Ok(()),
            Err(_) => Err(SnapshotError("snapshot flush timed out".to_string())),
        }
    }
}

/// Constructs the (sender, receiver) pair. The sender half becomes every
/// `SnapshotHandle`; the receiver is handed to [`run`], spawned separately
/// once the `Registry` it needs to read from exists.
pub fn channel() -> (SnapshotHandle, mpsc::UnboundedReceiver<Command>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (SnapshotHandle { tx }, rx)
}

/// The snapshotter's background task. Owns `dirty_seq`/`durable_seq`
/// generation counters and a debounce timer; persists via
/// temp-file-write + fsync + atomic rename + best-effort parent-dir sync.
pub async fn run(mut rx: mpsc::UnboundedReceiver<Command>, registry: Arc<Registry>, state_file: PathBuf) {
    let mut dirty_seq: u64 = 0;
    let mut durable_seq: u64 = 0;
    // Each pending waiter is resolved once `durable_seq` reaches its seq.
    let mut waiters: Vec<(u64, oneshot::Sender<Result<(), SnapshotError>>)> = Vec::new();
    let write_lock = Mutex::new(());

    loop {
        let debounce_pending = dirty_seq > durable_seq;
        tokio::select! {
            cmd = rx.recv() => {
                match cmd {
                    None => return,
                    Some(Command::Mutate) => {
                        dirty_seq += 1;
                    }
                    Some(Command::Terminal(reply)) => {
                        dirty_seq += 1;
                        waiters.push((dirty_seq, reply));
                        let result = persist(&registry, &state_file, &write_lock).await;
                        if result.is_ok() {
                            durable_seq = dirty_seq;
                        }
                        resolve_waiters(&mut waiters, durable_seq, &result);
                    }
                    Some(Command::FlushAndStop(reply)) => {
                        let result = if dirty_seq > durable_seq {
                            let r = persist(&registry, &state_file, &write_lock).await;
                            if r.is_ok() { durable_seq = dirty_seq; }
                            r
                        } else {
                            Ok(())
                        };
                        resolve_waiters(&mut waiters, durable_seq, &result);
                        let _ = reply.send(result);
                        return;
                    }
                }
            }
            _ = tokio::time::sleep(DEBOUNCE), if debounce_pending => {
                let result = persist(&registry, &state_file, &write_lock).await;
                if result.is_ok() {
                    durable_seq = dirty_seq;
                }
                resolve_waiters(&mut waiters, durable_seq, &result);
            }
        }
    }
}

fn resolve_waiters(waiters: &mut Vec<(u64, oneshot::Sender<Result<(), SnapshotError>>)>, durable_seq: u64, result: &Result<(), SnapshotError>) {
    let mut remaining = Vec::with_capacity(waiters.len());
    for (seq, reply) in waiters.drain(..) {
        if seq <= durable_seq || result.is_err() {
            let _ = reply.send(result.clone());
        } else {
            remaining.push((seq, reply));
        }
    }
    *waiters = remaining;
}

async fn persist(registry: &Arc<Registry>, path: &Path, write_lock: &Mutex<()>) -> Result<(), SnapshotError> {
    let rooms = registry.build_snapshot().await;
    let dto = StateSnapshotDto::new(rooms);
    let data = serde_json::to_vec(&dto).map_err(|e| SnapshotError(e.to_string()))?;
    let _guard = write_lock.lock().await;
    persist_atomic(path, &data).map_err(|e| SnapshotError(e.to_string()))
}

fn persist_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp_path = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp_path)?;
        use std::io::Write;
        f.write_all(data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp_path, path)?;
    // Best-effort parent-directory sync; failure here is warning-only since
    // the atomic rename already committed.
    if let Some(dir) = path.parent() {
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

pub(crate) fn build_room_dto(
    session_id: &str,
    host_peer_id: &str,
    host_verifier: &Verifier,
    reservations: &HashMap<String, crate::room::PeerReservation>,
    created_at: SystemTime,
    last_activity_at: SystemTime,
) -> RoomDto {
    let reservations_dto: HashMap<String, ReservationDto> = reservations
        .iter()
        .filter(|(_, r)| !r.release_pending)
        .map(|(peer_id, r)| {
            let absent_since_millis = r.absent_since.map(|instant| {
                let elapsed = Instant::now().saturating_duration_since(instant);
                to_millis(SystemTime::now() - elapsed)
            });
            (peer_id.clone(), ReservationDto { verifier: r.verifier.encode(), absent_since_millis })
        })
        .collect();
    RoomDto {
        session_id: session_id.to_string(),
        host_peer_id: host_peer_id.to_string(),
        host_verifier: host_verifier.encode(),
        protocol_version: crate::protocol::RELAY_PROTOCOL_VERSION,
        reservations: reservations_dto,
        created_at_millis: to_millis(created_at),
        last_activity_at_millis: to_millis(last_activity_at),
    }
}
