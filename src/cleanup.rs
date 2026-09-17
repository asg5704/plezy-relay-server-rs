//! 5-minute sweep: prunes expired guest reservations, evicts rooms that are
//! empty+idle past `EMPTY_ROOM_MAX_AGE` or simply older than
//! `ROOM_MAX_AGE` regardless of occupancy, and expires log-store entries.
//! Mirrors Go's `runCleanupStep`.

use std::time::{Duration, SystemTime};

use crate::room::{EMPTY_ROOM_MAX_AGE, ROOM_MAX_AGE};
use crate::AppState;

const CLEANUP_INTERVAL: Duration = Duration::from_secs(5 * 60);

pub async fn run(app: AppState) {
    let mut interval = tokio::time::interval(CLEANUP_INTERVAL);
    interval.tick().await; // skip the immediate first tick
    loop {
        interval.tick().await;
        sweep(&app).await;
    }
}

pub async fn sweep(app: &AppState) {
    let now = SystemTime::now();
    let rooms = app.registry.all_rooms().await;
    for room in rooms {
        let (remove, changed, evict) = {
            let mut state = room.state.lock().await;
            let pruned = state.prune_expired_reservations();
            let empty = state.connected_peer_count() == 0;
            let idle = state.idle_for(now);
            let age = state.age(now);
            let expired = age > ROOM_MAX_AGE;
            let remove = (empty && idle > EMPTY_ROOM_MAX_AGE) || expired;
            let mut evict = Vec::new();
            if remove {
                state.closing = true;
                if expired && !empty {
                    evict = state.peers.values().map(|p| p.cancel.clone()).collect();
                    state.peers.clear();
                }
            }
            (remove, pruned || remove, evict)
        };
        if remove {
            app.registry.remove_if_current(&room.session_id, &room).await;
        }
        if changed {
            app.snapshot.record_mutation();
        }
        for cancel in evict {
            cancel.cancel();
        }
    }
    app.logs.cleanup(now).await;
}
