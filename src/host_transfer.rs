//! Host-transfer eligibility algorithm, isolated from `room.rs` so it's
//! directly unit-testable with plain data, no room/lock/networking types.
//!
//! Ported from Go's `hostTransferHostLocked` / `hostTransferTargetLocked` /
//! `publishHostTransferEligibilityLocked`. The gating is deliberately
//! all-or-nothing: a room only offers host transfer at all once every
//! connected peer at the host's own sync-protocol version has also declared
//! the `hostTransfer` capability, and every connected peer (any version) has
//! a positive sync-protocol version.

/// A connected peer's transfer-relevant admission metadata (set from the
/// message that admitted its connection — see `room.rs::ConnectedPeer`).
#[derive(Debug, Clone, Copy)]
pub struct PeerInfo<'a> {
    pub peer_id: &'a str,
    pub sync_protocol_version: i32,
    pub host_transfer_capable: bool,
}

/// True iff the room currently has an "eligible transferring host": the
/// connected host declared the capability and has a positive sync version,
/// and no other connected peer at the same sync version lacks the
/// capability (one straggler at the host's version blocks transfer
/// entirely; peers at other versions never block it, they're just never
/// valid targets).
pub fn eligible_host(host_peer_id: &str, peers: &[PeerInfo]) -> bool {
    let Some(host) = peers.iter().find(|p| p.peer_id == host_peer_id) else {
        return false;
    };
    if host.sync_protocol_version <= 0 || !host.host_transfer_capable {
        return false;
    }
    peers.iter().all(|p| {
        p.sync_protocol_version > 0
            && !(p.sync_protocol_version == host.sync_protocol_version && !p.host_transfer_capable)
    })
}

/// True iff `target_peer_id` is a legal transfer target given an eligible
/// host: connected, not the host itself, reserved (via `is_reserved`, which
/// callers wire to their reservation map and must additionally exclude a
/// release-pending reservation), capability-declaring, and at the host's
/// exact sync-protocol version.
pub fn is_valid_target(
    host_peer_id: &str,
    peers: &[PeerInfo],
    is_reserved: impl Fn(&str) -> bool,
    target_peer_id: &str,
) -> bool {
    if target_peer_id == host_peer_id {
        return false;
    }
    if !eligible_host(host_peer_id, peers) {
        return false;
    }
    let Some(host) = peers.iter().find(|p| p.peer_id == host_peer_id) else {
        return false;
    };
    let Some(target) = peers.iter().find(|p| p.peer_id == target_peer_id) else {
        return false;
    };
    is_reserved(target_peer_id)
        && target.host_transfer_capable
        && target.sync_protocol_version == host.sync_protocol_version
}

/// Computes the `hostTransferEligibility` broadcast payload for the current
/// roster, or `None` if the message should not be sent at all this round
/// (no connected peer has ever declared the `hostTransfer` capability, so
/// there's no one to notify regardless of eligibility).
///
/// Returns `Some(recipients, targets)`: `recipients` is every
/// capability-declaring connected peer (who always gets notified, even if
/// `targets` ends up empty for them); `targets` is the (possibly empty) list
/// of peer ids that are currently valid transfer destinations.
pub fn compute_eligibility(
    host_peer_id: &str,
    peers: &[PeerInfo],
    is_reserved: impl Fn(&str) -> bool,
) -> Option<(Vec<String>, Vec<String>)> {
    let recipients: Vec<String> = peers
        .iter()
        .filter(|p| p.host_transfer_capable)
        .map(|p| p.peer_id.to_string())
        .collect();
    if recipients.is_empty() {
        return None;
    }
    let targets: Vec<String> = peers
        .iter()
        .filter(|p| is_valid_target(host_peer_id, peers, &is_reserved, p.peer_id))
        .map(|p| p.peer_id.to_string())
        .collect();
    Some((recipients, targets))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer<'a>(id: &'a str, sync_version: i32, capable: bool) -> PeerInfo<'a> {
        PeerInfo { peer_id: id, sync_protocol_version: sync_version, host_transfer_capable: capable }
    }

    #[test]
    fn host_not_capable_is_never_eligible() {
        let peers = vec![peer("host", 5, false), peer("guest", 5, true)];
        assert!(!eligible_host("host", &peers));
    }

    #[test]
    fn host_with_zero_sync_version_is_never_eligible() {
        let peers = vec![peer("host", 0, true)];
        assert!(!eligible_host("host", &peers));
    }

    #[test]
    fn straggler_at_hosts_version_blocks_eligibility() {
        // guest2 shares the host's sync version but lacks the capability.
        let peers = vec![peer("host", 5, true), peer("guest1", 5, true), peer("guest2", 5, false)];
        assert!(!eligible_host("host", &peers));
    }

    #[test]
    fn peer_at_different_version_without_capability_does_not_block() {
        let peers = vec![peer("host", 5, true), peer("guest", 3, false)];
        assert!(eligible_host("host", &peers));
    }

    #[test]
    fn any_zero_sync_version_peer_blocks_eligibility_regardless_of_version() {
        let peers = vec![peer("host", 5, true), peer("guest", 0, true)];
        assert!(!eligible_host("host", &peers));
    }

    #[test]
    fn valid_target_requires_reservation_and_matching_version() {
        let peers = vec![peer("host", 5, true), peer("guest", 5, true)];
        assert!(is_valid_target("host", &peers, |_| true, "guest"));
        assert!(!is_valid_target("host", &peers, |_| false, "guest"));
    }

    #[test]
    fn valid_target_rejects_mismatched_sync_version() {
        let peers = vec![peer("host", 5, true), peer("guest", 3, true)];
        assert!(!is_valid_target("host", &peers, |_| true, "guest"));
    }

    #[test]
    fn compute_eligibility_none_when_nobody_capable() {
        let peers = vec![peer("host", 5, false), peer("guest", 5, false)];
        assert!(compute_eligibility("host", &peers, |_| true).is_none());
    }

    #[test]
    fn compute_eligibility_sends_empty_targets_to_lone_capable_recipient() {
        // Host itself is the only capability-declaring peer; it can't be a
        // target of its own transfer, so targets is empty but the message
        // still goes out (to the host).
        let peers = vec![peer("host", 5, true)];
        let (recipients, targets) = compute_eligibility("host", &peers, |_| true).unwrap();
        assert_eq!(recipients, vec!["host".to_string()]);
        assert!(targets.is_empty());
    }

    #[test]
    fn compute_eligibility_full_room() {
        let peers = vec![peer("host", 5, true), peer("g1", 5, true), peer("g2", 5, true)];
        let (recipients, targets) = compute_eligibility("host", &peers, |_| true).unwrap();
        assert_eq!(recipients.len(), 3);
        assert_eq!(targets, vec!["g1".to_string(), "g2".to_string()]);
    }
}
