//! Session/peer ID validation and reconnect-token verifier handling.
//!
//! Protocol v2 always has the *client* mint its own 32-byte reconnect
//! token and hand it to the server on `create`/`join`; the server never
//! generates one itself (that's a v0-legacy behavior, out of scope here).
//! The server only ever stores `SHA-256(token bytes)` — the "verifier" —
//! and compares presented tokens against it in constant time.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::protocol::RECONNECT_TOKEN_BYTES;

/// Base64 URL-safe, no-padding character length for a `RECONNECT_TOKEN_BYTES`
/// raw byte string: `ceil(n * 4 / 3)`. Checked up front so an
/// implausibly long "token" string is rejected without decoding it.
const RECONNECT_TOKEN_ENCODED_LEN: usize = (RECONNECT_TOKEN_BYTES * 4 + 2) / 3;

/// SHA-256 of a reconnect token's raw bytes. Never compared with `==`
/// directly — use [`Verifier::matches`], which is constant-time.
#[derive(Clone, Copy, Eq)]
pub struct Verifier([u8; 32]);

impl std::fmt::Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Verifier(..)")
    }
}

// Equality must stay constant-time even for incidental uses (e.g. in a
// `HashMap` context or test assertions), so `PartialEq` itself is wired to
// the same constant-time comparison rather than a derived byte-for-byte one.
impl PartialEq for Verifier {
    fn eq(&self, other: &Self) -> bool {
        self.matches(other)
    }
}

impl Verifier {
    pub fn from_token(token: &str) -> Option<Self> {
        if token.len() != RECONNECT_TOKEN_ENCODED_LEN {
            return None;
        }
        let raw = URL_SAFE_NO_PAD.decode(token).ok()?;
        if raw.len() != RECONNECT_TOKEN_BYTES {
            return None;
        }
        Some(Self::from_raw_bytes(&raw))
    }

    fn from_raw_bytes(raw: &[u8]) -> Self {
        let digest = Sha256::digest(raw);
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&digest);
        Self(bytes)
    }

    pub fn matches(&self, other: &Self) -> bool {
        self.0.ct_eq(&other.0).into()
    }

    pub fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0)
    }

    /// Restores a verifier from its persisted (base64) form — used only by
    /// the snapshot loader, never from a client-presented token (that path
    /// always goes through [`Verifier::from_token`], which re-derives the
    /// hash from raw token bytes rather than trusting an encoded digest).
    pub fn from_encoded(encoded: &str) -> Option<Self> {
        let raw = URL_SAFE_NO_PAD.decode(encoded).ok()?;
        if raw.len() != 32 {
            return None;
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&raw);
        Some(Self(bytes))
    }
}

/// `^[A-Za-z0-9_-]+$`, 1..=max_length chars — matches `relay_protocol.json`'s
/// `idPattern`. Implemented as a hand-rolled predicate rather than pulling in
/// the `regex` crate for one fixed, trivial pattern.
pub fn valid_id(value: &str, max_length: usize) -> bool {
    if value.is_empty() || value.len() > max_length {
        return false;
    }
    value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_id_accepts_expected_shapes() {
        assert!(valid_id("abc123_-XYZ", 64));
        assert!(!valid_id("", 64));
        assert!(!valid_id("has space", 64));
        assert!(!valid_id("has/slash", 64));
        assert!(!valid_id(&"a".repeat(65), 64));
        assert!(valid_id(&"a".repeat(64), 64));
    }

    #[test]
    fn verifier_round_trips_through_encoding() {
        let token = URL_SAFE_NO_PAD.encode([7u8; RECONNECT_TOKEN_BYTES]);
        let v1 = Verifier::from_token(&token).expect("structurally valid token");
        let encoded = v1.encode();
        let v2 = Verifier::from_encoded(&encoded).expect("round trip");
        assert!(v1.matches(&v2));
    }

    #[test]
    fn verifier_rejects_malformed_tokens() {
        assert!(Verifier::from_token("").is_none());
        assert!(Verifier::from_token("not-base64!!!").is_none());
        assert!(Verifier::from_token(&URL_SAFE_NO_PAD.encode([1u8; 16])).is_none());
    }

    #[test]
    fn different_tokens_yield_non_matching_verifiers() {
        let a = Verifier::from_token(&URL_SAFE_NO_PAD.encode([1u8; RECONNECT_TOKEN_BYTES])).unwrap();
        let b = Verifier::from_token(&URL_SAFE_NO_PAD.encode([2u8; RECONNECT_TOKEN_BYTES])).unwrap();
        assert!(!a.matches(&b));
    }
}
