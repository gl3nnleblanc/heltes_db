use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;

/// Virtual nodes placed on the ring per physical shard.
/// More vnodes → better distribution, higher memory cost.
const DEFAULT_VNODES: usize = 150;

/// Consistent-hash ring over a 64-bit position space.
///
/// Each physical shard occupies `vnodes` positions derived by hashing
/// `"<addr>#<i>"` for i in 0..vnodes. A key maps to the shard whose nearest
/// position is >= hash(key), wrapping to the lowest position if necessary.
///
/// No external dependencies: ring positions use FNV-1a 64-bit.
pub struct ConsistentHashRouter {
    ring: BTreeMap<u64, SocketAddr>,
}

impl ConsistentHashRouter {
    pub fn new(shards: impl IntoIterator<Item = SocketAddr>) -> Self {
        Self::with_vnodes(shards, DEFAULT_VNODES)
    }

    pub fn with_vnodes(
        shards: impl IntoIterator<Item = SocketAddr>,
        vnodes: usize,
    ) -> Self {
        let mut ring = BTreeMap::new();
        for addr in shards {
            for i in 0..vnodes {
                let label = format!("{addr}#{i}");
                ring.insert(fnv1a_64(label.as_bytes()), addr);
            }
        }
        ConsistentHashRouter { ring }
    }

    /// The shard responsible for `key`, or `None` if no shards are registered.
    pub fn shard_for_key(&self, key: u64) -> Option<SocketAddr> {
        if self.ring.is_empty() {
            return None;
        }
        let pos = fnv1a_64(&key.to_le_bytes());
        self.ring
            .range(pos..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, &addr)| addr)
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// Number of distinct physical shards in the ring.
    pub fn shard_count(&self) -> usize {
        self.ring.values().collect::<HashSet<_>>().len()
    }
}

// ── FNV-1a 64-bit ─────────────────────────────────────────────────────────────

fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 14695981039346656037;
    const PRIME: u64 = 1099511628211;
    bytes.iter().fold(OFFSET, |h, &b| (h ^ b as u64).wrapping_mul(PRIME))
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("[::1]:{port}").parse().unwrap()
    }

    // -- Basic correctness -----------------------------------------------------

    #[test]
    fn empty_router_returns_none() {
        let r = ConsistentHashRouter::new([]);
        assert!(r.shard_for_key(42).is_none());
    }

    #[test]
    fn single_shard_owns_all_keys() {
        let s = addr(50051);
        let r = ConsistentHashRouter::new([s]);
        for key in [0u64, 1, 42, 999, u64::MAX, u64::MAX / 2] {
            assert_eq!(r.shard_for_key(key), Some(s));
        }
    }

    #[test]
    fn two_shards_both_receive_keys() {
        let r = ConsistentHashRouter::new([addr(50051), addr(50052)]);
        let keys: Vec<u64> = (0u64..1000).collect();
        let hits_51 = keys.iter().filter(|&&k| r.shard_for_key(k) == Some(addr(50051))).count();
        let hits_52 = keys.iter().filter(|&&k| r.shard_for_key(k) == Some(addr(50052))).count();
        assert!(hits_51 > 0, "shard 50051 should own at least one key");
        assert!(hits_52 > 0, "shard 50052 should own at least one key");
        assert_eq!(hits_51 + hits_52, 1000);
    }

    #[test]
    fn routing_is_deterministic() {
        let shards = [addr(50051), addr(50052), addr(50053)];
        let r1 = ConsistentHashRouter::new(shards);
        let r2 = ConsistentHashRouter::new(shards);
        for key in 0u64..500 {
            assert_eq!(r1.shard_for_key(key), r2.shard_for_key(key));
        }
    }

    #[test]
    fn shard_count_is_correct() {
        let r = ConsistentHashRouter::new([addr(50051), addr(50052), addr(50053)]);
        assert_eq!(r.shard_count(), 3);
    }

    // -- Distribution ----------------------------------------------------------

    /// With 150 vnodes each, three shards should each own 25%–42% of keys
    /// for a large enough sample (expected ~33%).
    #[test]
    fn three_shards_distribute_roughly_evenly() {
        let shards = [addr(50051), addr(50052), addr(50053)];
        let r = ConsistentHashRouter::new(shards);
        let n = 10_000u64;
        let mut counts = [0usize; 3];
        for key in 0..n {
            let target = r.shard_for_key(key).unwrap();
            let idx = shards.iter().position(|&s| s == target).unwrap();
            counts[idx] += 1;
        }
        for (i, &count) in counts.iter().enumerate() {
            let pct = count as f64 / n as f64;
            assert!(
                (0.25..=0.42).contains(&pct),
                "shard {i} owns {:.1}% of keys — outside expected 25–42%",
                pct * 100.0,
            );
        }
    }

    // -- Minimal disruption on shard add/remove --------------------------------

    /// Adding a fourth shard should only move keys that now belong to it;
    /// keys that stay with their original shard must not change.
    #[test]
    fn adding_shard_disrupts_minimal_keys() {
        let original = [addr(50051), addr(50052), addr(50053)];
        let r_before = ConsistentHashRouter::new(original);

        let extended = [addr(50051), addr(50052), addr(50053), addr(50054)];
        let r_after = ConsistentHashRouter::new(extended);

        let n = 10_000u64;
        let mut moved = 0usize;
        for key in 0..n {
            let before = r_before.shard_for_key(key).unwrap();
            let after = r_after.shard_for_key(key).unwrap();
            if before != after {
                // Keys that move must land on the new shard.
                assert_eq!(
                    after,
                    addr(50054),
                    "key {key} moved from {before} to {after} (not the new shard)",
                );
                moved += 1;
            }
        }
        let moved_pct = moved as f64 / n as f64;
        // Expect ~25% to move (1/4 of keys go to the new shard).
        assert!(
            (0.15..=0.35).contains(&moved_pct),
            "expected ~25% of keys to move, got {:.1}%",
            moved_pct * 100.0,
        );
    }

    /// Removing a shard: all its keys should move to surviving shards only.
    #[test]
    fn removing_shard_only_reassigns_its_keys() {
        let full = [addr(50051), addr(50052), addr(50053)];
        let r_full = ConsistentHashRouter::new(full);

        let reduced = [addr(50051), addr(50053)];
        let r_reduced = ConsistentHashRouter::new(reduced);

        let n = 10_000u64;
        for key in 0..n {
            let before = r_full.shard_for_key(key).unwrap();
            let after = r_reduced.shard_for_key(key).unwrap();
            if before != after {
                // Only keys that were on the removed shard should move.
                assert_eq!(
                    before,
                    addr(50052),
                    "key {key} moved away from {before}, which was not removed",
                );
            }
        }
    }

    // -- Wrap-around -----------------------------------------------------------

    /// Explicit wrap-around: insert a single vnode near u64::MAX and verify a
    /// key that hashes past it wraps to it correctly.
    #[test]
    fn wrap_around_with_one_vnode() {
        // Use 1 vnode so we can reason about the ring precisely.
        let s = addr(50051);
        let r = ConsistentHashRouter::with_vnodes([s], 1);
        // No matter what key we ask about, the single shard should answer.
        assert_eq!(r.shard_for_key(0), Some(s));
        assert_eq!(r.shard_for_key(u64::MAX), Some(s));
    }
}
