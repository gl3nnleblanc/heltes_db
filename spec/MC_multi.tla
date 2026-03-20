---- MODULE MC_multi ----
(* Multi-coordinator, single-shard model for TLC.
 *
 * Two coordinators (c1, c2), one shard (s1), one key (k1→s1),
 * two transactions (t1→c1, t2→c2).  This minimal configuration exercises:
 *   - Concurrent multi-coordinator operations against a shared shard
 *   - Cross-coordinator Inquire (t1 on c1 reads k1 prepared by t2 on c2)
 *   - Multi-coordinator clock divergence and CoordSyncClockBatch
 *   - CoordAbortFromPrepare across coordinator boundaries
 *   - Write-write conflicts between transactions owned by different coordinators
 *
 * Using one key and one shard keeps the state space tractable (<10M states)
 * while still exercising all cross-coordinator protocol invariants.  The key
 * multi-coordinator concern — independent coordinator clocks creating
 * inconsistent start_ts or commit_ts values — is fully exercised even with
 * a single shared key: t1 (on c1) and t2 (on c2) both attempt to write k1,
 * and the protocol must correctly detect and resolve the conflict.
 *
 * Note: SI2 (global commit-timestamp uniqueness) is omitted from
 * MC_MultiInvariant.  Two independent coordinators can commit at the same
 * logical timestamp without violating SI; per-key version uniqueness (SI3)
 * and write-write conflict detection (SI4) are the true requirements.
 * SI2 is checked in the single-coordinator config.
 *)
EXTENDS HeltesDB

CONSTANTS t1, t2, k1, v1, v2, c1, c2, s1

MC_multi_CoordOf == (t1 :> c1 @@ t2 :> c2)
MC_multi_ShardOf == (k1 :> s1)

\* Multi-coordinator safety invariant — see note above.
MC_MultiInvariant ==
    TypeInvariant /\ WriteKeyOwnerConsistent /\ SI3 /\ SI4
====
