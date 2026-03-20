---------------------------- MODULE HeltesDB ----------------------------
(*
 * TLA+ specification of the HeltesDB Snapshot Isolation protocol.
 *
 * Models the distributed SI protocol for a sharded key-value store.
 *
 * Architecture:
 *   - Coordinator processes manage transaction lifecycle (start/read/update/commit)
 *   - Shard processes own keys and maintain MVCC version history
 *   - All communication is via explicit message passing
 *
 * Assumptions (matching the proof):
 *   - Each key k is owned by exactly one shard: ShardOf[k]
 *   - Each transaction id has exactly one coordinator: CoordOf[id]
 *   - Processes do not fail
 *   - Read-only and single-shard transactions are not modeled (out of scope)
 *
 * Properties checked:
 *   SI1 - T2 reads T1's write if commit_t(T1) <= start_t(T2)
 *   SI2 - Committed transactions have unique commit timestamps
 *   SI3 - Reads return the latest committed version strictly before start_t
 *   SI4 - Write-write conflicts prevent two concurrent transactions from both
 *         committing writes to the same key
 *)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    TxIds,          \* Set of transaction identifiers
    Keys,           \* Set of keys
    Values,         \* Set of data values
    Coordinators,   \* Set of coordinator process identifiers
    Shards,         \* Set of shard process identifiers
    CoordOf,        \* CoordOf[id] : TxId -> Coordinator
                    \*   In the implementation, CoordOf is derived from the tx_id
                    \*   encoding itself (high bits = coordinator port), so no
                    \*   external lookup or coordination between coordinators is needed.
                    \*
                    \*   The spec assumes TxIds are globally unique — including across
                    \*   coordinator restarts.  The implementation enforces this by
                    \*   seeding TxIdGen with a time-mixed starting sequence on each
                    \*   construction, so a restarted coordinator's new seq numbers are
                    \*   pseudo-randomly offset from the previous epoch's, making
                    \*   collisions with surviving shard-side aborted entries negligibly
                    \*   unlikely (probability ≈ aborted_set_size / 2^32 per transaction).
    ShardOf,        \* ShardOf[k]  : Key  -> Shard
    MaxTimestamp,   \* Upper bound on timestamps (for finite model checking)
    MaxHops,        \* Maximum cross-coordinator inquiry hop depth; guards circular-inquiry deadlock
    MaxWritesPerTx, \* Per-transaction write-buffer limit (number of distinct keys per tx per shard)
                    \*   Prevents a misbehaving client from exhausting shard memory by issuing
                    \*   unlimited Update calls.  When a write would add a new key to write_buff[id]
                    \*   and |write_buff[id]| >= MaxWritesPerTx, the shard aborts the transaction.
                    \*   Overwriting an already-buffered key does not count against the limit.
                    \*   In the implementation this is a configurable field on ShardState
                    \*   (--max-writes-per-tx CLI flag), defaulting to usize::MAX (unlimited).
    ABORT,          \* Sentinel: abort signal (model value, not in Values)
    NONE            \* Sentinel: absent/not-found (model value, not in Timestamps or Values)

ASSUME ABORT \notin Values
ASSUME MaxWritesPerTx >= 1
ASSUME \A id \in TxIds    : CoordOf[id] \in Coordinators
ASSUME \A k  \in Keys     : ShardOf[k]  \in Shards

Timestamps == 0..MaxTimestamp

------------------------------------------------------------------------
(*
 * Benchmark workload profiles (informational — no new protocol actions).
 *
 * The bench binary exercises the following existing spec actions.  All paths
 * are already covered by the spec; no new states or transitions are needed.
 *
 * Profile "uniform-write-single-shard":
 *   ClientBegin → ClientUpdate → CoordFastCommit (ShardFastCommit).
 *   Conflict rate: near zero with uniform key distribution.
 *
 * Profile "uniform-write-multi-shard":
 *   ClientBegin → ClientUpdate × 2 → CoordBeginCommit → ShardPrepare × 2
 *     → CoordCollectPrepare × 2 → CoordSendCommit → ShardCommit × 2.
 *   Exercises the full two-phase commit path.
 *
 * Profile "hot-key" (Zipf α ≥ 1):
 *   Same as uniform-write but many transactions race on the same hot key.
 *   Exercises CommittedConflict (write_start_ts < commit_t of a concurrent
 *   writer) and PreparedConflict (write_key_owner already set) abort paths.
 *
 * Profile "read-heavy":
 *   ClientBegin → ShardRead (loop) → CoordHandleInquiry (if needed) → Abort.
 *   Exercises the NeedsInquiry / resolve_inquiry path under high concurrency.
 *
 * Criterion micro-benchmark hot paths (benches/shard_hot_paths.rs):
 *
 *   handle_read / ShardRead — binary-search cost over N MVCC versions.
 *     Trace: commit N versions for key k; read with start_ts > all of them.
 *     Measures O(log N) partition_point over versions[k].
 *
 *   handle_update / ShardUpdate — O(P) PreparedConflict scan.
 *     Trace: prepare P transactions on distinct keys; update key 0 (no conflict).
 *     Measures the full prepared.iter().any(…) loop that the inverted-index
 *     Performance TODO would replace with an O(1) lookup.
 *
 *   handle_commit / ShardCommit — install W writes + update write_keys index.
 *     Trace: buffer W writes; prepare; commit at ts T.
 *     Measures binary-search insertion into versions[key] × W.
 *
 *   compact_versions / ShardCompactVersions — watermark-gated MVCC pruning.
 *     Trace: commit N keys × 5 versions; hold sentinel tx at high start_ts;
 *     compact_versions prunes versions 1–4 for every dirty key.
 *     Measures drain(..cutoff-1) across |dirty_keys|.
 *)
------------------------------------------------------------------------
(* State variables *)

VARIABLES
    \* -- Coordinator state --
    c_clock,        \* c_clock[coord]    : Nat
    start_t,        \* start_t[id]       : Nat
    commit_t,       \* commit_t[id]      : Nat | NONE
    is_committed,   \* is_committed[id]  : Bool
    participants,   \* participants[id]  : SUBSET Shards  (write-path shards)
    read_participants, \* read_participants[id] : SUBSET Shards  (shards that served reads)
                    \*   Populated by CoordRead when a READ_KEY is sent.
                    \*   Included in the abort fan-out so read-serving shards can clear
                    \*   read_start_ts and read_times entries promptly, rather than waiting
                    \*   for TTL-based expire_reads() to fire.  No new shard logic is needed:
                    \*   handle_abort already clears both read_start_ts and read_times.
    tx_state,       \* tx_state[id]      : TxStateSet (see below)

    \* -- Shard state --
    s_clock,        \* s_clock[shard]       : Nat
    versions,       \* versions[k]          : SUBSET (Value x Timestamp)  (MVCC history)
    write_buff,     \* write_buff[id]       : SUBSET (Key x Value)         (buffered writes)
    write_key_owner,\* write_key_owner[k]   : TxId | NONE  (O(1) write-lock index; NONE = unlocked)
                    \*   Refinement of write_buff: write_key_owner[k] = id iff
                    \*   ∃ kv ∈ write_buff[id] : kv[1] = k ∧ id ∉ s_aborted[ShardOf[k]]
                    \*   Maintained by ShardHandleUpdate (set), ShardHandleCommit/Abort (clear).
    s_prepared,     \* s_prepared[shard]    : SUBSET (TxId x Timestamp)   (id -> prep_t)
    s_aborted,      \* s_aborted[shard]     : SUBSET TxIds
    dirty_keys,     \* dirty_keys[shard]    : SUBSET Keys  (keys committed since last compaction)
    s_readers,      \* s_readers[shard]     : SUBSET TxIds (txids with active reads served by this shard)
                    \*   Populated by ShardHandleRead. Cleared on commit/abort at this shard.
                    \*   For read-only transactions (never in participants[id]) the entry
                    \*   remains until the transaction finishes (tx_state # ACTIVE); the
                    \*   watermark filters by tx_state[id] = "ACTIVE" so stale entries are
                    \*   harmless. In the implementation, expire_reads() provides TTL-based
                    \*   cleanup for abandoned read-only transactions.

    \* -- Network --
    msgs            \* msgs : SUBSET Message  (unordered, duplicates allowed)

vars == <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
          s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers, msgs>>

------------------------------------------------------------------------
(* Types *)

TxStateSet == {"IDLE", "ACTIVE", "PREPARING", "COMMIT_WAIT", "COMMITTED", "ABORTED"}

TypeInvariant ==
    /\ c_clock       \in [Coordinators -> Timestamps]
    /\ start_t       \in [TxIds -> Timestamps]
    /\ \A id \in TxIds : IF commit_t[id] = NONE THEN TRUE ELSE commit_t[id] \in Timestamps
    /\ is_committed  \in [TxIds -> BOOLEAN]
    /\ participants      \in [TxIds -> SUBSET Shards]
    /\ read_participants \in [TxIds -> SUBSET Shards]
    /\ tx_state          \in [TxIds -> TxStateSet]
    /\ s_clock       \in [Shards -> Timestamps]
    /\ versions         \in [Keys -> SUBSET (Values \X Timestamps)]
    /\ write_buff       \in [TxIds -> SUBSET (Keys \X Values)]
    /\ write_key_owner  \in [Keys -> TxIds \cup {NONE}]
    /\ s_prepared       \in [Shards -> SUBSET (TxIds \X Timestamps)]
    /\ s_aborted        \in [Shards -> SUBSET TxIds]
    /\ dirty_keys       \in [Shards -> SUBSET Keys]
    /\ s_readers        \in [Shards -> SUBSET TxIds]

------------------------------------------------------------------------
(* Message constructors *)

ReadKeyMsg(id, t, k)     == [type |-> "READ_KEY",         txid |-> id, ts |-> t,    key |-> k]
ReadKeyReply(id, v)      == [type |-> "READ_KEY_REPLY",   txid |-> id, val |-> v]
UpdateKeyMsg(id, t, k,v) == [type |-> "UPDATE_KEY",       txid |-> id, ts |-> t,    key |-> k, val |-> v]
UpdateKeyReply(id, k, r) == [type |-> "UPDATE_KEY_REPLY", txid |-> id, key |-> k, result |-> r]
PrepareMsg(id)           == [type |-> "PREPARE",          txid |-> id]
PrepareReply(id, r)      == [type |-> "PREPARE_REPLY",    txid |-> id, result |-> r]
CommitMsg(id, t)         == [type |-> "COMMIT",           txid |-> id, ts |-> t]
AbortMsg(id)             == [type |-> "ABORT",            txid |-> id]
InquireMsg(id, t, asker, hops) == [type |-> "INQUIRE",    txid |-> id, ts |-> t,    from |-> asker, hops |-> hops]
InquireReply(id, r, t, dest, cc) ==
    [type |-> "INQUIRE_REPLY", txid |-> id, result |-> r, ts |-> t, to |-> dest, cc |-> cc]
\* Fast-path: combines Prepare+Commit into a single shard-side operation.
\* Reply ts = NONE means the shard aborted; ts ≠ NONE is the assigned commit timestamp.
FastCommitMsg(id)        == [type |-> "FAST_COMMIT",       txid |-> id]
FastCommitReply(id, ct)  == [type |-> "FAST_COMMIT_REPLY", txid |-> id, ts |-> ct]
\* Write-and-fast-commit: coordinator sends a single RPC carrying the write payload,
\* eliminating the separate UPDATE_KEY → FAST_COMMIT round trips.
\* Only valid for single-write single-shard transactions (participants empty at send time).
\* The shard reply reuses FastCommitReply (ts = NONE on abort/conflict).
WriteAndFastCommitMsg(id, k, v) == [type |-> "WRITE_AND_FAST_COMMIT", txid |-> id, key |-> k, val |-> v]

Send(m)  == msgs' = msgs \cup {m}
SendAll(S) == msgs' = msgs \cup S

------------------------------------------------------------------------
(* Helpers *)

\* The latest committed (value, timestamp) pair for key k strictly before t.
\* Returns <<NONE, 0>> if no version exists before t (key does not exist at that snapshot).
\*
\* Implementation note: the Rust implementation maintains versions[k] as a Vec<Version>
\* sorted ascending by timestamp. This operator is therefore implemented as a binary
\* search (partition_point) in O(log N) rather than a linear scan. All three linear
\* scans on the sorted vec — handle_read (find latest visible version),
\* handle_update (CommittedConflict: any version with ts >= start_ts?), and
\* handle_commit/handle_fast_commit (idempotency: ts == commit_ts already installed?) —
\* are replaced with O(log N) binary-search equivalents.
LatestVersionBefore(k, t) ==
    LET eligible == {tv \in versions[k] : tv[2] < t}
    IN IF eligible = {}
       THEN <<NONE, 0>>
       ELSE CHOOSE tv \in eligible : \A tv2 \in eligible : tv2[2] <= tv[2]

\* True if shard s has a prepared transaction that buffered a write to key k
\* with a prepare timestamp >= t  (used for write-write conflict detection)
PreparedConflict(s, k, t) ==
    \E pair \in s_prepared[s] :
        /\ pair[2] >= t
        /\ \E kv \in write_buff[pair[1]] : kv[1] = k

\* True if there is a committed version of k with timestamp >= t
CommittedConflict(k, t) ==
    \E tv \in versions[k] : tv[2] >= t

\* True if any other non-aborted transaction already holds a write to k
\* (the "write lock" implicit in the protocol's unlock-on-abort/commit semantics)
WriteBuffConflict(s, k, id) ==
    \E other \in TxIds :
        /\ other # id
        /\ other \notin s_aborted[s]
        /\ \E kv \in write_buff[other] : kv[1] = k

\* O(1) equivalent of WriteBuffConflict using the write_key_owner index.
\* Correct whenever WriteKeyOwnerConsistent holds.
WriteBuffConflictFast(k, id) ==
    write_key_owner[k] # NONE /\ write_key_owner[k] # id

\* Consistency invariant for the write_key_owner index:
\*   - If write_key_owner[k] = NONE then no non-aborted tx has k in its write_buff.
\*   - If write_key_owner[k] = id  then id has k in its write_buff.
WriteKeyOwnerConsistent ==
    \A k \in Keys :
        IF write_key_owner[k] = NONE
        THEN \A id \in TxIds :
                 \A kv \in write_buff[id] : kv[1] # k
        ELSE \E kv \in write_buff[write_key_owner[k]] : kv[1] = k

\* Max of a non-empty set of naturals
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x

\* Min of a non-empty set of naturals
MinOf(S) == CHOOSE x \in S : \A y \in S : y >= x

------------------------------------------------------------------------
(* Initial state *)

Init ==
    /\ c_clock      = [coord \in Coordinators |-> 0]
    /\ start_t      = [id \in TxIds |-> 0]
    /\ commit_t     = [id \in TxIds |-> NONE]
    /\ is_committed = [id \in TxIds |-> FALSE]
    /\ participants      = [id \in TxIds |-> {}]
    /\ read_participants = [id \in TxIds |-> {}]
    /\ tx_state          = [id \in TxIds |-> "IDLE"]
    /\ s_clock          = [s  \in Shards |-> 0]
    /\ versions         = [k  \in Keys   |-> {}]
    /\ write_buff       = [id \in TxIds  |-> {}]
    /\ write_key_owner  = [k  \in Keys   |-> NONE]
    /\ s_prepared       = [s  \in Shards |-> {}]
    /\ s_aborted        = [s  \in Shards |-> {}]
    /\ dirty_keys       = [s  \in Shards |-> {}]
    /\ s_readers        = [s  \in Shards |-> {}]
    /\ msgs             = {}

------------------------------------------------------------------------
(* Coordinator actions *)

\* start_tx(): assign start_t = now().latest, simplified as clock + 1
CoordStartTx(id) ==
    LET coord == CoordOf[id]
        t     == c_clock[coord] + 1
    IN
    /\ tx_state[id] = "IDLE"
    /\ t \in Timestamps
    /\ tx_state'     = [tx_state     EXCEPT ![id]    = "ACTIVE"]
    /\ start_t'      = [start_t      EXCEPT ![id]    = t]
    /\ c_clock'      = [c_clock      EXCEPT ![coord] = t]
    /\ is_committed'      = [is_committed      EXCEPT ![id] = FALSE]
    /\ participants'      = [participants      EXCEPT ![id] = {}]
    /\ read_participants' = [read_participants EXCEPT ![id] = {}]
    /\ UNCHANGED <<commit_t, s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers, msgs>>

\* read(id, k): send READ_KEY to the owning shard; record the serving shard in
\* read_participants so abort_and_notify can fan out AbortMsg to read-only shards.
\*
\* Concrete execution traces driving Phase-2 tests (CoordAbortNotifiesReadShards):
\*   RN1: tx reads k, abort fires → read shard (ShardOf[k]) in read_participants[id]
\*   RN2: tx reads k then writes k2 (same shard) → participants = read_participants = {s}
\*   RN3: tx reads k1 (shard s1) and k2 (shard s2) → read_participants = {s1, s2}
\*   RN4: tx has no reads (write-only) → read_participants = {}; abort still notifies
\*        write participants only (no regression)
\*   RN5: abort notifies participants ∪ read_participants; ShardHandleAbort clears
\*        s_readers on both sets
CoordRead(id, k) ==
    /\ tx_state[id] = "ACTIVE"
    /\ read_participants' = [read_participants EXCEPT ![id] = @ \cup {ShardOf[k]}]
    /\ Send(ReadKeyMsg(id, start_t[id], k))
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>

\* update(id, k, v): track participant shard, send UPDATE_KEY
CoordUpdate(id, k, v) ==
    /\ tx_state[id] = "ACTIVE"
    /\ participants' = [participants EXCEPT ![id] = @ \cup {ShardOf[k]}]
    /\ Send(UpdateKeyMsg(id, start_t[id], k, v))
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, read_participants, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>

\* Coordinator aborts a transaction because the read-loop global deadline expired.
\*
\* The coordinator's read() handler wraps the NeedsInquiry retry loop in a
\* tokio::time::timeout(read_loop_timeout, ...).  If the loop cannot resolve all
\* prepared writers within that wall-clock window (e.g. because a committed
\* writer's version has not yet been installed at the shard), the coordinator
\* aborts the reader transaction and returns Abort to the client.
\*
\* In the spec, deadlines are modelled as non-deterministic: any ACTIVE
\* transaction may be aborted by the deadline at any point.  This is sound
\* because aborting a transaction is always safe w.r.t. SI1–SI4, and TLC
\* will explore both the deadline-fires and deadline-does-not-fire paths,
\* verifying safety in all branches.
\*
\* Implementation note — exponential backoff with jitter between retries:
\*   After each NeedsInquiry reply the implementation sleeps for
\*     base = min(initial_delay * 2^retry, max_delay)
\*     jitter = deterministic pseudo-random in [0, jitter_max]
\*   before sending the next READ_KEY to the shard.  This is purely a
\*   timing concern and does not affect any TLA+ state variables; the
\*   spec is unchanged.  The backoff is configured by ReadRetryPolicy
\*   (initial_delay=5 ms, max_delay=100 ms, jitter_max=10 ms by default).
\*
\* Concrete execution traces modelled here (drive Phase-2 tests):
\*   T1: READ_KEY → Value                             (no NeedsInquiry, no sleep)
\*   T2: READ_KEY → NeedsInquiry → sleep(initial) → READ_KEY → Value
\*   T3: READ_KEY → NeedsInquiry → sleep(initial) → READ_KEY →
\*         NeedsInquiry → sleep(2*initial) → ... → CoordAbortReadDeadline
\*   T4: delay doubles each retry but is capped at max_delay once 2^k * initial >= max
\*   T5: initial_delay = 0  →  no sleep at all  (ReadRetryPolicy::no_backoff)
CoordAbortReadDeadline(id) ==
    /\ tx_state[id] = "ACTIVE"
    /\ tx_state' = [tx_state EXCEPT ![id] = "ABORTED"]
    /\ SendAll({AbortMsg(id)})
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>

\* Coordinator receives an ABORT reply from a read or update operation
CoordAbortOnReply(id) ==
    /\ tx_state[id] = "ACTIVE"
    /\ \/ ReadKeyReply(id, ABORT) \in msgs
       \/ \E k \in Keys : UpdateKeyReply(id, k, ABORT) \in msgs
    /\ tx_state' = [tx_state EXCEPT ![id] = "ABORTED"]
    /\ SendAll({AbortMsg(id)})
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>

\* Single-shard fast path: coordinator sends FAST_COMMIT to the sole participant
\* shard, skipping the two-round-trip Prepare → Commit sequence.
\* Guard conditions mirror CoordBeginCommit (all update acks received).
\* Reuses the PREPARING phase while waiting for FastCommitReply.
\*
\* Implementation note — concurrent-abort protection:
\*   The implementation's `begin_fast_commit` transitions `tx.phase → Preparing`
\*   before returning the shard id to the caller.  This matches the spec's
\*   `tx_state' = PREPARING` assignment here.  The implementation's `abort()` RPC
\*   handler checks the current phase and is a no-op when phase = Preparing, because
\*   `CoordAbortOnReply` requires `tx_state = "ACTIVE"` and cannot fire during PREPARING.
\*   Without this guard, a concurrent client abort could set coordinator state to
\*   ABORTED while the shard's handle_fast_commit has already installed the write,
\*   leaving coordinator = Aborted with data permanently visible to future readers.
\*   The `finalize_fast_commit` implementation checks for `Preparing` phase (not Active)
\*   and transitions directly to Committed, matching CoordHandleFastCommitReply here.
\*
\* Concrete execution traces driving Phase-2 tests:
\*   T1: ACTIVE→PREPARING (begin_fast_commit), shard ok, PREPARING→COMMITTED (finalize)
\*   T2: ACTIVE→PREPARING (begin_fast_commit), shard abort, PREPARING→ABORTED (abort_tx)
\*   T3: CoordAbortOnReply disabled during PREPARING — client abort RPC is no-op
\*   T4: Internal abort (shard reply path calls abort_tx) still transitions PREPARING→ABORTED
\*   T5: finalize_fast_commit on ACTIVE phase (begin_fast_commit never called) → NotReady
CoordFastCommit(id) ==
    /\ tx_state[id] = "ACTIVE"
    /\ participants[id] # {}
    /\ Cardinality(participants[id]) = 1
    /\ \A m \in msgs :
           (m.type = "UPDATE_KEY" /\ m.txid = id)
           => UpdateKeyReply(id, m.key, "OK") \in msgs
    /\ tx_state' = [tx_state EXCEPT ![id] = "PREPARING"]
    /\ Send(FastCommitMsg(id))
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>

\* Coordinator receives a FastCommitReply.
\* ts = NONE means the shard aborted; ts ≠ NONE is the assigned commit timestamp.
CoordHandleFastCommitReply(id) ==
    LET coord == CoordOf[id]
        reply == CHOOSE m \in msgs :
                     /\ m.type = "FAST_COMMIT_REPLY"
                     /\ m.txid = id
    IN
    /\ \E m \in msgs : m.type = "FAST_COMMIT_REPLY" /\ m.txid = id
    /\ tx_state[id] = "PREPARING"
    /\ IF reply.ts = NONE
       THEN \* shard aborted
            /\ tx_state' = [tx_state EXCEPT ![id] = "ABORTED"]
            /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants,
                           s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers, msgs>>
       ELSE \* shard committed — finalise at the coordinator
            LET ct == reply.ts
            IN
            /\ ct \in Timestamps
            /\ ct + 1 \in Timestamps
            /\ tx_state'     = [tx_state     EXCEPT ![id]    = "COMMITTED"]
            /\ is_committed' = [is_committed EXCEPT ![id]    = TRUE]
            /\ commit_t'     = [commit_t     EXCEPT ![id]    = ct]
            /\ c_clock'      = [c_clock      EXCEPT ![coord] =
                                    IF ct + 1 > c_clock[coord] THEN ct + 1 ELSE c_clock[coord]]
            /\ UNCHANGED <<start_t, participants, read_participants, s_clock, versions,
                           write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers, msgs>>

\* Single-RPC write-and-fast-commit optimisation: coordinator sends one message
\* carrying the write payload to the owning shard.  The shard atomically checks
\* conflicts, installs the write, and commits — eliminating the separate
\* UPDATE_KEY → FAST_COMMIT round trips.
\*
\* Precondition: participants must be empty (no prior updates on this tx).
\* This constraint ensures the optimisation path is only taken for fresh,
\* single-write single-shard transactions, preserving protocol correctness.
\*
\* The coordinator transitions ACTIVE → PREPARING and records the participant
\* shard before sending, exactly as CoordFastCommit does.  The reply handler
\* CoordHandleFastCommitReply is reused unchanged.
\*
\* Concrete execution traces driving Phase-2 tests:
\*   WF1: ACTIVE → PREPARING, shard ok, PREPARING → COMMITTED (normal path)
\*   WF2: ACTIVE → PREPARING, shard conflicts → PREPARING → ABORTED
\*   WF3: CoordAbortOnReply disabled during PREPARING (same guard as FastCommit)
CoordWriteAndFastCommit(id, k, v) ==
    /\ tx_state[id] = "ACTIVE"
    /\ participants[id] = {}
    /\ tx_state'     = [tx_state     EXCEPT ![id] = "PREPARING"]
    /\ participants' = [participants EXCEPT ![id] = {ShardOf[k]}]
    /\ Send(WriteAndFastCommitMsg(id, k, v))
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, read_participants,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>

\* Shard handles WRITE_AND_FAST_COMMIT(id, k, v) — atomically check conflicts,
\* install the write, and commit in a single step.
\* Bypasses write_buff entirely: the write goes directly into versions[k].
\* Reuses FastCommitReply for the response.
\*
\* Conflict checks are identical to ShardHandleUpdate:
\*   CommittedConflict(k, t)      — another tx committed k at ts >= start_t
\*   PreparedConflict(s, k, t)    — another prepared tx holds k with prep_t >= start_t
\*   WriteBuffConflictFast(k, id) — another tx currently holds the write lock for k
\*                                   (write_key_owner[k] ≠ NONE and ≠ id)
\*
\* WriteBuffConflictFast is essential: if another tx has k buffered (not yet
\* committed or aborted), it may commit k at any future timestamp, including one
\* ≥ start_t[id], violating SI4.  Omitting this check allows two concurrent txns
\* to both commit to the same key — an SI4 violation confirmed by TLC.
ShardHandleWriteAndFastCommit(s, id, k, v) ==
    LET t  == start_t[id]
        ct == s_clock[s] + 1
        m  == WriteAndFastCommitMsg(id, k, v)
    IN
    /\ m \in msgs
    /\ ShardOf[k] = s
    /\ tx_state[id] = "PREPARING"
    /\ ct \in Timestamps
    /\ IF id \in s_aborted[s]
       THEN \* already aborted at this shard — reply abort
            /\ Send(FastCommitReply(id, NONE))
            /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                           s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>
       ELSE IF CommittedConflict(k, t) \/ PreparedConflict(s, k, t) \/ WriteBuffConflictFast(k, id)
            THEN \* write-write conflict — abort
                 /\ s_aborted' = [s_aborted EXCEPT ![s] = @ \cup {id}]
                 /\ Send(FastCommitReply(id, NONE))
                 /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                                s_clock, versions, write_buff, write_key_owner, s_prepared, dirty_keys, s_readers>>
            ELSE \* no conflict — install version directly (bypass write_buff)
                 /\ versions'   = [versions   EXCEPT ![k] = @ \cup {<<v, ct>>}]
                 /\ s_clock'    = [s_clock    EXCEPT ![s] = ct]
                 /\ dirty_keys' = [dirty_keys EXCEPT ![s] = @ \cup {k}]
                 /\ Send(FastCommitReply(id, ct))
                 /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                                write_buff, write_key_owner, s_prepared, s_aborted, s_readers>>

\* commit_tx(id): send PREPARE to all participant shards (2PC phase 1)
\* Guard: all UPDATE_KEY messages for this tx have received OK replies,
\* modelling the coordinator waiting for acknowledgements before committing.
\*
\* Implementation note: the PREPARING phase is guarded by a configurable
\* shard_rpc_timeout applied to the join_all(PREPARE RPCs). If all replies do
\* not arrive within that deadline, the coordinator treats it identically to
\* receiving ABORT from the outstanding shards, and falls through to the same
\* abort path as CoordAbortOnReply. The same deadline guards FastCommit, Read,
\* and Update shard RPCs — a timeout on any of those propagates as an error to
\* the client and (for write-path RPCs) aborts the transaction.
\* shard_rpc_timeout and read_loop_timeout are configurable at startup via
\* --shard-rpc-timeout-ms and --read-loop-timeout-ms CLI flags on the
\* coordinator binary (default: 30 000 ms each).
CoordBeginCommit(id) ==
    /\ tx_state[id] = "ACTIVE"
    /\ participants[id] # {}
    /\ \A m \in msgs :
           (m.type = "UPDATE_KEY" /\ m.txid = id)
           => UpdateKeyReply(id, m.key, "OK") \in msgs
    /\ tx_state' = [tx_state EXCEPT ![id] = "PREPARING"]
    /\ SendAll({PrepareMsg(id)})
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>

\* CoordAbortFromPrepare(id): coordinator sees an ABORT reply from a shard during
\* the PREPARE phase of 2PC.  At least one participating shard has voted Abort;
\* the coordinator transitions to ABORTED and sends AbortMsg to all participants.
\*
\* This action closes the gap in the 2PC state machine: CoordFinalizePrepare only
\* fires when no shard aborted (anyAbort = FALSE); CoordAbortFromPrepare fires in
\* the complementary case.  Without this action the spec has no transition out of
\* PREPARING when a shard votes Abort, leaving a dead state that TLC cannot explore.
\*
\* The implementation's post-prepare-abort notification (join_all(AbortMsg)) is
\* bounded by shard_rpc_timeout so a hung shard cannot stall the commit() RPC.
\* In the message-passing model this is captured by AbortMsg being sent to the
\* network non-deterministically delivered: ShardHandleAbort may fire or not; if
\* not, ShardTimeoutPrepared eventually cleans up the prepared entry on the shard.
\*
\* Concrete execution traces driving Phase-2 tests:
\*   P1: 2 shards, shard 1 returns ABORT first, coordinator aborts (PREPARING → ABORTED)
\*   P2: 2 shards, both return ABORT, coordinator aborts (same result)
\*   P3: tx in COMMIT_WAIT → action disabled (requires PREPARING)
\*   P4: tx in ACTIVE → action disabled (requires PREPARING; misrouted call)
CoordAbortFromPrepare(id) ==
    /\ tx_state[id] = "PREPARING"
    /\ \E m \in msgs : m.type = "PREPARE_REPLY" /\ m.txid = id /\ m.result = ABORT
    /\ tx_state' = [tx_state EXCEPT ![id] = "ABORTED"]
    /\ SendAll({AbortMsg(id)})
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>

\* After receiving all PREPARE_REPLYs: compute commit_t, enter COMMIT_WAIT
\* commit_t = max(clock, all prepare timestamps)
CoordFinalizePrepare(id) ==
    LET coord      == CoordOf[id]
        replies    == {m \in msgs : m.type = "PREPARE_REPLY" /\ m.txid = id}
        allReplied == Cardinality(replies) >= Cardinality(participants[id])
        anyAbort   == \E m \in replies : m.result = ABORT
        prepTimes  == {m.result : m \in replies} \ {ABORT}
        ct         == MaxOf(prepTimes \cup {c_clock[coord]})
    IN
    /\ tx_state[id] = "PREPARING"
    /\ allReplied
    /\ ~anyAbort
    /\ ct \in Timestamps
    /\ ct + 1 \in Timestamps   \* commit-wait clock advance must stay in bounds
    /\ tx_state'  = [tx_state  EXCEPT ![id]    = "COMMIT_WAIT"]
    /\ commit_t'  = [commit_t  EXCEPT ![id]    = ct]
    \* Advance clock past ct so the next transaction's commit timestamp is strictly greater,
    \* ensuring SI2 (unique commit timestamps).
    /\ c_clock'   = [c_clock   EXCEPT ![coord] = ct + 1]
    /\ UNCHANGED <<start_t, is_committed, participants, read_participants, s_clock, versions,
                   write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers, msgs>>

\* Commit-wait passes (now().earliest > commit_t): send COMMIT to all participants.
\* In the logical clock model we abstract TrueTime commit-wait as: some other
\* coordinator's clock has advanced past commit_t[id], ensuring SI1.
\* For model checking we allow this step unconditionally (soundly conservative).
\*
\* The coordinator marks is_committed=TRUE and sends CommitMsg before the client
\* receives a reply.  In the implementation, the concurrent join_all(COMMIT RPCs)
\* is bounded by shard_rpc_timeout; a timeout surfaces deadline_exceeded to the
\* client but does not change the committed state.  ShardHandleCommit fires
\* asynchronously: the message is already in the network.
CoordSendCommit(id) ==
    LET ct == commit_t[id]
    IN
    /\ tx_state[id]    = "COMMIT_WAIT"
    /\ commit_t[id]   # NONE
    /\ tx_state'      = [tx_state      EXCEPT ![id] = "COMMITTED"]
    /\ is_committed'  = [is_committed  EXCEPT ![id] = TRUE]
    /\ SendAll({CommitMsg(id, ct)})
    /\ UNCHANGED <<c_clock, start_t, commit_t, participants, read_participants,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>

\* Coordinator handles INQUIRE(id, t, hops) from shard s.
\*
\* When the INQUIRE is for a transaction owned by a different coordinator, the
\* implementation forwards the request over gRPC to that coordinator. The spec
\* abstracts this with CoordOf[id]: any coordinator that owns id can answer.
\*
\* Implementation note: CoordOf is encoded in the high 32 bits of tx_id (the
\* coordinator's listening port). The server resolves the remote coordinator's
\* full address from a config-supplied table (coordinator_uris: port → URI),
\* NOT from a hardcoded host such as [::1]. This enables multi-machine deployments.
\* If the port is absent from coordinator_uris the RPC returns UNAVAILABLE.
\*
\* Deadlock guard — hop counter:
\*   A circular inquiry deadlock arises if coordinator A calls B's inquire() for
\*   T_B while B concurrently calls A's inquire() for T_A, and each handler waits
\*   on the other before answering.  The A-awaits-B-awaits-A cycle cannot be broken
\*   by shard_rpc_timeout alone because both sides are blocked inside the same
\*   timeout window.
\*
\*   Fix: each InquireMsg carries a hop counter initialised to 0 by ShardSendInquire.
\*   A handler that needs to forward the inquiry to a peer increments hops before
\*   sending.  When hops >= MaxHops the handler is disabled; the inquiry is never
\*   answered; CoordAbortReadDeadline fires for the blocked reader, breaking the cycle.
\*
\*   In the current implementation the inquire() handler answers from local state and
\*   never forwards, so hop_count stays 0 and the guard never fires.  The guard is
\*   defensive: it prevents an infinite loop if forwarding is ever added.
\*
\* Concrete execution traces driving Phase-2 tests:
\*   T1: hops=0 < MaxHops → action enabled → reply sent (normal path)
\*   T2: hops=MaxHops-1 → action enabled (last allowed hop)
\*   T3: hops=MaxHops → action DISABLED → CoordAbortReadDeadline fires for reader
\*   T4: A↔B mutual inquiry with hops=0 on both sides → both resolve immediately
CoordHandleInquire(id, s) ==
    LET coord  == CoordOf[id]
        m      == CHOOSE msg \in msgs :
                      /\ msg.type = "INQUIRE"
                      /\ msg.txid = id
                      /\ msg.from = s
        new_cc == IF m.ts > c_clock[coord] THEN m.ts ELSE c_clock[coord]
        reply  == IF is_committed[id]
                  THEN InquireReply(id, "COMMITTED", commit_t[id], s, new_cc)
                  ELSE InquireReply(id, "ACTIVE",    NONE,         s, new_cc)
    IN
    /\ \E msg \in msgs : msg.type = "INQUIRE" /\ msg.txid = id /\ msg.from = s
    /\ m.hops < MaxHops
    /\ c_clock' = [c_clock EXCEPT ![coord] = new_cc]
    /\ Send(reply)
    /\ UNCHANGED <<start_t, commit_t, is_committed, participants, read_participants, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>

------------------------------------------------------------------------
(* Shard actions *)

\* Shard handles READ_KEY(id, t, k)
\*
\* The INQUIRE sub-protocol is modeled conservatively: a shard may only reply
\* to a read once every prepared writer of k with prep_t < t has had its
\* INQUIRE_REPLY delivered. This preserves the blocking semantics from the proof
\* without modeling the full async inquiry round-trip as separate steps.
ShardHandleRead(s, id, k) ==
    LET m == ReadKeyMsg(id, start_t[id], k)
        t == start_t[id]
        \* Prepared writers of k on this shard whose prepare timestamp < t
        pendingWriters ==
            {pair \in s_prepared[s] :
                /\ pair[2] < t
                /\ \E kv \in write_buff[pair[1]] : kv[1] = k}
        \* All such writers must be resolved: either committed (version installed)
        \* or an INQUIRE_REPLY(ACTIVE) has been received
        allResolved ==
            \A pair \in pendingWriters :
                \/ is_committed[pair[1]]
                \/ \E r \in msgs :
                       /\ r.type = "INQUIRE_REPLY"
                       /\ r.txid = pair[1]
                       /\ r.to   = s
                       /\ r.result = "ACTIVE"
        lv    == LatestVersionBefore(k, t)
        reply == ReadKeyReply(id, lv[1])
    IN
    /\ m \in msgs
    /\ ShardOf[k] = s
    /\ id \notin s_aborted[s]
    /\ allResolved
    /\ s_clock'   = [s_clock   EXCEPT ![s] = IF t > s_clock[s] THEN t ELSE s_clock[s]]
    /\ s_readers' = [s_readers EXCEPT ![s] = @ \cup {id}]
    /\ Send(reply)
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                   versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys>>

\* Shard initiates INQUIRE for a prepared writer of k before replying to a read.
\* (Separate action so model checker can interleave it.)
ShardSendInquire(s, id, prepId, k) ==
    LET t == start_t[id]
    IN
    /\ ReadKeyMsg(id, t, k) \in msgs
    /\ ShardOf[k] = s
    /\ id \notin s_aborted[s]
    /\ \E pair \in s_prepared[s] :
           /\ pair[1] = prepId
           /\ pair[2] < t
           /\ \E kv \in write_buff[prepId] : kv[1] = k
    /\ ~(\E r \in msgs :
             /\ r.type = "INQUIRE_REPLY"
             /\ r.txid = prepId
             /\ r.to   = s)
    /\ Send(InquireMsg(prepId, t, s, 0))
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>

\* Shard handles UPDATE_KEY(id, t, k, v)
\*
\* Write-write conflict check (corrected from proof typo):
\*   Abort if ∃ committed version of k with ts >= start_t(id)   [committed conflict]
\*   Abort if ∃ prepared tx that wrote k with prep_t >= start_t(id)  [prepared conflict]
\*
\* Write buffer limit (MaxWritesPerTx):
\*   Abort if this write would add a NEW distinct key to write_buff[id] AND
\*   |write_buff[id]| >= MaxWritesPerTx.  Overwriting an already-buffered key
\*   does not count — only new keys consume the quota.
\*
\* Concrete execution traces driving Phase-2 tests:
\*   WL1: |write_buff[id]| < MaxWritesPerTx, new key → buffer succeeds
\*   WL2: |write_buff[id]| = MaxWritesPerTx, new key → LimitExceeded abort
\*   WL3: key already in write_buff[id], |write_buff| = MaxWritesPerTx → overwrite succeeds (no limit hit)
\*   WL4: pre-aborted tx → Abort returned immediately (limit not even checked)
ShardHandleUpdate(s, id, k, v) ==
    LET t == start_t[id]
        m == UpdateKeyMsg(id, t, k, v)
        isNewKey == ~(\E kv \in write_buff[id] : kv[1] = k)
    IN
    /\ m \in msgs
    /\ ShardOf[k] = s
    /\ tx_state[id] = "ACTIVE"
    /\ IF id \in s_aborted[s]
       THEN \* Already aborted at this shard
            /\ Send(UpdateKeyReply(id, k, ABORT))
            /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                           s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>
       ELSE IF CommittedConflict(k, t) \/ PreparedConflict(s, k, t) \/ WriteBuffConflictFast(k, id)
            THEN \* Write-write conflict — abort
                 /\ s_aborted' = [s_aborted EXCEPT ![s] = @ \cup {id}]
                 /\ Send(UpdateKeyReply(id, k, ABORT))
                 /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants,
                                tx_state, s_clock, versions, write_buff, write_key_owner, s_prepared, dirty_keys, s_readers>>
            ELSE IF isNewKey /\ Cardinality(write_buff[id]) >= MaxWritesPerTx
                 THEN \* Write buffer limit exceeded — abort to prevent unbounded memory growth
                      /\ s_aborted' = [s_aborted EXCEPT ![s] = @ \cup {id}]
                      /\ Send(UpdateKeyReply(id, k, ABORT))
                      /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants,
                                     tx_state, s_clock, versions, write_buff, write_key_owner, s_prepared, dirty_keys, s_readers>>
                 ELSE \* No conflict, within limit — buffer the write (overwrite any prior write to k by id)
                      /\ write_buff' = [write_buff EXCEPT ![id] =
                             (@ \ {kv \in @ : kv[1] = k}) \cup {<<k, v>>}]
                      /\ write_key_owner' = [write_key_owner EXCEPT ![k] = id]
                      /\ s_clock' = [s_clock EXCEPT ![s] =
                             IF t > s_clock[s] THEN t ELSE s_clock[s]]
                      /\ Send(UpdateKeyReply(id, k, "OK"))
                      /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants,
                                     tx_state, versions, s_prepared, s_aborted, dirty_keys, s_readers>>

\* Shard handles PREPARE(id) — assign a prepare timestamp and record it.
\* Guards:
\*   tx_state[id] = "PREPARING" — coordinator must still be in the prepare phase;
\*     prevents the shard from re-preparing a transaction that the coordinator has
\*     already finalized (COMMIT_WAIT/COMMITTED).  Without this, the persistent
\*     PrepareMsg message causes repeated prepare→commit cycles after the initial
\*     commit, driving s_clock to MaxTimestamp and eventually violating TypeInvariant
\*     when CoordSyncClockBatch computes s_clock+1 outside Timestamps.
\*   not already prepared — message set is persistent; prevent re-firing within phase.
ShardHandlePrepare(s, id) ==
    LET t == s_clock[s] + 1
    IN
    /\ PrepareMsg(id) \in msgs
    /\ s \in participants[id]
    /\ tx_state[id] = "PREPARING"
    /\ id \notin s_aborted[s]
    /\ id \notin {p[1] : p \in s_prepared[s]}   \* not already prepared
    /\ t \in Timestamps
    /\ s_prepared' = [s_prepared EXCEPT ![s] = @ \cup {<<id, t>>}]
    /\ s_clock'    = [s_clock    EXCEPT ![s] = t]
    /\ Send(PrepareReply(id, t))
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                   versions, write_buff, write_key_owner, s_aborted, dirty_keys, s_readers>>

\* Shard handles FAST_COMMIT(id) — atomically prepare + commit for single-shard txns.
\* Combines ShardHandlePrepare + ShardHandleCommit in one step.
\* Guard write_buff[id] ≠ {} prevents re-firing after the first successful commit.
\*
\* Implementation note — idempotent retry:
\*   The guard write_buff[id] ≠ {} prevents this spec action from firing twice,
\*   because the spec assumes processes do not fail (no coordinator crash+replay).
\*   The Rust implementation adds a fast_committed: HashMap<TxId, Timestamp> field
\*   to ShardState.  handle_fast_commit checks write_buff BEFORE advancing the clock:
\*     1. If write_buff has entry: advance clock, install versions, store commit_ts in
\*        fast_committed, return Ok(commit_ts).
\*     2. If write_buff absent AND fast_committed has entry: idempotent — return stored
\*        commit_ts WITHOUT advancing the clock (the bug was doing this advance anyway).
\*     3. If write_buff absent AND not in fast_committed: return Abort (never committed).
\*   fast_committed is pruned alongside s_aborted in prune_aborted() using the same
\*   per-port seq-watermark, so it does not grow without bound.
\*
\* Concrete execution traces driving Phase-2 tests:
\*   FC-N: write_buff non-empty, not aborted → commit, clock advances, version installed
\*   FC-I: already in fast_committed (retry) → same commit_ts, clock does NOT advance
\*   FC-A: id ∈ s_aborted → reply abort, no state change
\*   FC-E: write_buff absent AND not in fast_committed → Abort (never committed here)
ShardHandleFastCommit(s, id) ==
    /\ FastCommitMsg(id) \in msgs
    /\ s \in participants[id]
    /\ write_buff[id] # {}          \* idempotency: nothing left to commit after first fire
    /\ IF id \in s_aborted[s]
       THEN \* tx was aborted before the fast-commit arrived — reply abort;
            \* s_clock is NOT advanced (no ct needed for the abort path).
            /\ Send(FastCommitReply(id, NONE))
            /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                           s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers>>
       ELSE \* prepare + commit atomically; clock advances only in this branch
            LET ct == s_clock[s] + 1
            IN
            /\ ct \in Timestamps
            /\ versions' = [k \in Keys |->
                   LET kWrites == {kv \in write_buff[id] : kv[1] = k}
                   IN IF kWrites # {}
                      THEN versions[k] \cup {<<(CHOOSE kv \in kWrites : TRUE)[2], ct>>}
                      ELSE versions[k]]
            /\ write_buff'      = [write_buff      EXCEPT ![id] = {}]
            /\ write_key_owner' = [k \in Keys |->
                   IF \E kv \in write_buff[id] : kv[1] = k THEN NONE ELSE write_key_owner[k]]
            /\ s_prepared'      = [s_prepared      EXCEPT ![s]  = {p \in @ : p[1] # id}]
            /\ s_clock'         = [s_clock         EXCEPT ![s]  = ct]
            /\ dirty_keys'      = [dirty_keys      EXCEPT ![s]  =
                   @ \cup {k \in Keys : \E kv \in write_buff[id] : kv[1] = k}]
            /\ Send(FastCommitReply(id, ct))
            /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants,
                           tx_state, s_aborted, s_readers>>

\* Shard handles COMMIT(id, ct) — install buffered writes as new MVCC versions
\*
\* Guard id \notin s_aborted[s]: if the shard already auto-aborted this prepared
\* entry (ShardTimeoutPrepared), it must REJECT the COMMIT and signal an error
\* rather than silently succeeding with an empty write_buff. This prevents silent
\* data loss when expire_prepared fires between the coordinator's PREPARE phase
\* and the arrival of the COMMIT message.
\*
\* This guard IS reachable: ShardTimeoutPrepared fires during PREPARING (removing
\* the shard's prepared entry), then the coordinator later finalizes and calls
\* CoordSendCommit (setting is_committed[id]=TRUE), so both can fire in the same
\* execution.  The guard prevents the shard from silently installing a stale write
\* whose coordinator already considers committed — a real implementation concern.
ShardHandleCommit(s, id) ==
    LET ct == commit_t[id]
    IN
    /\ CommitMsg(id, ct) \in msgs
    /\ s \in participants[id]
    /\ is_committed[id]         \* coordinator has set the flag
    /\ id \notin s_aborted[s]   \* shard has not TTL-aborted this entry
    /\ versions' = [k \in Keys |->
           LET kWrites == {kv \in write_buff[id] : kv[1] = k}
           IN IF kWrites # {}
              THEN versions[k] \cup {<<(CHOOSE kv \in kWrites : TRUE)[2], ct>>}
              ELSE versions[k]]
    /\ write_buff'      = [write_buff      EXCEPT ![id] = {}]
    /\ write_key_owner' = [k \in Keys |->
           IF \E kv \in write_buff[id] : kv[1] = k THEN NONE ELSE write_key_owner[k]]
    /\ s_prepared'      = [s_prepared      EXCEPT ![s]  = {p \in @ : p[1] # id}]
    /\ dirty_keys'      = [dirty_keys      EXCEPT ![s]  =
           @ \cup {k \in Keys : \E kv \in write_buff[id] : kv[1] = k}]
    \* Advance s_clock to ct when the coordinator-assigned commit timestamp exceeds
    \* the shard's current clock.  This prevents a subsequent WAFC from reusing a
    \* timestamp already installed by this commit (SI3 violation confirmed by TLC).
    /\ s_clock'         = [s_clock         EXCEPT ![s]  = IF ct > s_clock[s] THEN ct ELSE s_clock[s]]
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                   s_aborted, s_readers, msgs>>

\* Shard handles ABORT(id) — discard buffered writes, release prepared entry, unlock write_key_owner
ShardHandleAbort(s, id) ==
    /\ AbortMsg(id) \in msgs
    /\ s_aborted'       = [s_aborted       EXCEPT ![s]  = @ \cup {id}]
    /\ write_buff'      = [write_buff       EXCEPT ![id] = {}]
    /\ write_key_owner' = [k \in Keys |->
           IF \E kv \in write_buff[id] : kv[1] = k THEN NONE ELSE write_key_owner[k]]
    /\ s_prepared'      = [s_prepared       EXCEPT ![s]  = {p \in @ : p[1] # id}]
    /\ s_readers'       = [s_readers        EXCEPT ![s]  = @ \ {id}]
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                   s_clock, versions, dirty_keys, msgs>>

\* Shard auto-aborts a prepared entry whose coordinator has become unresponsive.
\*
\* Models coordinator crash mid-2PC: if the coordinator sent PREPARE but crashed
\* before deciding COMMIT/ABORT (tx_state = "PREPARING"), the shard may
\* spontaneously abort the prepared entry after a configurable prepare_ttl,
\* unblocking conflicting writers.
\*
\* The guard tx_state[id] = "PREPARING" makes the action safe in all SI properties:
\* the coordinator has not committed, so no COMMIT message is in transit and
\* aborting the prepared entry loses no durable data. Once the coordinator
\* reaches COMMIT_WAIT or COMMITTED this action is disabled.
\*
\* This action also sends PrepareReply(id, ABORT) to notify the coordinator.
\* This is essential for protocol correctness: without the abort notification,
\* the coordinator could process the earlier OK PrepareReply and call
\* CoordFinalizePrepare, assigning commit_ts = max(c_clock, prep_ts). A later
\* WAFC could then claim the same timestamp (because the shard clock was not
\* advanced by the aborted commit), violating SI2. The ABORT reply ensures
\* anyAbort=TRUE in CoordFinalizePrepare (disabling it) and enables
\* CoordAbortFromPrepare to fire instead.
\*
\* Implementation note: the Rust implementation records Instant::now() at
\* handle_prepare time, then calls expire_prepared(Instant::now()) lazily at
\* the start of each update/prepare/read RPC handler. Entries older than
\* prepare_ttl are aborted with the same cleanup as handle_abort.
\* prepare_ttl is configurable at startup via the --prepare-ttl-ms CLI flag
\* on the shard binary (default: 30 000 ms).
\*
\* Implementation note — abort notification:
\*   expire_prepared() must notify the coordinator when it aborts a prepared
\*   entry. In the implementation this can be done by:
\*     (a) Proactively sending AbortMsg(id) to CoordOf[id] in the background, OR
\*     (b) Returning an error (ABORTED gRPC status) when handle_commit is called
\*         for a tx in the aborted set, and having the coordinator detect this.
\*   The spec models both via the persistent PrepareReply(id, ABORT) message;
\*   whichever path the implementation uses, the coordinator must not mark
\*   is_committed = true when a shard has aborted the prepared entry.
\*
\* Concrete execution traces driving Phase-2 tests:
\*   TP1: tx_state=PREPARING, TTL fires → prepared entry removed, ABORT reply sent
\*   TP2: coordinator processes ABORT reply via CoordAbortFromPrepare → tx ABORTED
\*   TP3: ShardHandleCommit guard (id \notin s_aborted) prevents silent install
ShardTimeoutPrepared(s, id) ==
    /\ id \in {pair[1] : pair \in s_prepared[s]}
    /\ tx_state[id] = "PREPARING"
    /\ s_aborted'       = [s_aborted       EXCEPT ![s] = @ \cup {id}]
    /\ write_buff'      = [write_buff      EXCEPT ![id] = {}]
    /\ write_key_owner' = [k \in Keys |->
           IF \E kv \in write_buff[id] : kv[1] = k THEN NONE ELSE write_key_owner[k]]
    /\ s_prepared'      = [s_prepared      EXCEPT ![s] = {p \in @ : p[1] # id}]
    /\ Send(PrepareReply(id, ABORT))
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                   s_clock, versions, dirty_keys, s_readers>>

\* Shard TTL-expires a stale read-only tracker (models expire_reads()).
\*
\* An abandoned read-only transaction (client crashed, etc.) whose start_ts
\* anchors the compaction watermark can prevent MVCC history from shrinking
\* indefinitely.  expire_reads() evicts the tracker after read_ttl to reclaim
\* the watermark anchor.
\*
\* The guard write_buff[id] = {} ensures we only expire pure readers: write-path
\* transactions are evicted by ShardTimeoutPrepared, not this action.
\*
\* After eviction the tx is added to s_aborted[s], blocking subsequent
\* ShardHandleRead calls (which guard on id \notin s_aborted[s]).  The client
\* receives ReadResult::Abort rather than re-registering with a stale snapshot
\* and potentially seeing NotFound for a key whose version was later compacted.
\*
\* Note: ShardPruneOrphanedPort cannot remove the s_aborted entry while the
\* coordinator still has the tx in state ACTIVE (its guard requires terminal
\* tx_state), so the abort signal is durable for the lifetime of the transaction.
\* The implementation uses a separate `read_aborted` set (not `aborted`) for the
\* same reason: prune_aborted() would immediately remove entries for read-only txs
\* since they have no write_buff or prepared anchor.
\*
\* Concrete execution traces driving Phase-2 tests:
\*   ER1: id ∈ s_readers[s], write_buff[id]={}, tx_state=ACTIVE →
\*        add to s_aborted, remove from s_readers
\*   ER2: After ER1, ShardCompactVersions can raise watermark (reader no longer active)
\*   ER3: After ER1 + compaction, ShardHandleRead(s,id,k) → blocked (id ∈ s_aborted[s]);
\*        client receives Abort rather than NotFound for a compacted version
\*   ER4: id ∈ write_buff → action disabled; write-path tx not evicted by expire_reads
ShardExpireRead(s, id) ==
    /\ id \in s_readers[s]
    /\ tx_state[id] = "ACTIVE"
    /\ write_buff[id] = {}
    /\ id \notin s_aborted[s]
    /\ s_aborted' = [s_aborted EXCEPT ![s] = @ \cup {id}]
    /\ s_readers' = [s_readers EXCEPT ![s] = @ \ {id}]
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, dirty_keys, msgs>>

\* Shard compacts MVCC versions for key k, discarding versions that are
\* permanently shadowed and can never be the result of LatestVersionBefore
\* for any active transaction's snapshot.
\*
\* Watermark = min start_t across all transactions that are active on this shard:
\*   activeOnShard(s) = {write-buffered txs} ∪ {txs that have read from this shard}
\*
\* Using only write-buffered transactions (the previous approximation) was unsafe:
\* a transaction T with start_t=5 that reads key K before its first write to this
\* shard has no write_buff entry here.  If a concurrent writer has start_t=100,
\* the old watermark would be 100 and compact away K@ts=3, which T still needs.
\* Tracking readers via s_readers fixes this: T's start_t=5 is included in the
\* minimum, so K@ts=3 is preserved until T commits or aborts.
\*
\* Implementation note:
\*   The Rust implementation adds a read_start_ts: HashMap<TxId, Timestamp> field
\*   to ShardState, populated on handle_read (keyed by tx_id, value = start_ts).
\*   compact_versions uses watermark = min(write_start_ts ∪ read_start_ts).
\*   Entries are cleared on handle_commit / handle_abort / handle_fast_commit /
\*   expire_prepared. A new expire_reads(now) method provides TTL-based cleanup
\*   for abandoned read-only transactions that never commit or abort.
\*
\* A version at timestamp t for key k is removable when:
\*   (1) t < watermark  (older than all active snapshots we track)
\*   (2) there exists a newer committed version at t' with t < t' < watermark
\*       (so no active snapshot would choose t over t')
\*
\* Equivalently: among versions with timestamp < watermark, only the one with
\* the highest timestamp (maxBelowTs) need be kept; all older ones are redundant.
\*
\* Safety: this action is purely a restriction of versions[] and does not affect
\* any coordinator or network state, so SI1-SI4 are preserved.
\*
\* Concrete execution traces driving Phase-2 tests:
\*   T1: T1.start_t=5 reads K (s_readers{T1}), T2.start_t=100 writes K (write_buff{T2});
\*       watermark = min(5,100) = 5; K@ts=3 NOT removed even though 3 < 100.
\*   T2: T1 reads then writes K2 (both read_start_ts and write_start_ts = 5); watermark=5.
\*   T3: Read-only T1 (start_t=5) finishes (tx_state→COMMITTED); s_readers still has T1
\*       but tx_state filter excludes it; watermark rises to next active tx or 0.
\*   T4: T1 aborts; ShardHandleAbort removes T1 from s_readers; watermark unaffected.
\*   T5: s_readers={} and write_buff={}; activeOnShard={}; watermark=0; nothing compacted.
\*   T6: Only committed or aborted txs in s_readers; filtered out; same as T5.
\*
\* Implementation note — dirty_keys optimization:
\*   Rather than scanning all keys on every compaction tick, the implementation
\*   maintains a dirty_keys set per shard.  A key is added to dirty_keys when a
\*   version is committed for it (handle_commit / handle_fast_commit). The
\*   compaction pass iterates only over dirty_keys and clears each key it visits,
\*   reducing the per-tick cost from O(N_keys) to O(committed_since_last_tick).
\*   In the spec this is modelled by gating ShardCompactVersions on k ∈ dirty_keys[s]
\*   and clearing k from dirty_keys after each compaction step.
ShardCompactVersions(s, k) ==
    LET \* Transactions active on this shard: write-buffered OR have read from it
        activeOnShard ==
            {id \in TxIds : write_buff[id] # {}}
            \cup {id \in s_readers[s] : tx_state[id] = "ACTIVE"}
        watermark ==
            IF activeOnShard = {}
            THEN 0  \* no active txs on this shard; nothing safe to compact
            ELSE MinOf({start_t[id] : id \in activeOnShard})
        vsBelowWatermark == {tv \in versions[k] : tv[2] < watermark}
        maxBelowTs ==
            IF vsBelowWatermark = {} THEN 0
            ELSE MaxOf({tv[2] : tv \in vsBelowWatermark})
        toRemove == {tv \in vsBelowWatermark : tv[2] < maxBelowTs}
    IN
    /\ ShardOf[k] = s
    /\ k \in dirty_keys[s]
    /\ versions' = [versions EXCEPT ![k] = @ \ toRemove]
    /\ dirty_keys' = [dirty_keys EXCEPT ![s] = @ \ {k}]
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                   s_clock, write_buff, write_key_owner, s_prepared, s_aborted, s_readers, msgs>>

\* Shard prunes a resolved transaction from its aborted set.
\* Safe once the coordinator has fully finalized the transaction:
\*   tx_state[id] = COMMITTED => coordinator sent COMMIT; no further writes for id possible.
\*   tx_state[id] = ABORTED   => coordinator sent ABORT;  no further writes for id possible.
\* In the implementation this is approximated by a coordinator-port seq-watermark:
\*   prune seq N from port P when min active seq from P is > N (or P has no active txs).
ShardPruneAborted(s, id) ==
    /\ tx_state[id] \in {"COMMITTED", "ABORTED"}
    /\ id \in s_aborted[s]
    /\ s_aborted' = [s_aborted EXCEPT ![s] = @ \ {id}]
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, dirty_keys, s_readers, msgs>>

\* Shard prunes ALL aborted entries from coordinator coord when coord has no
\* in-flight transactions (write_buff or prepared) on this shard.
\*
\* This models the shard background task that periodically evicts aborted entries
\* from dead or permanently-quiescent coordinator ports. Without this action,
\* aborted entries from a coordinator that crashes and never restarts accumulate
\* indefinitely: ShardPruneAborted only fires when new RPCs arrive from the same
\* coordinator, but a dead coordinator never sends new RPCs.
\*
\* Safety: if coord has no write_buff or prepared entries on s, it has no
\* in-flight writes that could race with a COMMIT.  Any future transaction from
\* coord will use a new tx_id (implementation: randomly seeded seq), so old
\* aborted entries are irrelevant.
\*
\* Implementation note: the background compaction task (compact_interval = 100 ms)
\* calls prune_aborted() on each tick in addition to compact_versions(). The Rust
\* prune_aborted() uses a per-port seq-watermark: if a coordinator port has no
\* in-flight tx (no write_buff or prepared entry), all its aborted entries are
\* pruned (the None => false arm). Running it from the background task means dead
\* coordinator entries are collected within one compaction interval even if no new
\* RPC from that port ever arrives.
\*
\* Concrete execution traces driving Phase-2 tests:
\*   T1: coord dead (no in-flight txs) → all its aborted entries pruned in one pass
\*   T2: mixed: dead coord entries pruned; active coord entries retained by seq watermark
\*   T3: coord transiently idle (no active txs) → entries pruned (safe: new txs have fresh ids)
\*   T4: coord still has prepared entry → entries NOT pruned
ShardPruneOrphanedPort(s, coord) ==
    LET coordTxIds == {id \in TxIds : CoordOf[id] = coord}
        noInFlight ==
            \A id \in coordTxIds :
                \* Shard-local: no write buffered, no prepared entry
                /\ write_buff[id] = {}
                /\ \A pair \in s_prepared[s] : pair[1] # id
                \* Coordinator-level: transaction has reached a terminal state.
                \* In the implementation tx_ids are not reused (randomly seeded seq),
                \* so this condition is implicit.  In the spec, TxIds are finite and
                \* may be reused, so we require terminal state to prevent pruning an
                \* aborted entry for a tx that is still in the PREPARING phase — which
                \* would re-enable ShardHandlePrepare and create an unbounded re-prepare
                \* loop driving s_clock past MaxTimestamp.
                /\ tx_state[id] \in {"IDLE", "COMMITTED", "ABORTED"}
        hasOrphans == \E id \in coordTxIds : id \in s_aborted[s]
    IN
    /\ noInFlight
    /\ hasOrphans
    /\ s_aborted' = [s_aborted EXCEPT ![s] =
           {id \in @ : CoordOf[id] # coord}]
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, read_participants, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, dirty_keys, s_readers, msgs>>

\* CoordSyncClockBatch(coord, S): coordinator issues GetClock concurrently to the
\* shards in S ⊆ Shards and advances c_clock to max(c_clock[coord], all s_clock[s]
\* for s ∈ S).  Shards absent from S did not respond within shard_rpc_timeout.
\*
\* Modelling S as a free existentially-quantified subset captures the concurrent
\* join_all(GetClock RPCs) wrapped in shard_rpc_timeout:
\*   S = {}     — all shards timed out; c_clock is unchanged (safe: monotone)
\*   S = Shards — all shards responded; c_clock advances to the global maximum
\*   S ⊂ Shards — partial response; c_clock advances past the responding subset
\*
\* The action is monotone: it can only increase c_clock, and it never touches any
\* transaction state, so SI1–SI4 remain satisfied in all executions that include it.
\*
\* This action models BOTH the one-time startup sync (coordinator joins a cluster
\* that has already processed transactions) AND periodic re-sync (a coordinator
\* that processes only reads and no commits never advances c_clock through its own
\* commit path; without periodic sync its c_clock drifts behind s_clock values
\* advanced by other coordinators, producing start_ts values that miss recent
\* writes from peer coordinators).  The Next relation admits this action at any
\* step, so TLC verifies safety across both patterns in a single model.
\*
\* Without this sync, a fresh coordinator starts with c_clock=0 and assigns
\* start_ts values near 0; shards that have already committed versions at higher
\* timestamps then reject every write with CommittedConflict.
\*
\* Advancing to s_clock[s]+1 (not s_clock[s]) ensures c_clock is strictly past the
\* last-used shard timestamp.  s_clock[s] represents the last timestamp USED at
\* shard s.  Setting c_clock = s_clock would allow commit_ts = max(c_clock, prep_ts)
\* to equal an already-used timestamp if another transaction committed at s_clock
\* between the prepare and finalize steps (confirmed by TLC: SI2 violated).
\*
\* Concrete execution traces driving Phase-2 tests:
\*   SC1: S = Shards → c_clock = max(c_clock, max(s_clock)+1) (normal startup)
\*   SC2: S = {} → c_clock unchanged (all shards timed out; coordinator safe to proceed)
\*   SC3: S ⊂ Shards → c_clock advances past responding shards only (partial timeout)
\*   SC4: CoordSyncClockBatch fires with c_clock already > s_clock[s]+1 → clock unchanged
\*        (monotone: periodic re-sync must never decrease c_clock even if shard is behind)
\*   SC5: CoordSyncClockBatch fires twice in sequence → second fire is idempotent when
\*        s_clock has not advanced (c_clock stays at prior synced value)
CoordSyncClockBatch(coord, S) ==
    LET newClock == IF S = {} THEN c_clock[coord]
                   ELSE MaxOf({s_clock[s] + 1 : s \in S} \cup {c_clock[coord]})
    IN
    /\ S \subseteq Shards
    \* Guard: the advanced clock value must still be within the model's timestamp
    \* bounds.  In the implementation c_clock is unbounded (u64); in the model
    \* Timestamps = 0..MaxTimestamp caps the state space.  Without this guard,
    \* CoordSyncClockBatch fires when s_clock[s] = MaxTimestamp, computing
    \* s_clock+1 = MaxTimestamp+1 outside Timestamps and violating TypeInvariant.
    /\ newClock \in Timestamps
    /\ c_clock' = [c_clock EXCEPT ![coord] = newClock]
    /\ UNCHANGED <<start_t, commit_t, is_committed, participants, read_participants, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers, msgs>>

\* CoordSyncFromInquireReply(coord, prepId, s): coordinator coord advances its
\* Lamport clock from the cc field carried in an INQUIRE_REPLY for prepId to
\* shard s.
\*
\* In the implementation, resolve_inquiry() proxies the inquiry to the owning
\* coordinator via gRPC.  The response now carries coordinator_clock, which the
\* caller uses to advance its own clock:
\*   c_clock = max(c_clock, reply.coordinator_clock)
\* This closes the SI gap where coordinator A (clock=100) could assign
\* start_ts=101 after learning that coordinator B committed at ts=5000.
\*
\* This action is only meaningful when coord /= CoordOf[prepId]: if coord owns
\* prepId, CoordHandleInquire already advanced the same clock.  The guard
\* new_cc > c_clock[coord] ensures the action is disabled once coord's clock is
\* already >= reply.cc, bounding the state space.
\*
\* Concrete execution traces (Phase 2):
\*   CI1: CommittedAt reply, cc=5 > c_clock[coord]=1 → c_clock[coord] advances to 5
\*   CI2: Active reply,      cc=3 > c_clock[coord]=1 → c_clock[coord] advances to 3
\*   CI3: reply.cc <= c_clock[coord] → action DISABLED (monotone, no advancement)
\*   CI4: two coordinators, t2 on c2 commits, t1 on c1 reads → c1 syncs from c2's cc
CoordSyncFromInquireReply(coord, prepId, s) ==
    LET reply  == CHOOSE msg \in msgs :
                      /\ msg.type = "INQUIRE_REPLY"
                      /\ msg.txid = prepId
                      /\ msg.to   = s
        new_cc == IF reply.cc > c_clock[coord] THEN reply.cc ELSE c_clock[coord]
    IN
    /\ \E msg \in msgs : msg.type = "INQUIRE_REPLY" /\ msg.txid = prepId /\ msg.to = s
    /\ coord /= CoordOf[prepId]        \* cross-coordinator case only
    /\ new_cc > c_clock[coord]         \* only fire when clock actually advances
    /\ new_cc \in Timestamps           \* model-bounds guard (same as CoordSyncClockBatch)
    /\ c_clock' = [c_clock EXCEPT ![coord] = new_cc]
    /\ UNCHANGED <<start_t, commit_t, is_committed, participants, read_participants, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, dirty_keys, s_readers, msgs>>

------------------------------------------------------------------------
(* Next-state relation *)

Next ==
    \/ \E id \in TxIds :
           \/ CoordStartTx(id)
           \/ CoordFastCommit(id)
           \/ CoordHandleFastCommitReply(id)
           \/ CoordBeginCommit(id)
           \/ CoordAbortFromPrepare(id)
           \/ CoordFinalizePrepare(id)
           \/ CoordSendCommit(id)
           \/ CoordAbortOnReply(id)
           \/ CoordAbortReadDeadline(id)
    \/ \E coord \in Coordinators, S \in SUBSET Shards :
           \/ CoordSyncClockBatch(coord, S)
    \/ \E coord \in Coordinators, s \in Shards :
           \/ ShardPruneOrphanedPort(s, coord)
    \/ \E id \in TxIds, k \in Keys :
           \/ CoordRead(id, k)
           \/ ShardHandlePrepare(ShardOf[k], id)
           \/ ShardHandleCommit(ShardOf[k], id)
           \/ ShardHandleAbort(ShardOf[k], id)
           \/ ShardHandleRead(ShardOf[k], id, k)
    \/ \E id \in TxIds, k \in Keys, v \in Values :
           \/ CoordUpdate(id, k, v)
           \/ ShardHandleUpdate(ShardOf[k], id, k, v)
           \/ CoordWriteAndFastCommit(id, k, v)
           \/ ShardHandleWriteAndFastCommit(ShardOf[k], id, k, v)
    \/ \E id \in TxIds, s \in Shards :
           \/ CoordHandleInquire(id, s)
           \/ ShardPruneAborted(s, id)
           \/ ShardHandleFastCommit(s, id)
           \/ ShardTimeoutPrepared(s, id)
           \/ ShardExpireRead(s, id)
    \/ \E s \in Shards, k \in Keys :
           ShardCompactVersions(s, k)
    \/ \E id \in TxIds, prepId \in TxIds, k \in Keys, s \in Shards :
           ShardSendInquire(s, id, prepId, k)
    \/ \E coord \in Coordinators, prepId \in TxIds, s \in Shards :
           CoordSyncFromInquireReply(coord, prepId, s)

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

------------------------------------------------------------------------
(* Safety properties *)

\* SI1: If T1 committed and commit_t(T1) <= start_t(T2), then after T1 commits
\*      the MVCC version installed by T1 is visible to T2's reads.
\*      We express this as: once T1's commit is installed at a shard, any
\*      LatestVersionBefore(k, start_t[T2]) for a key T1 wrote returns T1's value
\*      (or a later one), not an older value.
SI1 ==
    \A id1, id2 \in TxIds :
        (id1 # id2
         /\ is_committed[id1]
         /\ commit_t[id1] # NONE
         /\ start_t[id2] >= commit_t[id1])
        =>
        \A k \in Keys :
            \E kv \in write_buff[id1] : kv[1] = k
            =>
            \* T1's version exists in the MVCC history of k
            \E tv \in versions[k] : tv[2] = commit_t[id1]

\* SI2: Committed transactions have distinct commit timestamps.
SI2 ==
    \A id1, id2 \in TxIds :
        (id1 # id2 /\ is_committed[id1] /\ is_committed[id2])
        => commit_t[id1] # commit_t[id2]

\* SI3: MVCC version timestamps for a given key are distinct.
\*      (Structural invariant enabling well-defined LatestVersionBefore.)
SI3 ==
    \A k \in Keys :
        \A tv1, tv2 \in versions[k] :
            tv1[2] = tv2[2] => tv1[1] = tv2[1]

\* SI4: No two committed transactions concurrently wrote the same key.
\*      "Concurrent" means neither committed strictly before the other started.
\*      After both are committed, at most one can have been the writer.
\*      (The write_buff is cleared on commit, so we track via MVCC history.)
\*
\* tv1 # tv2 is required for correctness in the multi-coordinator case.
\* In a multi-coordinator deployment, two transactions can receive the same
\* commit_t when they write to disjoint shards (each shard's clock starts at
\* 0 independently and the coordinators never directly exchange clocks).
\* Without the tv1 # tv2 guard, if t1 commits k1@ts=2 and t2 commits k2@ts=2,
\* then for key k1: tv1 = <<v1,2>> (t1's version) and tv2 = <<v1,2>> (the same
\* tuple, because commit_t[t2]=2 also matches).  tv1 = tv2, so they are NOT
\* two distinct committed versions — t2 never wrote k1 — and SI4 must not fire.
\* The guard ensures we only flag the case where two genuinely distinct versions
\* coexist for the same key inside the concurrent windows, i.e., both txs actually
\* wrote that key. (SI3 separately catches the impossible case of two different
\* values at the same timestamp for the same key.)
SI4 ==
    \A id1, id2 \in TxIds :
        (id1 # id2 /\ is_committed[id1] /\ is_committed[id2])
        =>
        \A k \in Keys :
            \* At most one can have installed a DISTINCT version in their concurrent window
            ~( \E tv1 \in versions[k] : tv1[2] = commit_t[id1]
            /\ \E tv2 \in versions[k] : tv2[2] = commit_t[id2]
            /\ tv1 # tv2
            /\ commit_t[id1] > start_t[id2]
            /\ commit_t[id2] > start_t[id1])

HeltesDBInvariant == TypeInvariant /\ WriteKeyOwnerConsistent /\ SI2 /\ SI3 /\ SI4

------------------------------------------------------------------------

THEOREM Spec => []HeltesDBInvariant

=============================================================================
\* Model configuration (for TLC)
\* Suggested small instance:
\*   TxIds        <- {t1, t2}
\*   Keys         <- {k1}
\*   Values       <- {v1, v2}
\*   Coordinators <- {c1}
\*   Shards       <- {s1}
\*   CoordOf      <- (t1 :> c1 @@ t2 :> c1)
\*   ShardOf      <- (k1 :> s1)
\*   MaxTimestamp <- 6
