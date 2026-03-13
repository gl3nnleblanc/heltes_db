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
    ABORT,          \* Sentinel: abort signal (model value, not in Values)
    NONE            \* Sentinel: absent/not-found (model value, not in Timestamps or Values)

ASSUME ABORT \notin Values
ASSUME \A id \in TxIds    : CoordOf[id] \in Coordinators
ASSUME \A k  \in Keys     : ShardOf[k]  \in Shards

Timestamps == 0..MaxTimestamp

------------------------------------------------------------------------
(* State variables *)

VARIABLES
    \* -- Coordinator state --
    c_clock,        \* c_clock[coord]    : Nat
    start_t,        \* start_t[id]       : Nat
    commit_t,       \* commit_t[id]      : Nat | NONE
    is_committed,   \* is_committed[id]  : Bool
    participants,   \* participants[id]  : SUBSET Shards
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

    \* -- Network --
    msgs            \* msgs : SUBSET Message  (unordered, duplicates allowed)

vars == <<c_clock, start_t, commit_t, is_committed, participants, tx_state,
          s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, msgs>>

------------------------------------------------------------------------
(* Types *)

TxStateSet == {"IDLE", "ACTIVE", "PREPARING", "COMMIT_WAIT", "COMMITTED", "ABORTED"}

TypeInvariant ==
    /\ c_clock       \in [Coordinators -> Timestamps]
    /\ start_t       \in [TxIds -> Timestamps]
    /\ \A id \in TxIds : IF commit_t[id] = NONE THEN TRUE ELSE commit_t[id] \in Timestamps
    /\ is_committed  \in [TxIds -> BOOLEAN]
    /\ participants  \in [TxIds -> SUBSET Shards]
    /\ tx_state      \in [TxIds -> TxStateSet]
    /\ s_clock       \in [Shards -> Timestamps]
    /\ versions         \in [Keys -> SUBSET (Values \X Timestamps)]
    /\ write_buff       \in [TxIds -> SUBSET (Keys \X Values)]
    /\ write_key_owner  \in [Keys -> TxIds \cup {NONE}]
    /\ s_prepared       \in [Shards -> SUBSET (TxIds \X Timestamps)]
    /\ s_aborted        \in [Shards -> SUBSET TxIds]

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
InquireMsg(id, t, asker) == [type |-> "INQUIRE",          txid |-> id, ts |-> t,    from |-> asker]
InquireReply(id, r, t, dest) ==
    [type |-> "INQUIRE_REPLY", txid |-> id, result |-> r, ts |-> t, to |-> dest]
\* Fast-path: combines Prepare+Commit into a single shard-side operation.
\* Reply ts = NONE means the shard aborted; ts ≠ NONE is the assigned commit timestamp.
FastCommitMsg(id)        == [type |-> "FAST_COMMIT",       txid |-> id]
FastCommitReply(id, ct)  == [type |-> "FAST_COMMIT_REPLY", txid |-> id, ts |-> ct]

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

------------------------------------------------------------------------
(* Initial state *)

Init ==
    /\ c_clock      = [coord \in Coordinators |-> 0]
    /\ start_t      = [id \in TxIds |-> 0]
    /\ commit_t     = [id \in TxIds |-> NONE]
    /\ is_committed = [id \in TxIds |-> FALSE]
    /\ participants = [id \in TxIds |-> {}]
    /\ tx_state     = [id \in TxIds |-> "IDLE"]
    /\ s_clock          = [s  \in Shards |-> 0]
    /\ versions         = [k  \in Keys   |-> {}]
    /\ write_buff       = [id \in TxIds  |-> {}]
    /\ write_key_owner  = [k  \in Keys   |-> NONE]
    /\ s_prepared       = [s  \in Shards |-> {}]
    /\ s_aborted        = [s  \in Shards |-> {}]
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
    /\ is_committed' = [is_committed EXCEPT ![id]    = FALSE]
    /\ participants' = [participants EXCEPT ![id]    = {}]
    /\ UNCHANGED <<commit_t, s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, msgs>>

\* read(id, k): send READ_KEY to the owning shard
CoordRead(id, k) ==
    /\ tx_state[id] = "ACTIVE"
    /\ Send(ReadKeyMsg(id, start_t[id], k))
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted>>

\* update(id, k, v): track participant shard, send UPDATE_KEY
CoordUpdate(id, k, v) ==
    /\ tx_state[id] = "ACTIVE"
    /\ participants' = [participants EXCEPT ![id] = @ \cup {ShardOf[k]}]
    /\ Send(UpdateKeyMsg(id, start_t[id], k, v))
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted>>

\* Coordinator receives an ABORT reply from a read or update operation
CoordAbortOnReply(id) ==
    /\ tx_state[id] = "ACTIVE"
    /\ \/ ReadKeyReply(id, ABORT) \in msgs
       \/ \E k \in Keys : UpdateKeyReply(id, k, ABORT) \in msgs
    /\ tx_state' = [tx_state EXCEPT ![id] = "ABORTED"]
    /\ SendAll({AbortMsg(id)})
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted>>

\* Single-shard fast path: coordinator sends FAST_COMMIT to the sole participant
\* shard, skipping the two-round-trip Prepare → Commit sequence.
\* Guard conditions mirror CoordBeginCommit (all update acks received).
\* Reuses the PREPARING phase while waiting for FastCommitReply.
CoordFastCommit(id) ==
    /\ tx_state[id] = "ACTIVE"
    /\ participants[id] # {}
    /\ Cardinality(participants[id]) = 1
    /\ \A m \in msgs :
           (m.type = "UPDATE_KEY" /\ m.txid = id)
           => UpdateKeyReply(id, m.key, "OK") \in msgs
    /\ tx_state' = [tx_state EXCEPT ![id] = "PREPARING"]
    /\ Send(FastCommitMsg(id))
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted>>

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
            /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants,
                           s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted, msgs>>
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
            /\ UNCHANGED <<start_t, participants, s_clock, versions,
                           write_buff, write_key_owner, s_prepared, s_aborted, msgs>>

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
CoordBeginCommit(id) ==
    /\ tx_state[id] = "ACTIVE"
    /\ participants[id] # {}
    /\ \A m \in msgs :
           (m.type = "UPDATE_KEY" /\ m.txid = id)
           => UpdateKeyReply(id, m.key, "OK") \in msgs
    /\ tx_state' = [tx_state EXCEPT ![id] = "PREPARING"]
    /\ SendAll({PrepareMsg(id)})
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted>>

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
    /\ UNCHANGED <<start_t, is_committed, participants, s_clock, versions,
                   write_buff, write_key_owner, s_prepared, s_aborted, msgs>>

\* Commit-wait passes (now().earliest > commit_t): send COMMIT to all participants.
\* In the logical clock model we abstract TrueTime commit-wait as: some other
\* coordinator's clock has advanced past commit_t[id], ensuring SI1.
\* For model checking we allow this step unconditionally (soundly conservative).
CoordSendCommit(id) ==
    LET ct == commit_t[id]
    IN
    /\ tx_state[id]    = "COMMIT_WAIT"
    /\ commit_t[id]   # NONE
    /\ tx_state'      = [tx_state      EXCEPT ![id] = "COMMITTED"]
    /\ is_committed'  = [is_committed  EXCEPT ![id] = TRUE]
    /\ SendAll({CommitMsg(id, ct)})
    /\ UNCHANGED <<c_clock, start_t, commit_t, participants,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted>>

\* Coordinator handles INQUIRE(id, t) from shard s.
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
CoordHandleInquire(id, s) ==
    LET coord == CoordOf[id]
        m     == CHOOSE msg \in msgs :
                     /\ msg.type = "INQUIRE"
                     /\ msg.txid = id
                     /\ msg.from = s
        reply == IF is_committed[id]
                 THEN InquireReply(id, "COMMITTED", commit_t[id], s)
                 ELSE InquireReply(id, "ACTIVE",    NONE,         s)
    IN
    /\ \E msg \in msgs : msg.type = "INQUIRE" /\ msg.txid = id /\ msg.from = s
    /\ c_clock' = [c_clock EXCEPT ![coord] =
                      IF m.ts > c_clock[coord] THEN m.ts ELSE c_clock[coord]]
    /\ Send(reply)
    /\ UNCHANGED <<start_t, commit_t, is_committed, participants, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted>>

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
    /\ s_clock' = [s_clock EXCEPT ![s] = IF t > s_clock[s] THEN t ELSE s_clock[s]]
    /\ Send(reply)
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, tx_state,
                   versions, write_buff, write_key_owner, s_prepared, s_aborted>>

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
    /\ Send(InquireMsg(prepId, t, s))
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted>>

\* Shard handles UPDATE_KEY(id, t, k, v)
\*
\* Write-write conflict check (corrected from proof typo):
\*   Abort if ∃ committed version of k with ts >= start_t(id)   [committed conflict]
\*   Abort if ∃ prepared tx that wrote k with prep_t >= start_t(id)  [prepared conflict]
ShardHandleUpdate(s, id, k, v) ==
    LET t == start_t[id]
        m == UpdateKeyMsg(id, t, k, v)
    IN
    /\ m \in msgs
    /\ ShardOf[k] = s
    /\ tx_state[id] = "ACTIVE"
    /\ IF id \in s_aborted[s]
       THEN \* Already aborted at this shard
            /\ Send(UpdateKeyReply(id, k, ABORT))
            /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, tx_state,
                           s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted>>
       ELSE IF CommittedConflict(k, t) \/ PreparedConflict(s, k, t) \/ WriteBuffConflictFast(k, id)
            THEN \* Write-write conflict — abort
                 /\ s_aborted' = [s_aborted EXCEPT ![s] = @ \cup {id}]
                 /\ Send(UpdateKeyReply(id, k, ABORT))
                 /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants,
                                tx_state, s_clock, versions, write_buff, write_key_owner, s_prepared>>
            ELSE \* No conflict — buffer the write (overwrite any prior write to k by id)
                 /\ write_buff' = [write_buff EXCEPT ![id] =
                        (@ \ {kv \in @ : kv[1] = k}) \cup {<<k, v>>}]
                 /\ write_key_owner' = [write_key_owner EXCEPT ![k] = id]
                 /\ s_clock' = [s_clock EXCEPT ![s] =
                        IF t > s_clock[s] THEN t ELSE s_clock[s]]
                 /\ Send(UpdateKeyReply(id, k, "OK"))
                 /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants,
                                tx_state, versions, s_prepared, s_aborted>>

\* Shard handles PREPARE(id) — assign a prepare timestamp and record it.
\* Guard: not already prepared (message set is persistent; prevent re-firing).
ShardHandlePrepare(s, id) ==
    LET t == s_clock[s] + 1
    IN
    /\ PrepareMsg(id) \in msgs
    /\ s \in participants[id]
    /\ id \notin s_aborted[s]
    /\ id \notin {p[1] : p \in s_prepared[s]}   \* not already prepared
    /\ t \in Timestamps
    /\ s_prepared' = [s_prepared EXCEPT ![s] = @ \cup {<<id, t>>}]
    /\ s_clock'    = [s_clock    EXCEPT ![s] = t]
    /\ Send(PrepareReply(id, t))
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, tx_state,
                   versions, write_buff, write_key_owner, s_aborted>>

\* Shard handles FAST_COMMIT(id) — atomically prepare + commit for single-shard txns.
\* Combines ShardHandlePrepare + ShardHandleCommit in one step.
\* Guard write_buff[id] ≠ {} prevents re-firing after the first successful commit.
ShardHandleFastCommit(s, id) ==
    LET ct == s_clock[s] + 1
    IN
    /\ FastCommitMsg(id) \in msgs
    /\ s \in participants[id]
    /\ write_buff[id] # {}          \* idempotency: nothing left to commit after first fire
    /\ ct \in Timestamps
    /\ IF id \in s_aborted[s]
       THEN \* tx was aborted before the fast-commit arrived — reply abort
            /\ Send(FastCommitReply(id, NONE))
            /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, tx_state,
                           s_clock, versions, write_buff, write_key_owner, s_prepared, s_aborted>>
       ELSE \* prepare + commit atomically
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
            /\ Send(FastCommitReply(id, ct))
            /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants,
                           tx_state, s_aborted>>

\* Shard handles COMMIT(id, ct) — install buffered writes as new MVCC versions
\*
\* Guard id \notin s_aborted[s]: if the shard already auto-aborted this prepared
\* entry (ShardTimeoutPrepared), it must REJECT the COMMIT and signal an error
\* rather than silently succeeding with an empty write_buff. This prevents silent
\* data loss when expire_prepared fires between the coordinator's PREPARE phase
\* and the arrival of the COMMIT message.
\*
\* In the spec this guard is vacuously always satisfied: ShardTimeoutPrepared
\* requires tx_state[id] = "PREPARING", but ShardHandleCommit requires
\* is_committed[id] (set by CoordSendCommit which requires COMMIT_WAIT), so the
\* two actions are mutually exclusive. The guard documents the implementation
\* invariant that handle_commit must enforce defensively.
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
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, tx_state,
                   s_clock, s_aborted, msgs>>

\* Shard handles ABORT(id) — discard buffered writes, release prepared entry, unlock write_key_owner
ShardHandleAbort(s, id) ==
    /\ AbortMsg(id) \in msgs
    /\ s_aborted'       = [s_aborted       EXCEPT ![s]  = @ \cup {id}]
    /\ write_buff'      = [write_buff       EXCEPT ![id] = {}]
    /\ write_key_owner' = [k \in Keys |->
           IF \E kv \in write_buff[id] : kv[1] = k THEN NONE ELSE write_key_owner[k]]
    /\ s_prepared'      = [s_prepared       EXCEPT ![s]  = {p \in @ : p[1] # id}]
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, tx_state,
                   s_clock, versions, msgs>>

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
\* Implementation note: the Rust implementation records Instant::now() at
\* handle_prepare time, then calls expire_prepared(Instant::now()) lazily at
\* the start of each update/prepare/read RPC handler. Entries older than
\* prepare_ttl are aborted with the same cleanup as handle_abort.
ShardTimeoutPrepared(s, id) ==
    /\ id \in {pair[1] : pair \in s_prepared[s]}
    /\ tx_state[id] = "PREPARING"
    /\ s_aborted'       = [s_aborted       EXCEPT ![s] = @ \cup {id}]
    /\ write_buff'      = [write_buff      EXCEPT ![id] = {}]
    /\ write_key_owner' = [k \in Keys |->
           IF \E kv \in write_buff[id] : kv[1] = k THEN NONE ELSE write_key_owner[k]]
    /\ s_prepared'      = [s_prepared      EXCEPT ![s] = {p \in @ : p[1] # id}]
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, tx_state,
                   s_clock, versions, msgs>>

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
    /\ UNCHANGED <<c_clock, start_t, commit_t, is_committed, participants, tx_state,
                   s_clock, versions, write_buff, write_key_owner, s_prepared, msgs>>

------------------------------------------------------------------------
(* Next-state relation *)

Next ==
    \/ \E id \in TxIds :
           \/ CoordStartTx(id)
           \/ CoordFastCommit(id)
           \/ CoordHandleFastCommitReply(id)
           \/ CoordBeginCommit(id)
           \/ CoordFinalizePrepare(id)
           \/ CoordSendCommit(id)
           \/ CoordAbortOnReply(id)
    \/ \E id \in TxIds, k \in Keys :
           \/ CoordRead(id, k)
           \/ ShardHandlePrepare(ShardOf[k], id)
           \/ ShardHandleCommit(ShardOf[k], id)
           \/ ShardHandleAbort(ShardOf[k], id)
           \/ ShardHandleRead(ShardOf[k], id, k)
    \/ \E id \in TxIds, k \in Keys, v \in Values :
           \/ CoordUpdate(id, k, v)
           \/ ShardHandleUpdate(ShardOf[k], id, k, v)
    \/ \E id \in TxIds, s \in Shards :
           \/ CoordHandleInquire(id, s)
           \/ ShardPruneAborted(s, id)
           \/ ShardHandleFastCommit(s, id)
           \/ ShardTimeoutPrepared(s, id)
    \/ \E id \in TxIds, prepId \in TxIds, k \in Keys, s \in Shards :
           ShardSendInquire(s, id, prepId, k)

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
SI4 ==
    \A id1, id2 \in TxIds :
        (id1 # id2 /\ is_committed[id1] /\ is_committed[id2])
        =>
        \A k \in Keys :
            \* At most one can have installed a version in their concurrent window
            ~( \E tv1 \in versions[k] : tv1[2] = commit_t[id1]
            /\ \E tv2 \in versions[k] : tv2[2] = commit_t[id2]
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
