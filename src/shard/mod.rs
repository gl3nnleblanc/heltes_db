use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

pub type TxId = u64;
pub type Key = u64;
pub type Timestamp = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Value(pub u64);

/// A single MVCC version of a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    pub value: Value,
    pub timestamp: Timestamp,
}

// ---------------------------------------------------------------------------
// Message result types
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
pub enum ReadResult {
    /// Returned the value visible at start_ts.
    Value(Value),
    /// No committed version of this key exists before start_ts (key not yet inserted).
    NotFound,
    /// Transaction was already aborted.
    Abort,
    /// One or more prepared writers of this key have prep_t < start_ts and
    /// their status is unknown. The caller must resolve these via INQUIRE and
    /// call handle_read again with the results populated.
    NeedsInquiry(Vec<TxId>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum UpdateResult {
    Ok,
    /// Write-write conflict detected, or transaction already aborted.
    Abort,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PrepareResult {
    /// Prepare timestamp assigned by this shard.
    Timestamp(Timestamp),
    /// Transaction was already aborted.
    Abort,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FastCommitResult {
    /// All writes installed; returned commit_ts = shard_clock + 1 at call time.
    Ok(Timestamp),
    /// Transaction was already aborted.
    Abort,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CommitResult {
    /// Writes installed (or nothing to install — idempotent re-commit).
    Ok,
    /// Transaction was already aborted at this shard before COMMIT arrived
    /// (e.g. via TTL expiry). No data was installed; the coordinator must
    /// signal failure to the client rather than silently reporting success.
    Abort,
}

/// The coordinator's answer to an INQUIRE for a prepared transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InquiryStatus {
    /// Transaction committed at this timestamp.
    Committed(Timestamp),
    /// Transaction is still active (not yet committed).
    Active,
}

// ---------------------------------------------------------------------------
// Shard state
// ---------------------------------------------------------------------------

pub struct ShardState {
    /// Logical clock (s_clock in TLA+).
    pub clock: Timestamp,
    /// MVCC version history per key. Each vec is sorted ascending by timestamp.
    /// Keys are absent from the map until their first write is committed (insert semantics).
    pub versions: HashMap<Key, Vec<Version>>,
    /// Buffered (uncommitted) writes per transaction.
    pub write_buff: HashMap<TxId, HashMap<Key, Value>>,
    /// O(1) write-lock index: maps each key to the transaction that currently holds
    /// a buffered write for it (write_key_owner in TLA+).
    /// Invariant: write_keys[k] = tx_id  iff  write_buff[tx_id][k] exists.
    pub write_keys: HashMap<Key, TxId>,
    /// Prepared transactions and their prepare timestamps.
    pub prepared: HashMap<TxId, Timestamp>,
    /// Transactions that have been aborted at this shard.
    pub aborted: HashSet<TxId>,
    /// Wall-clock instant at which each transaction was prepared on this shard.
    /// Cleared on commit, abort, or TTL-based auto-abort.
    prepare_times: HashMap<TxId, Instant>,
    /// Maximum time a prepared entry may sit unresolved before being auto-aborted
    /// by `expire_prepared`. Protects conflicting writers from being permanently
    /// blocked by a coordinator that crashed mid-2PC (ShardTimeoutPrepared in TLA+).
    pub prepare_ttl: Duration,
    /// TxIds that were auto-aborted by `expire_prepared` and have not yet received
    /// a COMMIT or ABORT from the coordinator. Checked by `handle_commit` to detect
    /// the TTL-expiry + late-COMMIT race: entries in `aborted` may be pruned by the
    /// watermark before COMMIT arrives, so we need a separate durable signal.
    /// Cleared by `handle_commit` (returns Abort) and `handle_abort`.
    ttl_expired: HashSet<TxId>,
    /// Keys that have had a version committed since the last compaction tick.
    /// `compact_versions` iterates only this set instead of all keys, reducing
    /// the per-tick cost from O(N_keys) to O(keys committed since last tick).
    /// Keys are added by `handle_commit` and `handle_fast_commit`, and removed
    /// (cleared) by `compact_versions` after processing each key.
    /// (dirty_keys in TLA+)
    pub dirty_keys: HashSet<Key>,
    /// Records the `start_ts` supplied in the first `handle_update` call for each
    /// transaction that currently has a write buffer on this shard.  Cleared on
    /// commit, abort, or TTL expiry.
    ///
    /// Used by `compact_versions` to compute the compaction watermark.
    /// (One of two inputs — see also `read_start_ts`.)
    write_start_ts: HashMap<TxId, Timestamp>,

    /// Commit timestamps for transactions that completed via `handle_fast_commit`.
    ///
    /// Enables idempotent retries: if a coordinator crash+replay causes a second
    /// `FAST_COMMIT` RPC for an already-committed transaction, `handle_fast_commit`
    /// returns the stored commit_ts without advancing the shard clock.
    ///
    /// Without this map the original code advanced `s_clock` unconditionally before
    /// checking `write_buff`, burning a second timestamp and returning a different
    /// (wrong) commit_ts on every retry.
    ///
    /// Entries are pruned by `prune_aborted` using the same per-port seq-watermark
    /// as `aborted`, so the map does not grow without bound.
    pub fast_committed: HashMap<TxId, Timestamp>,

    /// Records the `start_ts` of every transaction that has issued a read to
    /// this shard and has not yet committed, aborted, or been TTL-expired.
    /// Populated by `handle_read`. Cleared by `handle_commit`, `handle_abort`,
    /// `handle_fast_commit`, and `expire_prepared` (write-side abort).
    ///
    /// Used by `compact_versions` together with `write_start_ts`:
    ///   watermark = min(write_start_ts.values() ∪ read_start_ts.values())
    ///
    /// Without this tracking, a transaction T with start_ts=5 that reads before
    /// its first write on this shard has no `write_start_ts` entry. A concurrent
    /// writer with start_ts=100 would set watermark=100, potentially compacting
    /// away MVCC versions that T's snapshot still needs (ShardCompactVersions fix
    /// in TLA+ spec: activeOnShard includes s_readers, not just write_buff).
    ///
    /// Read-only transactions (never in `write_buff`) stay in this map until
    /// `expire_reads` evicts them via TTL — see `read_ttl`.
    read_start_ts: HashMap<TxId, Timestamp>,

    /// Wall-clock instant at which each transaction's first read to this shard
    /// was recorded. Used by `expire_reads` to evict abandoned read-only
    /// transactions that never commit or abort (client crashed, etc.).
    read_times: HashMap<TxId, Instant>,

    /// Maximum time a read-only tracking entry may sit in `read_start_ts` before
    /// `expire_reads` discards it. Defaults to the same value as `prepare_ttl`.
    /// Should be set to at least the maximum expected client transaction duration.
    pub read_ttl: Duration,
}

impl Default for ShardState {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardState {
    /// Create a new shard with no committed versions.
    /// Keys come into existence on first commit (matching TLA+ `versions = [k \in Keys |-> {}]`).
    pub fn new() -> Self {
        ShardState {
            clock: 0,
            versions: HashMap::new(),
            write_buff: HashMap::new(),
            write_keys: HashMap::new(),
            prepared: HashMap::new(),
            aborted: HashSet::new(),
            prepare_times: HashMap::new(),
            prepare_ttl: Duration::from_secs(30),
            ttl_expired: HashSet::new(),
            write_start_ts: HashMap::new(),
            read_start_ts: HashMap::new(),
            read_times: HashMap::new(),
            read_ttl: Duration::from_secs(30),
            dirty_keys: HashSet::new(),
            fast_committed: HashMap::new(),
        }
    }

    /// Handle READ_KEY(id, start_ts, key).
    ///
    /// `inquiry_results` supplies coordinator responses for any prepared
    /// writers that have already been inquired about. If a prepared writer of
    /// `key` with prep_t < start_ts is absent from this map, the shard cannot
    /// yet determine visibility and returns `NeedsInquiry`.
    ///
    /// If a prepared writer is `Committed(ct)` with ct < start_ts, its version
    /// must already be installed in `self.versions` (i.e., handle_commit must
    /// have been called) before the read can proceed; otherwise `NeedsInquiry`
    /// is returned for that writer too.
    pub fn handle_read(
        &mut self,
        tx_id: TxId,
        start_ts: Timestamp,
        key: Key,
        inquiry_results: &HashMap<TxId, InquiryStatus>,
    ) -> ReadResult {
        if self.aborted.contains(&tx_id) {
            return ReadResult::Abort;
        }

        // Own buffered write — return it directly (read-your-writes).
        if let Some(writes) = self.write_buff.get(&tx_id) {
            if let Some(val) = writes.get(&key) {
                self.clock = self.clock.max(start_ts);
                return ReadResult::Value(val.clone());
            }
        }

        // Find prepared writers of this key whose prep_t < start_ts.
        // These are potentially visible and need coordinator inquiry.
        let mut needs_inquiry: Vec<TxId> = Vec::new();
        for (&writer, &prep_t) in &self.prepared {
            if writer == tx_id {
                continue;
            }
            let wrote_key = self
                .write_buff
                .get(&writer)
                .map(|wb| wb.contains_key(&key))
                .unwrap_or(false);
            if wrote_key && prep_t < start_ts {
                // Check if inquiry result is already provided.
                match inquiry_results.get(&writer) {
                    Some(InquiryStatus::Committed(ct)) => {
                        // Committed before our snapshot — version must be installed.
                        if *ct < start_ts {
                            let installed = self
                                .versions
                                .get(&key)
                                .map(|vs| vs.iter().any(|v| v.timestamp == *ct))
                                .unwrap_or(false);
                            if !installed {
                                needs_inquiry.push(writer);
                            }
                        }
                        // If ct >= start_ts it committed after our snapshot; not visible — skip.
                    }
                    Some(InquiryStatus::Active) => {
                        // Still in-flight; not visible to our snapshot — skip.
                    }
                    None => {
                        needs_inquiry.push(writer);
                    }
                }
            }
        }

        if !needs_inquiry.is_empty() {
            needs_inquiry.sort();
            return ReadResult::NeedsInquiry(needs_inquiry);
        }

        // Return the latest version with timestamp strictly before start_ts.
        // versions[key] is sorted ascending by timestamp, so partition_point
        // gives the first index with timestamp >= start_ts in O(log N).
        // The entry just before that index (if any) is the answer.
        let best = self
            .versions
            .get(&key)
            .and_then(|vs| {
                let pos = vs.partition_point(|v| v.timestamp < start_ts);
                if pos > 0 {
                    Some(&vs[pos - 1])
                } else {
                    None
                }
            })
            .cloned();

        self.clock = self.clock.max(start_ts);
        // Track this transaction's start_ts so compact_versions includes it in the
        // watermark. Without this, a reader whose start_ts is lower than all active
        // writers' start_ts values would be invisible to the watermark, allowing
        // compaction to remove MVCC versions that this reader's snapshot still needs.
        self.read_start_ts.entry(tx_id).or_insert(start_ts);
        self.read_times.entry(tx_id).or_insert_with(Instant::now);
        match best {
            Some(ver) => ReadResult::Value(ver.value),
            None => ReadResult::NotFound,
        }
    }

    /// Handle UPDATE_KEY(id, start_ts, key, value).
    ///
    /// Detects write-write conflicts:
    ///
    ///   - Abort if any committed version of `key` has timestamp >= start_ts.
    ///   - Abort if any prepared transaction that wrote `key` has prep_t >= start_ts.
    ///
    /// Otherwise, buffer the write (overwriting any prior write to `key` by this tx).
    pub fn handle_update(
        &mut self,
        tx_id: TxId,
        start_ts: Timestamp,
        key: Key,
        value: Value,
    ) -> UpdateResult {
        if self.aborted.contains(&tx_id) {
            return UpdateResult::Abort;
        }

        // CommittedConflict: any installed version with timestamp >= start_ts.
        // versions[key] is sorted ascending; the first entry at or after the
        // partition point has timestamp >= start_ts — O(log N) check.
        let committed_conflict = self
            .versions
            .get(&key)
            .map(|vs| vs.partition_point(|v| v.timestamp < start_ts) < vs.len())
            .unwrap_or(false);
        if committed_conflict {
            self.aborted.insert(tx_id);
            return UpdateResult::Abort;
        }

        // PreparedConflict: any other prepared tx that wrote this key has prep_t >= start_ts.
        let prepared_conflict = self.prepared.iter().any(|(&other, &prep_t)| {
            other != tx_id
                && prep_t >= start_ts
                && self
                    .write_buff
                    .get(&other)
                    .map(|wb| wb.contains_key(&key))
                    .unwrap_or(false)
        });
        if prepared_conflict {
            self.aborted.insert(tx_id);
            return UpdateResult::Abort;
        }

        // WriteBuffConflict: O(1) index lookup — any other tx holds the write lock for key.
        let write_buff_conflict = self
            .write_keys
            .get(&key)
            .map(|&owner| owner != tx_id)
            .unwrap_or(false);
        if write_buff_conflict {
            self.aborted.insert(tx_id);
            return UpdateResult::Abort;
        }

        // Buffer the write and advance clock.
        self.write_buff.entry(tx_id).or_default().insert(key, value);
        // Acquire the write lock for this key in the O(1) index.
        self.write_keys.insert(key, tx_id);
        self.clock = self.clock.max(start_ts);
        // Record start_ts for compaction watermark (only on first write for this tx).
        self.write_start_ts.entry(tx_id).or_insert(start_ts);
        UpdateResult::Ok
    }

    /// Handle PREPARE(id).
    ///
    /// Assigns a prepare timestamp = clock + 1, advances clock, records
    /// (tx_id, prep_t) in `self.prepared`.
    pub fn handle_prepare(&mut self, tx_id: TxId) -> PrepareResult {
        if self.aborted.contains(&tx_id) {
            return PrepareResult::Abort;
        }
        // Idempotent: if already prepared, return existing timestamp.
        if let Some(&prep_t) = self.prepared.get(&tx_id) {
            return PrepareResult::Timestamp(prep_t);
        }
        let prep_t = self.clock + 1;
        self.clock = prep_t;
        self.prepared.insert(tx_id, prep_t);
        self.prepare_times.insert(tx_id, Instant::now());
        PrepareResult::Timestamp(prep_t)
    }

    /// Handle COMMIT(id, commit_ts).
    ///
    /// Returns `CommitResult::Abort` if `tx_id` is in `self.aborted` (e.g. due
    /// to TTL expiry via `expire_prepared`). In that case no data is installed
    /// and the caller must surface a failure to the client — not silently succeed.
    ///
    /// Otherwise installs all buffered writes for `tx_id` as new MVCC versions at
    /// `commit_ts`, clears the write buffer, and removes the tx from `prepared`.
    pub fn handle_commit(&mut self, tx_id: TxId, commit_ts: Timestamp) -> CommitResult {
        // Check TTL-expiry first: `aborted` may have been pruned by the watermark
        // before this COMMIT arrived, but `ttl_expired` persists until this handler.
        if self.ttl_expired.remove(&tx_id) {
            self.aborted.remove(&tx_id);
            return CommitResult::Abort;
        }
        if self.aborted.contains(&tx_id) {
            return CommitResult::Abort;
        }
        if let Some(writes) = self.write_buff.remove(&tx_id) {
            for (key, value) in writes {
                // Release write lock.
                self.write_keys.remove(&key);
                let vs = self.versions.entry(key).or_default();
                // Idempotent + sorted insertion: binary search for commit_ts.
                // Ok(i) → already installed at index i, skip.
                // Err(i) → not present; insert at index i to maintain ascending order.
                if let Err(pos) = vs.binary_search_by_key(&commit_ts, |v| v.timestamp) {
                    vs.insert(
                        pos,
                        Version {
                            value,
                            timestamp: commit_ts,
                        },
                    );
                }
                self.dirty_keys.insert(key);
            }
        }
        self.prepared.remove(&tx_id);
        self.prepare_times.remove(&tx_id);
        self.write_start_ts.remove(&tx_id);
        self.read_start_ts.remove(&tx_id);
        self.read_times.remove(&tx_id);
        self.prune_aborted();
        CommitResult::Ok
    }

    /// Handle ABORT(id).
    ///
    /// Marks `tx_id` as aborted, clears its write buffer, and removes it from
    /// `prepared`.
    pub fn handle_abort(&mut self, tx_id: TxId) {
        self.ttl_expired.remove(&tx_id);
        self.aborted.insert(tx_id);
        if let Some(writes) = self.write_buff.remove(&tx_id) {
            for key in writes.into_keys() {
                self.write_keys.remove(&key);
            }
        }
        self.prepared.remove(&tx_id);
        self.prepare_times.remove(&tx_id);
        self.write_start_ts.remove(&tx_id);
        self.read_start_ts.remove(&tx_id);
        self.read_times.remove(&tx_id);
        self.prune_aborted();
    }

    /// Handle FAST_COMMIT(id) — single-shard optimisation.
    ///
    /// Atomically prepares and commits the transaction: assigns
    /// `commit_ts = clock + 1`, installs all buffered writes as MVCC versions,
    /// clears the write buffer, advances the shard clock, and calls
    /// `prune_aborted`. This replaces the separate PREPARE + COMMIT round trip.
    ///
    /// Returns:
    /// - `Abort` if the transaction is already aborted at this shard.
    /// - `Ok(commit_ts)` on success, storing the commit_ts in `fast_committed`.
    /// - `Ok(commit_ts)` (same stored value) on an idempotent retry where
    ///   `write_buff` is empty but `fast_committed` has an entry — the clock is
    ///   NOT advanced a second time.
    /// - `Abort` if `write_buff` is absent and no `fast_committed` entry exists
    ///   (the transaction never buffered any writes on this shard).
    ///
    /// The bug this fixes: the previous code computed `commit_ts = clock + 1` and
    /// advanced `clock` unconditionally before checking `write_buff`. On a retry
    /// (coordinator crash+replay), that burned a second timestamp and returned a
    /// different (wrong) `commit_ts`, breaking idempotency.
    ///
    /// Matches `ShardHandleFastCommit` in the TLA+ spec: the spec's
    /// `write_buff[id] ≠ {}` guard prevents re-firing; the implementation enforces
    /// the same invariant by checking `write_buff` before advancing `s_clock`.
    pub fn handle_fast_commit(&mut self, tx_id: TxId) -> FastCommitResult {
        if self.aborted.contains(&tx_id) {
            return FastCommitResult::Abort;
        }
        // Idempotent retry: already committed via this fast path.
        if let Some(&ct) = self.fast_committed.get(&tx_id) {
            return FastCommitResult::Ok(ct);
        }
        // Check write_buff BEFORE advancing the clock (spec: write_buff[id] ≠ {}).
        // If there is nothing to commit and we haven't recorded a prior commit,
        // treat as Abort (transaction was never started on this shard).
        let writes = match self.write_buff.remove(&tx_id) {
            Some(w) => w,
            None => return FastCommitResult::Abort,
        };
        // Clock advances only when we have real work to commit.
        let commit_ts = self.clock + 1;
        self.clock = commit_ts;
        for (key, value) in writes {
            self.write_keys.remove(&key);
            let vs = self.versions.entry(key).or_default();
            // Idempotent + sorted insertion via binary search (same as handle_commit).
            if let Err(pos) = vs.binary_search_by_key(&commit_ts, |v| v.timestamp) {
                vs.insert(
                    pos,
                    Version {
                        value,
                        timestamp: commit_ts,
                    },
                );
            }
            self.dirty_keys.insert(key);
        }
        self.prepared.remove(&tx_id);
        self.prepare_times.remove(&tx_id);
        self.write_start_ts.remove(&tx_id);
        self.read_start_ts.remove(&tx_id);
        self.read_times.remove(&tx_id);
        // Record the commit_ts for idempotent retries.
        self.fast_committed.insert(tx_id, commit_ts);
        self.prune_aborted();
        FastCommitResult::Ok(commit_ts)
    }

    /// Handle WRITE_AND_FAST_COMMIT(id, start_ts, key, value) — single-write fast path.
    ///
    /// Atomically checks conflicts, installs the write directly into `versions[key]`
    /// (bypassing `write_buff`), and commits in a single step.  This eliminates the
    /// separate UPDATE_KEY → FAST_COMMIT round trip for single-write single-shard
    /// transactions.
    ///
    /// Conflict checks are identical to `handle_update`:
    ///   - CommittedConflict: any installed version of `key` at ts >= `start_ts`.
    ///   - PreparedConflict: any prepared tx that wrote `key` with prep_t >= `start_ts`.
    ///
    /// On conflict the tx is aborted and `FastCommitResult::Abort` is returned.
    /// On success the version is installed at `clock + 1`, the clock advances,
    /// and `FastCommitResult::Ok(commit_ts)` is returned.
    ///
    /// Matches `ShardHandleWriteAndFastCommit` in the TLA+ spec.
    pub fn handle_write_and_fast_commit(
        &mut self,
        tx_id: TxId,
        start_ts: Timestamp,
        key: Key,
        value: Value,
    ) -> FastCommitResult {
        if self.aborted.contains(&tx_id) {
            return FastCommitResult::Abort;
        }

        // CommittedConflict: any installed version with timestamp >= start_ts.
        let committed_conflict = self
            .versions
            .get(&key)
            .map(|vs| vs.partition_point(|v| v.timestamp < start_ts) < vs.len())
            .unwrap_or(false);
        if committed_conflict {
            self.aborted.insert(tx_id);
            return FastCommitResult::Abort;
        }

        // PreparedConflict: any other prepared tx that wrote this key has prep_t >= start_ts.
        let prepared_conflict = self.prepared.iter().any(|(&other, &prep_t)| {
            other != tx_id
                && prep_t >= start_ts
                && self
                    .write_buff
                    .get(&other)
                    .map(|wb| wb.contains_key(&key))
                    .unwrap_or(false)
        });
        if prepared_conflict {
            self.aborted.insert(tx_id);
            return FastCommitResult::Abort;
        }

        // No conflict — install version directly, bypassing write_buff.
        let commit_ts = self.clock + 1;
        self.clock = commit_ts;
        let vs = self.versions.entry(key).or_default();
        if let Err(pos) = vs.binary_search_by_key(&commit_ts, |v| v.timestamp) {
            vs.insert(
                pos,
                Version {
                    value,
                    timestamp: commit_ts,
                },
            );
        }
        self.dirty_keys.insert(key);
        self.prune_aborted();
        FastCommitResult::Ok(commit_ts)
    }

    /// Discard MVCC versions that can never be read by any active transaction.
    ///
    /// Compaction watermark = min `start_ts` across all transactions that currently
    /// have a write buffer on this shard (`write_start_ts`).  If there are no such
    /// transactions the watermark is 0 and nothing is compacted (conservative: we
    /// cannot observe the start_ts of read-only transactions).
    ///
    /// For each key, among all versions with `timestamp < watermark`, only the one
    /// with the highest timestamp can ever be the result of `LatestVersionBefore`
    /// for any active snapshot — all older ones are permanently shadowed and safe
    /// to discard.
    ///
    /// Matches `ShardCompactVersions` in the TLA+ spec.
    pub fn compact_versions(&mut self) {
        // Watermark = min start_ts across all active transactions on this shard:
        //   - write-buffered transactions (write_start_ts)
        //   - transactions that have read from this shard (read_start_ts)
        // Without read tracking, a transaction T (start_ts=5) that reads before its
        // first write on this shard would be invisible to the watermark, and a
        // concurrent writer (start_ts=100) would raise the watermark to 100, allowing
        // compaction to discard MVCC versions that T's snapshot still needs.
        let watermark = self
            .write_start_ts
            .values()
            .chain(self.read_start_ts.values())
            .copied()
            .min();
        let watermark = match watermark {
            Some(w) => w,
            None => return, // no active transactions; nothing to compact
        };
        // Only scan keys that have been committed to since the last compaction tick,
        // reducing per-tick cost from O(N_keys) to O(|dirty_keys|).
        // (ShardCompactVersions in TLA+: gated on k ∈ dirty_keys[s])
        let dirty: Vec<Key> = self.dirty_keys.drain().collect();
        for key in dirty {
            if let Some(versions) = self.versions.get_mut(&key) {
                // Index of the first version with timestamp >= watermark.
                let cutoff = versions.partition_point(|v| v.timestamp < watermark);
                // versions[..cutoff] are all below the watermark.
                // Keep versions[cutoff-1] (latest before watermark); drain the rest.
                if cutoff > 1 {
                    versions.drain(..cutoff - 1);
                }
            }
        }
    }

    /// Prune entries from `aborted` that are safe to discard.
    ///
    /// TxIds are encoded as `(coordinator_port << 32) | seq`.  A coordinator
    /// assigns seq numbers monotonically, so once it has an in-flight transaction
    /// with seq M it has already fully resolved every transaction with seq < M.
    /// Therefore an aborted entry with seq N from port P is safe to remove when
    /// `min_active_seq[P] > N` — i.e. when every in-flight transaction from P
    /// has a strictly higher sequence number — or when port P has no in-flight
    /// transactions at all.
    ///
    /// "In-flight" means the transaction still has a write buffer entry or a
    /// prepared entry on this shard.
    pub fn prune_aborted(&mut self) {
        // Build a watermark: for each coordinator port, find the minimum seq
        // number among all transactions that are still in-flight on this shard
        // (i.e., have a write buffer entry or a prepared entry).
        let mut min_active_seq: HashMap<u32, u32> = HashMap::new();
        for &tx_id in self.write_buff.keys().chain(self.prepared.keys()) {
            let port = (tx_id >> 32) as u32;
            let seq = tx_id as u32;
            let e = min_active_seq.entry(port).or_insert(u32::MAX);
            *e = (*e).min(seq);
        }
        // An aborted entry with (port, seq) is safe to discard when:
        //   - No in-flight tx from the same port exists, OR
        //   - The minimum in-flight seq from that port is strictly greater than seq.
        // In both cases the coordinator at `port` has progressed past `seq` and
        // will never send new messages for this tx_id.
        self.aborted.retain(|&tx_id| {
            let port = (tx_id >> 32) as u32;
            let seq = tx_id as u32;
            match min_active_seq.get(&port) {
                Some(&min) => seq >= min,
                None => false,
            }
        });
        // Prune fast_committed entries using the same watermark.  Once the
        // coordinator has no in-flight tx with a lower seq from the same port,
        // it will never send another FAST_COMMIT for that tx_id, so the stored
        // commit_ts is no longer needed for idempotent retries.
        self.fast_committed.retain(|&tx_id, _| {
            let port = (tx_id >> 32) as u32;
            let seq = tx_id as u32;
            match min_active_seq.get(&port) {
                Some(&min) => seq >= min,
                None => false,
            }
        });
    }

    /// Auto-abort prepared entries that have been waiting longer than `prepare_ttl`.
    ///
    /// This implements `ShardTimeoutPrepared` from the TLA+ spec: when a coordinator
    /// crashes mid-2PC (stuck in PREPARING), the shard unilaterally aborts the
    /// prepared entry after `prepare_ttl`, releasing write locks and unblocking
    /// conflicting writers.
    ///
    /// Pass `now = Instant::now()` in production. In tests, pass a future instant
    /// (e.g. `Instant::now() + Duration::from_secs(9_999_999)`) to force-expire
    /// entries without sleeping.
    ///
    /// Returns the set of TxIds that were auto-aborted.
    pub fn expire_prepared(&mut self, now: Instant) -> HashSet<TxId> {
        let expired: HashSet<TxId> = self
            .prepare_times
            .iter()
            .filter(|(_, &t)| now.saturating_duration_since(t) >= self.prepare_ttl)
            .map(|(&tx_id, _)| tx_id)
            .collect();

        for &tx_id in &expired {
            self.prepare_times.remove(&tx_id);
            self.aborted.insert(tx_id);
            self.ttl_expired.insert(tx_id);
            if let Some(writes) = self.write_buff.remove(&tx_id) {
                for key in writes.into_keys() {
                    self.write_keys.remove(&key);
                }
            }
            self.prepared.remove(&tx_id);
            self.write_start_ts.remove(&tx_id);
            self.read_start_ts.remove(&tx_id);
            self.read_times.remove(&tx_id);
        }

        if !expired.is_empty() {
            self.prune_aborted();
        }

        expired
    }

    /// Evict `read_start_ts` / `read_times` entries for transactions that have
    /// been tracking a read on this shard longer than `read_ttl`.
    ///
    /// This is the cleanup path for abandoned read-only transactions (client
    /// crashed, network partition, etc.) that never send a COMMIT or ABORT to
    /// this shard. Without eviction, their `start_ts` would anchor the
    /// compaction watermark forever, preventing MVCC history from shrinking.
    ///
    /// Transactions that hold write buffers on this shard are NOT evicted here —
    /// those are handled by `expire_prepared` via the write-side `prepare_ttl`.
    ///
    /// Pass `now = Instant::now()` in production. In tests, pass a future instant
    /// to force eviction without sleeping.
    pub fn expire_reads(&mut self, now: Instant) {
        let expired: Vec<TxId> = self
            .read_times
            .iter()
            .filter(|(_, &t)| now.saturating_duration_since(t) >= self.read_ttl)
            .map(|(&tx_id, _)| tx_id)
            .collect();

        for tx_id in expired {
            self.read_times.remove(&tx_id);
            self.read_start_ts.remove(&tx_id);
        }
    }

    /// Override the recorded read time for `tx_id`. Only for use in tests
    /// to simulate entries that were read at an arbitrary past instant without
    /// requiring the test to sleep.
    #[cfg(test)]
    pub fn force_read_time(&mut self, tx_id: TxId, t: Instant) {
        self.read_times.insert(tx_id, t);
    }

    /// Override the recorded prepare time for `tx_id`. Only for use in tests
    /// to simulate entries that were prepared at an arbitrary past instant without
    /// requiring the test to sleep.
    #[cfg(test)]
    pub fn force_prepare_time(&mut self, tx_id: TxId, t: Instant) {
        self.prepare_times.insert(tx_id, t);
    }
}

#[cfg(test)]
mod tests;

pub mod server;
