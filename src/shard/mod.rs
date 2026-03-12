use std::collections::{HashMap, HashSet};

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
    /// Prepared transactions and their prepare timestamps.
    pub prepared: HashMap<TxId, Timestamp>,
    /// Transactions that have been aborted at this shard.
    pub aborted: HashSet<TxId>,
}

impl ShardState {
    /// Create a new shard with no committed versions.
    /// Keys come into existence on first commit (matching TLA+ `versions = [k \in Keys |-> {}]`).
    pub fn new() -> Self {
        ShardState {
            clock: 0,
            versions: HashMap::new(),
            write_buff: HashMap::new(),
            prepared: HashMap::new(),
            aborted: HashSet::new(),
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
        // If none exists the key has not been inserted at this snapshot.
        let best = self
            .versions
            .get(&key)
            .and_then(|vs| vs.iter().filter(|v| v.timestamp < start_ts).max_by_key(|v| v.timestamp))
            .cloned();

        self.clock = self.clock.max(start_ts);
        match best {
            Some(ver) => ReadResult::Value(ver.value),
            None => ReadResult::NotFound,
        }
    }

    /// Handle UPDATE_KEY(id, start_ts, key, value).
    ///
    /// Detects write-write conflicts:
    ///   - Abort if any committed version of `key` has timestamp >= start_ts.
    ///   - Abort if any prepared transaction that wrote `key` has prep_t >= start_ts.
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
        let committed_conflict = self
            .versions
            .get(&key)
            .map(|vs| vs.iter().any(|v| v.timestamp >= start_ts))
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

        // WriteBuffConflict: any other non-aborted tx already buffered a write to this key,
        // regardless of whether it is prepared. A prepared tx retains its write in write_buff
        // until commit, so the lock persists.
        let write_buff_conflict = self.write_buff.iter().any(|(&other, wb)| {
            other != tx_id && !self.aborted.contains(&other) && wb.contains_key(&key)
        });
        if write_buff_conflict {
            self.aborted.insert(tx_id);
            return UpdateResult::Abort;
        }

        // Buffer the write and advance clock.
        self.write_buff
            .entry(tx_id)
            .or_default()
            .insert(key, value);
        self.clock = self.clock.max(start_ts);
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
        PrepareResult::Timestamp(prep_t)
    }

    /// Handle COMMIT(id, commit_ts).
    ///
    /// Installs all buffered writes for `tx_id` as new MVCC versions at
    /// `commit_ts`, clears the write buffer, and removes the tx from `prepared`.
    pub fn handle_commit(&mut self, tx_id: TxId, commit_ts: Timestamp) {
        if let Some(writes) = self.write_buff.remove(&tx_id) {
            for (key, value) in writes {
                let vs = self.versions.entry(key).or_default();
                // Idempotent: skip if this timestamp is already installed.
                if !vs.iter().any(|v| v.timestamp == commit_ts) {
                    vs.push(Version { value, timestamp: commit_ts });
                    vs.sort_by_key(|v| v.timestamp);
                }
            }
        }
        self.prepared.remove(&tx_id);
    }

    /// Handle ABORT(id).
    ///
    /// Marks `tx_id` as aborted, clears its write buffer, and removes it from
    /// `prepared`.
    pub fn handle_abort(&mut self, tx_id: TxId) {
        self.aborted.insert(tx_id);
        self.write_buff.remove(&tx_id);
        self.prepared.remove(&tx_id);
    }
}


#[cfg(test)]
mod tests;

pub mod server;
