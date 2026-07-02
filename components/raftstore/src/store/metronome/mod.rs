// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Metronome: a deterministic log-shuffling scheme that reduces
//! redundant raft-log fsyncs across a cluster.
//!
//! Under standard Raft, every replica WAL-persists every committed
//! log entry, so every entry ends up durably on N replicas. Metronome
//! decouples durability from agreement: only a rotating K-sized
//! persist-set (K >= f+1) actually fsyncs each entry; the remaining
//! N-K replicas keep the entry only in their in-memory raft log and
//! ACK from memory. HardState (term, vote, commit) is still persisted
//! on every replica on every Ready event so election safety is
//! preserved.
//!
//! Safety. Because every persist-set is a majority (K >= f+1) and any
//! majority intersects every other majority, each committed entry
//! remains durably recoverable from any future majority of replicas.
//! See Ng et al., "Metronome: I/O-Efficient and Low-Cost Logging for
//! Consensus" for the full proof.
//!
//! Load balancing. The persist-set rotates by one position per entry
//! index, so consecutive entries share K-1 persisters. Over the long
//! run each node persists K/N of all entries.
//!
//! This module is pure computation — it has no side effects, no I/O,
//! and no dependency on raftstore internals. Callers wire
//! [`Scheme::should_persist`] into their storage save path. The
//! persist-set is built from the *voting* members of the region only;
//! learners never participate in Metronome's durability guarantees.

use std::{
    fmt,
    time::{Duration, Instant},
};

use lazy_static::lazy_static;
use prometheus::{register_int_counter, IntCounter};
use raft::eraftpb::{Entry, EntryType};

lazy_static! {
    /// Fires when this node's on-disk HardState.commit was clamped
    /// down to last_index at startup. Under metronome, this can
    /// legitimately happen after a crash because the sparse WAL may
    /// have lost entries that we memory-ACKed before the crash. An
    /// elevated rate of these relative to restarts suggests something
    /// else is wrong.
    pub static ref METRONOME_COMMIT_CLAMPS_ON_LOAD: IntCounter = register_int_counter!(
        "tikv_raftstore_metronome_commit_clamps_on_load_total",
        "Number of times a Peer clamped HardState.commit down to local last_index at startup under metronome mode."
    )
    .unwrap();

    /// Fires when an incoming MsgHeartbeat / MsgAppend carried a
    /// Commit value greater than our local last_index and we
    /// clamped it. A pre-existing counter would have no way to
    /// surface raft-rs panic pressure; this one does.
    pub static ref METRONOME_INCOMING_COMMIT_CLAMPS: IntCounter = register_int_counter!(
        "tikv_raftstore_metronome_incoming_commit_clamps_total",
        "Number of times an incoming heartbeat/append Commit was clamped to local last_index under metronome mode."
    )
    .unwrap();

    /// Fires whenever a ConfChange applies and we rebuild the
    /// persist-set scheme from the new voter list. One increment per
    /// region per ConfChange commit.
    pub static ref METRONOME_SCHEME_REBUILDS: IntCounter = register_int_counter!(
        "tikv_raftstore_metronome_scheme_rebuilds_total",
        "Number of times the metronome scheme was rebuilt after a ConfChange commit."
    )
    .unwrap();

    /// Increments by the number of entries this node did NOT fsync
    /// on each Ready because they fell outside its persist-set. This
    /// is the primary signal that metronome is doing useful work.
    pub static ref METRONOME_ENTRIES_SKIPPED: IntCounter = register_int_counter!(
        "tikv_raftstore_metronome_entries_skipped_total",
        "Cumulative count of raft log entries filtered out of the WAL write batch on followers under metronome mode."
    )
    .unwrap();

    /// Fires each time a stalled peer enters the "log everything"
    /// window via work-stealing (paper §4.2). An elevated rate
    /// correlates with a persist-set straggler in the cluster.
    pub static ref METRONOME_WORK_STEALS_TRIGGERED: IntCounter = register_int_counter!(
        "tikv_raftstore_metronome_work_steals_triggered_total",
        "Number of times a metronome follower triggered a work-steal due to a commit-index stall while holding buffered skipped entries."
    )
    .unwrap();
}

/// Errors that can be returned when constructing a [`Scheme`].
#[derive(Debug, PartialEq, Eq)]
pub enum SchemeError {
    EmptyNodeIDs,
    DuplicateNodeID(u64),
    QuorumBelowMajority { quorum: usize, min: usize },
    QuorumAboveCluster { quorum: usize, cluster: usize },
}

impl fmt::Display for SchemeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemeError::EmptyNodeIDs => write!(f, "metronome: node_ids must be non-empty"),
            SchemeError::DuplicateNodeID(id) => write!(f, "metronome: duplicate node_id {}", id),
            SchemeError::QuorumBelowMajority { quorum, min } => {
                write!(f, "metronome: quorum_size {} < f+1 ({})", quorum, min)
            }
            SchemeError::QuorumAboveCluster { quorum, cluster } => {
                write!(f, "metronome: quorum_size {} > N ({})", quorum, cluster)
            }
        }
    }
}

impl std::error::Error for SchemeError {}

/// A deterministic round-robin persist-set picker. Built from the
/// current voting membership and quorum size, and immutable
/// thereafter. On membership change, callers construct a new
/// `Scheme` from the updated voter list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scheme {
    /// Voting node IDs sorted ascending. Sorting ensures every node
    /// in the cluster derives the same scheme regardless of how the
    /// membership list was assembled.
    node_ids: Vec<u64>,
    /// K — number of nodes that persist each entry. Invariant:
    /// `f+1 <= quorum_size <= node_ids.len()`.
    quorum_size: usize,
}

/// Returns `f+1 = floor(n/2) + 1`, the smallest quorum that tolerates
/// `f = floor((n-1)/2)` failures. For `n == 0`, returns `1` as a
/// safe floor.
pub fn default_quorum_size(n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    n / 2 + 1
}

impl Scheme {
    /// Construct a new `Scheme` from the voting member ids and a
    /// desired quorum size. Pass `quorum_size == 0` to select the
    /// default (`f+1`).
    pub fn new(node_ids: Vec<u64>, quorum_size: usize) -> Result<Self, SchemeError> {
        if node_ids.is_empty() {
            return Err(SchemeError::EmptyNodeIDs);
        }
        let k = if quorum_size == 0 {
            default_quorum_size(node_ids.len())
        } else {
            quorum_size
        };
        let min = default_quorum_size(node_ids.len());
        if k < min {
            return Err(SchemeError::QuorumBelowMajority { quorum: k, min });
        }
        if k > node_ids.len() {
            return Err(SchemeError::QuorumAboveCluster {
                quorum: k,
                cluster: node_ids.len(),
            });
        }
        let mut sorted = node_ids;
        sorted.sort_unstable();
        for i in 1..sorted.len() {
            if sorted[i] == sorted[i - 1] {
                return Err(SchemeError::DuplicateNodeID(sorted[i]));
            }
        }
        Ok(Scheme {
            node_ids: sorted,
            quorum_size: k,
        })
    }

    /// Number of members in the cluster (voters only).
    #[inline]
    pub fn num_nodes(&self) -> usize {
        self.node_ids.len()
    }

    /// The persist-set size K.
    #[inline]
    pub fn quorum_size(&self) -> usize {
        self.quorum_size
    }

    /// The sorted voting ids.
    #[inline]
    pub fn node_ids(&self) -> &[u64] {
        &self.node_ids
    }

    /// Returns the K node IDs that should persist the entry at
    /// `index`. The returned Vec is freshly allocated; callers that
    /// only need membership testing should prefer
    /// [`Scheme::should_persist`] for zero-allocation queries.
    pub fn persist_set(&self, index: u64) -> Vec<u64> {
        let n = self.node_ids.len();
        let start = (index % n as u64) as usize;
        let mut out = Vec::with_capacity(self.quorum_size);
        for i in 0..self.quorum_size {
            out.push(self.node_ids[(start + i) % n]);
        }
        out
    }

    /// Reports whether `node_id` should WAL-persist the entry at
    /// `index` under this scheme. Returns `false` when `node_id` is
    /// not a voting member of the scheme.
    pub fn should_persist(&self, node_id: u64, index: u64) -> bool {
        let n = self.node_ids.len();
        let start = (index % n as u64) as usize;
        for i in 0..self.quorum_size {
            if self.node_ids[(start + i) % n] == node_id {
                return true;
            }
        }
        false
    }
}

// ---- Work stealing (paper §4.2) -------------------------------------

/// Per-peer work-stealing state. Instantiated once per region when
/// metronome is enabled and driven from the Ready loop. Pure
/// computation + time; no I/O, no locks — callers serialise access
/// naturally because all Peer state runs on a single FSM thread.
///
/// The algorithm: track the indices this follower dropped via the
/// filter; arm a stall timer the moment the buffer transitions from
/// empty → non-empty; reset the timer whenever the cluster's
/// committed index advances. If the timer's elapsed time exceeds
/// `timeout` while the buffer is still non-empty AND we've observed
/// at least one real commit (i.e. the cluster is past cold-start),
/// enter a "log everything" window for `duration`. In the window the
/// filter becomes a passthrough so subsequent entries are fsynced,
/// unsticking the leader's commit pipeline that had been waiting on
/// a straggler persister.
///
/// Regression guards baked into the code (each corresponds to a
/// real bug we hit in the etcd port):
///   - B3(a): default `timeout` is set by the caller (1s) — far
///     above typical p99 commit jitter.
///   - B3(b): `last_advance` is stamped only on the 0 → non-empty
///     transition of the skipped buffer, not on every Ready.
///   - B3(c): `maybe_trigger` refuses to fire until `last_commit > 0`,
///     i.e. at least one real commit has been observed.
#[derive(Debug)]
pub struct WorkSteal {
    /// Skipped indices, kept ascending. Drained by commit advance.
    skipped: Vec<u64>,
    /// Highest HardState.commit we've ever observed on this peer.
    /// Zero until the cluster's first commit propagates here.
    last_commit: u64,
    /// Wall-time stamp of the latest "stall clock reset" event:
    /// either a commit-index advance or the buffer arming on 0→N.
    /// None until armed the first time.
    last_advance: Option<Instant>,
    /// Non-None means we're in the "log everything" window; the
    /// instant is when the window expires.
    active_until: Option<Instant>,
    /// Stall threshold.
    timeout: Duration,
    /// "Log everything" window length.
    duration: Duration,
}

impl WorkSteal {
    pub fn new(timeout: Duration, duration: Duration) -> Self {
        WorkSteal {
            skipped: Vec::new(),
            last_commit: 0,
            last_advance: None,
            active_until: None,
            timeout,
            duration,
        }
    }

    /// Returns true if we're currently in the "log everything"
    /// window. The caller uses this to make the filter a passthrough.
    pub fn is_active(&self, now: Instant) -> bool {
        self.active_until.map_or(false, |t| now < t)
    }

    /// Called from the Ready loop after filtering, with:
    ///   - `hs_commit`: the HardState.commit in this Ready
    ///     (or the last known one if the Ready carried no new HS).
    ///   - `newly_skipped`: indices we chose not to fsync this round.
    pub fn record(&mut self, hs_commit: u64, newly_skipped: impl IntoIterator<Item = u64>, now: Instant) {
        // Advance commit + drain.
        if hs_commit > self.last_commit {
            self.last_commit = hs_commit;
            self.last_advance = Some(now);
            if !self.skipped.is_empty() {
                let drop_upto = hs_commit;
                let mut i = 0;
                while i < self.skipped.len() && self.skipped[i] <= drop_upto {
                    i += 1;
                }
                self.skipped.drain(..i);
            }
        }

        // If we're already in the active window, don't re-buffer —
        // everything is being persisted this Ready anyway.
        if self.is_active(now) {
            return;
        }

        // Append new skipped indices. Arm the timer on 0→N transition.
        let was_empty = self.skipped.is_empty();
        for idx in newly_skipped {
            if idx > self.last_commit {
                self.skipped.push(idx);
            }
        }
        if was_empty && !self.skipped.is_empty() {
            self.last_advance = Some(now);
        }
    }

    /// Decide whether to fire the work-steal. Returns true on the
    /// transition-to-active edge; the caller should bump metrics and
    /// proceed. Subsequent calls within the window return false.
    pub fn maybe_trigger(&mut self, now: Instant) -> bool {
        // Already active? Check if the window has elapsed.
        if let Some(until) = self.active_until {
            if now < until {
                return false;
            }
            // Window elapsed; clear so the next stall can re-arm.
            self.active_until = None;
            return false;
        }
        // Buffer empty → nothing at risk.
        if self.skipped.is_empty() {
            return false;
        }
        // Startup guard: don't fire before the cluster's first commit
        // has reached this peer (B3(c) regression).
        if self.last_commit == 0 {
            return false;
        }
        // Stall clock.
        match self.last_advance {
            Some(t) if now.saturating_duration_since(t) >= self.timeout => {
                self.active_until = Some(now + self.duration);
                // Buffer now absorbed by the window; subsequent entries
                // are persisted fully during the window so drop stale
                // tracking state.
                self.skipped.clear();
                self.last_advance = Some(now);
                true
            }
            _ => false,
        }
    }

    // ---- Exposed for tests/observability ----
    #[cfg(any(test, feature = "testexport"))]
    pub fn skipped_len(&self) -> usize {
        self.skipped.len()
    }
    #[cfg(any(test, feature = "testexport"))]
    pub fn last_commit(&self) -> u64 {
        self.last_commit
    }
}

// ---- Filter helper --------------------------------------------------

/// Filter a batch of entries according to the metronome scheme,
/// retaining only those this node should WAL-persist.
///
/// Behaviour:
/// - If `is_leader` is true, the vec is returned unchanged (leaders
///   always persist everything, which keeps recovery cheap).
/// - If `scheme` is None (metronome not initialised for this peer
///   yet — e.g. during bootstrap before the first voter list is
///   known), the vec is returned unchanged (fail-safe to baseline).
/// - `EntryConfChange` / `EntryConfChangeV2` entries are always
///   kept regardless of the scheme — membership transitions must
///   be durable on every node so the scheme can be rebuilt after
///   restart (Metronome §4.5).
/// - Otherwise, entries where `scheme.should_persist(node_id, idx)
///   == false` are dropped.
///
/// The returned `usize` is how many entries were skipped. Callers use
/// it for the work-stealing buffer (Phase 4) and the skipped-count
/// metric (Phase 5).
pub fn filter_entries(
    entries: &mut Vec<Entry>,
    scheme: Option<&Scheme>,
    node_id: u64,
    is_leader: bool,
) -> usize {
    if is_leader || scheme.is_none() || entries.is_empty() {
        return 0;
    }
    let scheme = scheme.unwrap();
    let original_len = entries.len();
    entries.retain(|e| match e.get_entry_type() {
        EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => true,
        _ => scheme.should_persist(node_id, e.get_index()),
    });
    original_len - entries.len()
}

// ---- Tests ----------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ----- constructor validation -----

    #[test]
    fn new_rejects_empty() {
        assert_eq!(Scheme::new(vec![], 0), Err(SchemeError::EmptyNodeIDs));
    }

    #[test]
    fn new_rejects_duplicates() {
        assert_eq!(
            Scheme::new(vec![1, 2, 2, 3], 0),
            Err(SchemeError::DuplicateNodeID(2))
        );
    }

    #[test]
    fn new_rejects_quorum_below_majority() {
        // n=5 → f+1 = 3. Quorum of 2 is invalid.
        match Scheme::new(vec![1, 2, 3, 4, 5], 2) {
            Err(SchemeError::QuorumBelowMajority { quorum: 2, min: 3 }) => {}
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn new_rejects_quorum_above_cluster() {
        match Scheme::new(vec![1, 2, 3], 4) {
            Err(SchemeError::QuorumAboveCluster {
                quorum: 4,
                cluster: 3,
            }) => {}
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn new_quorum_zero_selects_default() {
        let s = Scheme::new(vec![10, 20, 30], 0).unwrap();
        assert_eq!(s.quorum_size(), 2);

        let s = Scheme::new(vec![1, 2, 3, 4, 5], 0).unwrap();
        assert_eq!(s.quorum_size(), 3);

        let s = Scheme::new(vec![1, 2, 3, 4, 5, 6, 7], 0).unwrap();
        assert_eq!(s.quorum_size(), 4);
    }

    #[test]
    fn default_quorum_size_boundaries() {
        assert_eq!(default_quorum_size(0), 1);
        assert_eq!(default_quorum_size(1), 1);
        assert_eq!(default_quorum_size(2), 2);
        assert_eq!(default_quorum_size(3), 2);
        assert_eq!(default_quorum_size(4), 3);
        assert_eq!(default_quorum_size(5), 3);
        assert_eq!(default_quorum_size(6), 4);
        assert_eq!(default_quorum_size(7), 4);
    }

    // ----- persist_set rotation -----

    #[test]
    fn persist_set_rotates() {
        let s = Scheme::new(vec![1, 2, 3], 2).unwrap();
        assert_eq!(s.persist_set(0), vec![1, 2]);
        assert_eq!(s.persist_set(1), vec![2, 3]);
        assert_eq!(s.persist_set(2), vec![3, 1]);
        // wraps across N
        assert_eq!(s.persist_set(3), vec![1, 2]);
        assert_eq!(s.persist_set(4), vec![2, 3]);
    }

    #[test]
    fn persist_set_k_equals_n_is_everyone() {
        let s = Scheme::new(vec![1, 2, 3], 3).unwrap();
        for i in 0..10u64 {
            let mut got = s.persist_set(i);
            got.sort_unstable();
            assert_eq!(got, vec![1, 2, 3]);
        }
    }

    // ----- should_persist (hand-coded reference cross-check) -----

    #[test]
    fn should_persist_matches_persist_set() {
        let s = Scheme::new(vec![1, 2, 3, 4, 5], 3).unwrap();
        for idx in 0..50u64 {
            let set = s.persist_set(idx);
            for &node in s.node_ids() {
                let in_set = set.contains(&node);
                let got = s.should_persist(node, idx);
                assert_eq!(
                    got, in_set,
                    "idx={} node={} expected={} got={}",
                    idx, node, in_set, got
                );
            }
        }
    }

    #[test]
    fn should_persist_false_for_non_member() {
        let s = Scheme::new(vec![1, 2, 3], 2).unwrap();
        for idx in 0..20u64 {
            assert!(!s.should_persist(99, idx));
        }
    }

    // ----- determinism -----

    #[test]
    fn deterministic_from_unsorted_input() {
        let a = Scheme::new(vec![3, 1, 2], 2).unwrap();
        let b = Scheme::new(vec![1, 2, 3], 2).unwrap();
        let c = Scheme::new(vec![2, 3, 1], 2).unwrap();
        assert_eq!(a, b);
        assert_eq!(b, c);
        // persist_set is stable too
        for idx in 0..10u64 {
            assert_eq!(a.persist_set(idx), b.persist_set(idx));
            assert_eq!(b.persist_set(idx), c.persist_set(idx));
        }
    }

    // ----- load-distribution sanity -----

    #[test]
    fn each_node_persists_k_over_n_fraction() {
        // Over N consecutive indices each node should appear exactly
        // K times (every node is in K of the N rotating slots).
        let s = Scheme::new(vec![1, 2, 3, 4, 5], 3).unwrap();
        let mut count = std::collections::HashMap::<u64, usize>::new();
        for idx in 0..s.num_nodes() as u64 {
            for n in s.persist_set(idx) {
                *count.entry(n).or_default() += 1;
            }
        }
        for &n in s.node_ids() {
            assert_eq!(count[&n], s.quorum_size());
        }
    }

    // ----- filter_entries -----

    fn ent(index: u64, ty: EntryType) -> Entry {
        let mut e = Entry::default();
        e.set_index(index);
        e.set_entry_type(ty);
        e
    }

    #[test]
    fn filter_leader_keeps_all() {
        let s = Scheme::new(vec![1, 2, 3], 2).unwrap();
        let mut ents = (1..=10)
            .map(|i| ent(i, EntryType::EntryNormal))
            .collect::<Vec<_>>();
        let skipped = filter_entries(&mut ents, Some(&s), 1, true);
        assert_eq!(skipped, 0);
        assert_eq!(ents.len(), 10);
    }

    #[test]
    fn filter_no_scheme_keeps_all() {
        let mut ents = (1..=10)
            .map(|i| ent(i, EntryType::EntryNormal))
            .collect::<Vec<_>>();
        let skipped = filter_entries(&mut ents, None, 1, false);
        assert_eq!(skipped, 0);
        assert_eq!(ents.len(), 10);
    }

    #[test]
    fn filter_follower_drops_non_persist_set() {
        let s = Scheme::new(vec![1, 2, 3], 2).unwrap();
        // Node 3 is in persist-set for indices with (i % 3) in {1, 2} → {1, 2, 4, 5, 7, 8, ...}
        let mut ents = (1..=6)
            .map(|i| ent(i, EntryType::EntryNormal))
            .collect::<Vec<_>>();
        // Node 1 (sorted pos 0): in persist-set for idx%3 ∈ {0,2} → indices 3, 5, 6 (not 1,2,4)
        // The rotation starts at `index % N`; for index=1, persist-set = {node at pos 1, pos 2} = {2,3}
        // So node 1 is NOT in set for idx=1. Let's just verify against should_persist.
        let before: Vec<u64> = ents.iter().map(|e| e.get_index()).collect();
        let skipped = filter_entries(&mut ents, Some(&s), 1, false);
        let after: Vec<u64> = ents.iter().map(|e| e.get_index()).collect();
        for &i in &before {
            let was_kept = after.contains(&i);
            let expected = s.should_persist(1, i);
            assert_eq!(
                was_kept, expected,
                "index {} should_persist(1)={} was_kept={}",
                i, expected, was_kept
            );
        }
        assert_eq!(skipped, before.len() - after.len());
        assert!(skipped > 0);
    }

    #[test]
    fn filter_always_keeps_conf_change() {
        let s = Scheme::new(vec![1, 2, 3], 2).unwrap();
        // Construct 5 ConfChange entries; even if scheme says don't
        // persist, they must all be retained.
        let mut ents = (1..=5)
            .map(|i| ent(i, EntryType::EntryConfChange))
            .collect::<Vec<_>>();
        let skipped = filter_entries(&mut ents, Some(&s), 1, false);
        assert_eq!(skipped, 0);
        assert_eq!(ents.len(), 5);
        // Same with V2.
        let mut ents = (1..=5)
            .map(|i| ent(i, EntryType::EntryConfChangeV2))
            .collect::<Vec<_>>();
        let skipped = filter_entries(&mut ents, Some(&s), 1, false);
        assert_eq!(skipped, 0);
        assert_eq!(ents.len(), 5);
    }

    #[test]
    fn filter_mixed_keeps_confchange_drops_normal_when_not_in_set() {
        let s = Scheme::new(vec![1, 2, 3], 2).unwrap();
        let mut ents = vec![
            ent(1, EntryType::EntryNormal),
            ent(2, EntryType::EntryConfChange),
            ent(3, EntryType::EntryNormal),
            ent(4, EntryType::EntryConfChangeV2),
        ];
        filter_entries(&mut ents, Some(&s), 1, false);
        // Entries 2 and 4 must always be present.
        let indices: Vec<u64> = ents.iter().map(|e| e.get_index()).collect();
        assert!(indices.contains(&2));
        assert!(indices.contains(&4));
    }

    // ----- WorkSteal state machine -----

    fn ws(timeout_ms: u64, duration_ms: u64) -> WorkSteal {
        WorkSteal::new(
            Duration::from_millis(timeout_ms),
            Duration::from_millis(duration_ms),
        )
    }

    #[test]
    fn ws_baseline_init() {
        let mut w = ws(1000, 60_000);
        let t0 = Instant::now();
        // First Ready: commit=0, skipped=[2,3]. Buffer was empty →
        // last_advance must be armed.
        w.record(0, [2u64, 3], t0);
        assert_eq!(w.skipped_len(), 2);
        assert!(w.last_advance.is_some(), "timer must arm on 0→N");
    }

    #[test]
    fn ws_no_arm_without_skipped_entries() {
        // B3(b) regression guard: a Ready with no skipped entries
        // must NOT arm the timer.
        let mut w = ws(1000, 60_000);
        let t0 = Instant::now();
        w.record(0, std::iter::empty(), t0);
        assert!(w.last_advance.is_none(), "no arm with empty skip list");
        // Next Ready adds skipped indices → arm AT THAT MOMENT.
        let t1 = t0 + Duration::from_millis(50);
        w.record(0, [5u64], t1);
        assert!(w.last_advance.is_some());
        assert!(w.last_advance.unwrap() >= t1);
    }

    #[test]
    fn ws_commit_advance_drains_buffer() {
        let mut w = ws(1000, 60_000);
        let t0 = Instant::now();
        w.record(0, [2u64, 3, 4, 5], t0);
        assert_eq!(w.skipped_len(), 4);
        // Commit advances past 3 → indices 2 and 3 drain.
        let t1 = t0 + Duration::from_millis(10);
        w.record(3, std::iter::empty(), t1);
        assert_eq!(w.skipped_len(), 2);
    }

    #[test]
    fn ws_no_fire_before_first_commit() {
        // B3(c) regression guard: before the first commit ever
        // reaches us, even a stale timer must not fire. Otherwise
        // cluster cold-start spuriously puts us in log-everything
        // mode and wipes the byte savings for a full `duration`.
        let mut w = ws(1, 60_000);
        let t0 = Instant::now();
        w.record(0, [5u64], t0);
        // Advance time past the timeout; commit is still 0.
        let t1 = t0 + Duration::from_millis(500);
        let triggered = w.maybe_trigger(t1);
        assert!(!triggered, "must not fire when last_commit == 0");
        assert!(!w.is_active(t1));
    }

    #[test]
    fn ws_fires_on_stall() {
        let mut w = ws(10, 1_000);
        let t0 = Instant::now();
        // Armed + commit observed + buffered.
        w.record(1, [5u64, 7], t0);
        assert_eq!(w.last_commit(), 1);
        // Advance time past timeout without another commit.
        let t1 = t0 + Duration::from_millis(50);
        let fired = w.maybe_trigger(t1);
        assert!(fired);
        assert!(w.is_active(t1));
    }

    #[test]
    fn ws_no_fire_when_empty() {
        let mut w = ws(1, 1_000);
        let t0 = Instant::now();
        // commit advanced; never buffered anything.
        w.record(10, std::iter::empty(), t0);
        let t1 = t0 + Duration::from_millis(100);
        assert!(!w.maybe_trigger(t1));
    }

    #[test]
    fn ws_no_fire_within_timeout() {
        let mut w = ws(1_000, 60_000);
        let t0 = Instant::now();
        w.record(1, [3u64], t0);
        let t1 = t0 + Duration::from_millis(10);
        assert!(!w.maybe_trigger(t1), "still under timeout");
    }

    #[test]
    fn ws_exits_after_duration() {
        let mut w = ws(1, 20);
        let t0 = Instant::now();
        w.record(1, [3u64], t0);
        let t1 = t0 + Duration::from_millis(10);
        assert!(w.maybe_trigger(t1), "first trigger");
        assert!(w.is_active(t1));
        let t2 = t0 + Duration::from_millis(100);
        assert!(!w.is_active(t2));
        // After the window, a subsequent trigger check clears state.
        w.maybe_trigger(t2);
        assert!(w.active_until.is_none());
    }

    #[test]
    fn ws_active_suppresses_buffering() {
        let mut w = ws(1, 1_000);
        let t0 = Instant::now();
        w.record(1, [3u64], t0);
        let t1 = t0 + Duration::from_millis(10);
        assert!(w.maybe_trigger(t1));
        assert_eq!(w.skipped_len(), 0, "trigger should clear buffer");
        // New skipped entries during the window should NOT accumulate.
        let t2 = t0 + Duration::from_millis(50);
        w.record(1, [9u64], t2);
        assert_eq!(w.skipped_len(), 0, "no buffering while active");
    }

    #[test]
    fn ws_buffer_drained_in_commit_order() {
        let mut w = ws(1_000, 60_000);
        let t0 = Instant::now();
        w.record(0, [2u64, 3, 4], t0);
        w.record(0, [5u64, 6, 7], t0 + Duration::from_millis(1));
        w.record(0, [8u64, 9, 10], t0 + Duration::from_millis(2));
        assert_eq!(w.skipped_len(), 9);
        w.record(6, std::iter::empty(), t0 + Duration::from_millis(10));
        assert_eq!(w.skipped_len(), 4, "indices ≤ 6 drained");
        w.record(10, std::iter::empty(), t0 + Duration::from_millis(20));
        assert_eq!(w.skipped_len(), 0);
    }

    #[test]
    fn large_cluster_wraparound() {
        let ids: Vec<u64> = (1..=7).collect();
        let s = Scheme::new(ids, 0).unwrap(); // default quorum 4
        assert_eq!(s.quorum_size(), 4);
        // index 6 starts at slot 6 (last); wraps to 0,1,2
        assert_eq!(s.persist_set(6), vec![7, 1, 2, 3]);
        // index 10 = 10 % 7 = 3 → [4,5,6,7]
        assert_eq!(s.persist_set(10), vec![4, 5, 6, 7]);
    }
}
