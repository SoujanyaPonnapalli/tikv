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

use std::fmt;

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
