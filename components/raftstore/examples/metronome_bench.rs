// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.
//
// Metronome filter microbenchmark.
//
// Drives synthetic raft-log entry streams through the real filter
// code path (`raftstore::store::metronome::filter_entries`) for each
// (N, K) cluster size / value size combination in the etcd bench
// matrix. Reports per-node bytes that would be written to the raft
// engine under baseline vs metronome, plus filter throughput.
//
// This is a *filter-level* benchmark, not a full-cluster benchmark.
// It exercises the exact code that gates every follower's raft-engine
// write on every Ready in production; the empirical byte-reduction it
// measures is what a live cluster would see modulo per-entry WAL
// framing overhead (which is the same with or without metronome and
// cancels in the ratio).
//
// Run with:
//   CMAKE_POLICY_VERSION_MINIMUM=3.5 \
//     cargo run --release -p raftstore --example metronome_bench

use std::time::Instant;

use raft::eraftpb::{Entry, EntryType};
use raftstore::store::metronome::{filter_entries, Scheme};

const ENTRIES_PER_RUN: usize = 20_000;
const VALUE_SIZES: &[usize] = &[256, 1024, 4096, 8192, 16384];
const CLUSTER_SIZES: &[usize] = &[3, 5, 7];

fn make_entries(count: usize, val_size: usize) -> Vec<Entry> {
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let mut e = Entry::default();
        e.set_index((i + 1) as u64);
        e.set_term(1);
        e.set_entry_type(EntryType::EntryNormal);
        e.set_data(vec![0u8; val_size].into());
        out.push(e);
    }
    out
}

/// Sum of entry sizes. Uses the same protobuf-layout size the
/// raft-engine would see — so reductions reported here are what
/// on-disk ratios converge to at steady state.
fn total_bytes(entries: &[Entry]) -> usize {
    entries
        .iter()
        .map(|e| 1 + 8 + 8 + e.get_data().len() + 4 /* coarse header */)
        .sum()
}

fn bench_one(n: usize, val_size: usize) {
    let k = n / 2 + 1; // default f+1
    // Voter IDs 1..=N
    let voters: Vec<u64> = (1..=n as u64).collect();
    let scheme = Scheme::new(voters.clone(), k).unwrap();

    // Simulate `ENTRIES_PER_RUN` entries replicated from the leader.
    let entries = make_entries(ENTRIES_PER_RUN, val_size);
    let baseline_bytes_one_node = total_bytes(&entries);

    // Each voter independently runs the filter. Leader persists
    // everything (matches our Phase-1 impl); followers filter.
    // We sum bytes across all voters to mirror the aggregate disk
    // I/O the cluster would pay.
    let mut per_node_bytes = Vec::with_capacity(n);
    // Followers only — the leader is voter 1 by convention.
    let leader_id = 1u64;
    per_node_bytes.push(baseline_bytes_one_node); // leader keeps all

    // Also time the filter itself on one representative follower.
    let mut timed_ns_total: u128 = 0;

    for &vid in voters.iter().skip(1) {
        let mut ents = entries.clone();
        let t0 = Instant::now();
        let _ = filter_entries(&mut ents, Some(&scheme), vid, vid == leader_id);
        timed_ns_total += t0.elapsed().as_nanos();
        per_node_bytes.push(total_bytes(&ents));
    }

    let total_metronome: usize = per_node_bytes.iter().sum();
    let total_baseline = baseline_bytes_one_node * n;
    let cluster_reduction = 1.0 - (total_metronome as f64 / total_baseline as f64);
    let leader_bytes = per_node_bytes[0];
    // Average of the non-leader nodes = what we'd see in the
    // "follower_bytes" column of the etcd sweep CSV.
    let follower_avg: usize =
        per_node_bytes[1..].iter().sum::<usize>() / (n - 1).max(1);
    // Per-follower reduction (vs. baseline where each follower
    // would persist everything). This IS what the paper calls out
    // — each follower fsyncs only K/N of entries.
    let follower_reduction =
        1.0 - (follower_avg as f64 / baseline_bytes_one_node as f64);

    // Per-entry filter cost on one follower.
    let entries_filtered = ENTRIES_PER_RUN;
    let ns_per_entry = timed_ns_total / entries_filtered.max(1) as u128;

    println!(
        "| N={} K={} | val={:>5}B | baseline/node={:>11}B | follower(avg)={:>11}B | per-follower Δ={:>5.1}% | cluster-aggregate Δ={:>5.1}% | filter={:>3}ns/entry |",
        n, k, val_size, leader_bytes, follower_avg, follower_reduction * 100.0, cluster_reduction * 100.0, ns_per_entry,
    );
}

fn main() {
    println!("Metronome filter microbenchmark");
    println!(
        "ENTRIES_PER_RUN={} VALUE_SIZES={:?} CLUSTER_SIZES={:?}",
        ENTRIES_PER_RUN, VALUE_SIZES, CLUSTER_SIZES
    );
    println!();
    println!("Per-row: N nodes, K = f+1 persisters.");
    println!("  per-follower Δ  = fraction of baseline each follower no longer fsyncs.");
    println!("                    Theoretical target = (N - K) / N = {{33%, 40%, 43%}}.");
    println!("  cluster-aggregate Δ = reduction in cluster-wide WAL bytes. Lower because");
    println!("                        our impl has leaders persist everything for simpler");
    println!("                        recovery, so 1/N of the cluster is unaffected.");
    println!("                        Theoretical = (N-1)(N-K) / N^2 =");
    println!("                        {{22.2%, 32.0%, 36.7%}} for N={{3,5,7}}.");
    println!();
    for &n in CLUSTER_SIZES {
        for &val in VALUE_SIZES {
            bench_one(n, val);
        }
        println!();
    }
}
