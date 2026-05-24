#!/usr/bin/env bash
# Knee sweep for N ∈ {5, 7} across all four vals where N=3 showed wins.
#
# Theoretical wins (N/K with K = f+1):
#   N=5, K=3 → 5/3 ≈ 1.67x
#   N=7, K=4 → 7/4 = 1.75x
#
# Thread ranges bracket the N=3 metronome saturation point for each val,
# with extra headroom in case N=5/7 saturate at slightly different concurrency.
# Methodology: find metronome's saturation thread count, measure baseline
# at that load, report the gain.
#
# N=3 knees observed (for reference):
#   val=1MB  → t=4    (metronome ~70 ops/s)
#   val=64KB → t=64   (metronome ~1700 ops/s)
#   val=16KB → t=2048 (metronome ~5300 ops/s)
#   val=4KB  → t=2048 (metronome ~22000 ops/s)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
INNER="$SCRIPT_DIR/bench_knee.sh"
OUT_DIR="/data/disk1/bench-knee-N57"
COMBINED="$OUT_DIR/combined-results.csv"
INNER_CSV="/data/disk1/bench-knee/knee-results.csv"

mkdir -p "$OUT_DIR"
echo "n,val_bytes,mode,threads,phase,count,takes_ms,avg_us,p50_us,p90_us,p99_us,ops" > "$COMBINED"

run_set() {
  local n=$1 val=$2 threads=$3 ops=$4 records=$5
  echo "============================================================"
  echo "  N=$n  val=$val  threads=[$threads]  RUN_OPS=$ops"
  echo "============================================================"
  N=$n VAL=$val THREADS_SWEEP="$threads" RUN_OPS=$ops RECORDS=$records bash "$INNER"
  tail -n +2 "$INNER_CSV" >> "$COMBINED"
  cp "$INNER_CSV" "$OUT_DIR/knee-N${n}-v${val}.csv"
}

# N=5 — 5 TiKV nodes on disks 1..5; disks 6,7 idle.
run_set 5 1048576 "2 4 8 16 32"             2000   200
run_set 5 65536   "16 32 64 128 256"        5000   500
run_set 5 16384   "256 1024 2048 4096"      80000  2000
run_set 5 4096    "512 1024 2048 4096"      200000 5000

# N=7 — 7 TiKV nodes, one per gp3 volume.
run_set 7 1048576 "2 4 8 16 32"             2000   200
run_set 7 65536   "16 32 64 128 256"        5000   500
run_set 7 16384   "256 1024 2048 4096"      80000  2000
run_set 7 4096    "512 1024 2048 4096"      200000 5000

echo
echo "===== combined N=5,7 knee results ====="
column -t -s, "$COMBINED"
echo
echo "CSV: $COMBINED"
echo "Per-set: $OUT_DIR/knee-N*.csv"
