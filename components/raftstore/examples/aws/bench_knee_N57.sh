#!/usr/bin/env bash
# Knee sweep for N ∈ {5, 7} at the two vals (1 MB, 64 KB) where N=3 showed
# clean ~1.5x wins. Theoretical ceilings:
#   N=5, K=3 → 5/3 ≈ 1.67x
#   N=7, K=4 → 7/4 = 1.75x
#
# Wraps bench_knee.sh once per (N, val). Per-set CSV stashed; all rows
# concatenated into combined-results.csv for plotting.
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

# N=5 — 5 TiKV nodes on disks 1..5, idle disks 6,7.
run_set 5 1048576 "1 2 4 8 16 32 64 128"   2000 200
run_set 5 65536   "2 4 8 16 32 64 128"     5000 500

# N=7 — 7 TiKV nodes, one per gp3 volume.
run_set 7 1048576 "1 2 4 8 16 32 64 128"   2000 200
run_set 7 65536   "2 4 8 16 32 64 128"     5000 500

echo
echo "===== combined N=5,7 knee results ====="
column -t -s, "$COMBINED"
echo
echo "CSV: $COMBINED"
echo "Per-set: $OUT_DIR/knee-N*.csv"
