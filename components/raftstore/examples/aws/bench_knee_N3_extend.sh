#!/usr/bin/env bash
# Extended N=3 knee sweep at val=4 KB and 16 KB.
#
# Why: the original bench_knee_vals.sh didn't reach saturation at small val.
# At val=4 KB the baseline was still climbing at t=512 (24,937 ops/s);
# at val=16 KB still climbing at t=256 (5,810 ops/s). We need to push
# concurrency higher to find the true disk-bound plateau so we can compare
# baseline-cap vs metronome-cap cleanly.
#
# Sweep ranges deliberately overlap the previous run's tail (256, 128 etc.)
# so we can stitch the curves together.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
INNER="$SCRIPT_DIR/bench_knee.sh"
OUT_DIR="/data/disk1/bench-knee-N3-extend"
COMBINED="$OUT_DIR/combined-results.csv"
INNER_CSV="/data/disk1/bench-knee/knee-results.csv"

mkdir -p "$OUT_DIR"
echo "n,val_bytes,mode,threads,phase,count,takes_ms,avg_us,p50_us,p90_us,p99_us,ops" > "$COMBINED"

run_set() {
  local val=$1 threads=$2 ops=$3 records=$4
  echo "============================================================"
  echo "  N=3  val=$val  threads=[$threads]  RUN_OPS=$ops"
  echo "============================================================"
  N=3 VAL=$val THREADS_SWEEP="$threads" RUN_OPS=$ops RECORDS=$records bash "$INNER"
  tail -n +2 "$INNER_CSV" >> "$COMBINED"
  cp "$INNER_CSV" "$OUT_DIR/knee-N3-v${val}-extend.csv"
}

# val=4 KB: previous run at threads=[16..512] showed baseline still climbing
# (24,937 ops/s at t=512). Skip threads we already have; push much higher.
run_set 4096    "1024 2048 4096"             200000 5000

# val=16 KB: previous run at threads=[8..256] showed baseline still climbing
# (5,810 ops/s at t=256). Skip threads we already have; push higher.
run_set 16384   "512 1024 2048"              80000  2000

echo
echo "===== extended N=3 knee results ====="
column -t -s, "$COMBINED"
echo
echo "CSV: $COMBINED"
