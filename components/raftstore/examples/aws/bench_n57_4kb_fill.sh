#!/usr/bin/env bash
# Gap-fill: N=5 and N=7 at val=4 KB for the thread counts that ARE present
# in the N=3 sweep but missing in knee-N57-ebs125.csv.
#
# Existing for N=5,7 val=4096: [512, 1024, 2048, 4096]
# Existing for N=3 val=4096:   [16, 32, 64, 128, 256, 512, 1024, 2048, 4096,
#                               8192, 16384, 32768, 65536]
# Missing for N=5,7:           [16, 32, 64, 128, 256, 8192, 16384, 32768, 65536]
#
# Same per-cell shape as bench_knee.sh; just a smaller, focused matrix.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
INNER="$SCRIPT_DIR/bench_knee.sh"
OUT_DIR="/data/disk1/bench-n57-4kb-fill"
COMBINED="$OUT_DIR/combined-results.csv"
INNER_CSV="/data/disk1/bench-knee/knee-results.csv"

mkdir -p "$OUT_DIR"
echo "n,val_bytes,mode,threads,phase,count,takes_ms,avg_us,p50_us,p90_us,p99_us,ops" > "$COMBINED"

run_set() {
  local n=$1 threads=$2 ops=$3 records=$4
  echo "============================================================"
  echo "  N=$n  val=4096  threads=[$threads]  RUN_OPS=$ops"
  echo "============================================================"
  N=$n VAL=4096 THREADS_SWEEP="$threads" RUN_OPS=$ops RECORDS=$records bash "$INNER"
  tail -n +2 "$INNER_CSV" >> "$COMBINED"
  cp "$INNER_CSV" "$OUT_DIR/knee-N${n}-v4096-fill.csv"
}

# Low-concurrency cells (fast workload, modest ops count).
run_set 5 "16 32 64 128 256"          50000  2000
run_set 7 "16 32 64 128 256"          50000  2000

# High-concurrency cells (need large ops to ride out warmup at high t).
run_set 5 "8192 16384 32768 65536"    200000 5000
run_set 7 "8192 16384 32768 65536"    200000 5000

echo
echo "===== combined N=5,7 val=4 KB fill ====="
column -t -s, "$COMBINED"
echo
echo "CSV: $COMBINED"
