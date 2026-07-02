#!/usr/bin/env bash
# Multi-val concurrency knee sweep. Wraps bench_knee.sh, driving it once per
# value size with a val-appropriate thread range. Produces:
#   - /data/disk1/bench-knee-multi/knee-v<val>.csv   (per-val)
#   - /data/disk1/bench-knee-multi/combined-results.csv  (all in one)
#
# Question this answers: does the ~1.5x metronome win we saw at val=1MB hold
# up at smaller value sizes, or does the bench drop out of the disk-bound
# regime and the win shrink?
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
INNER="$SCRIPT_DIR/bench_knee.sh"
OUT_DIR="/data/disk1/bench-knee-multi"
COMBINED="$OUT_DIR/combined-results.csv"
INNER_CSV="/data/disk1/bench-knee/knee-results.csv"

mkdir -p "$OUT_DIR"
echo "n,val_bytes,mode,threads,phase,count,takes_ms,avg_us,p50_us,p90_us,p99_us,ops" > "$COMBINED"

run_set() {
  local val=$1 threads=$2 ops=$3 records=$4
  echo "============================================================"
  echo "  val=$val  threads=[$threads]  RUN_OPS=$ops  RECORDS=$records"
  echo "============================================================"
  VAL=$val THREADS_SWEEP="$threads" RUN_OPS=$ops RECORDS=$records bash "$INNER"
  # Append data rows (skip header), and stash a per-val copy.
  tail -n +2 "$INNER_CSV" >> "$COMBINED"
  cp "$INNER_CSV" "$OUT_DIR/knee-v${val}.csv"
}

# val=4 KB : theoretical baseline cap 125MiB/s / 4KiB ~= 32k ops/s.
#            Lots of threads needed; CPU/network may cap us short of disk.
run_set 4096    "16 32 64 128 256 512"           20000 2000

# val=16 KB: theoretical cap ~8k ops/s.
run_set 16384   "8 16 32 64 128 256"             10000 1000

# val=64 KB: theoretical cap ~2k ops/s.
run_set 65536   "2 4 8 16 32 64 128"             5000  500

echo
echo "===== combined knee results ====="
column -t -s, "$COMBINED"
echo
echo "CSV: $COMBINED"
echo "Per-val: $OUT_DIR/knee-v*.csv"
