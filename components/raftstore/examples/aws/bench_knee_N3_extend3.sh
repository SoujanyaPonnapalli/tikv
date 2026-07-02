#!/usr/bin/env bash
# Third N=3 extension at val=4 KB and 16 KB. Two previous extensions
# (up to t=16384) showed metronome creeping but never declining:
#   val=4K:  t=2048→22012 ... t=16384→22565  (+2.5% over 8x threads)
#   val=16K: t=2048→5331  ... t=16384→5519   (+3.5% over 8x threads)
#
# Push to t=32768 and t=65536 to either confirm the plateau decisively
# or find the cliff where TiKV's connection/queue limits cause drop.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
INNER="$SCRIPT_DIR/bench_knee.sh"
OUT_DIR="/data/disk1/bench-knee-N3-extend3"
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
  cp "$INNER_CSV" "$OUT_DIR/knee-N3-v${val}-extend3.csv"
}

run_set 4096    "32768 65536"    200000 5000
run_set 16384   "32768 65536"    80000  2000

echo
echo "===== third extension ====="
column -t -s, "$COMBINED"
