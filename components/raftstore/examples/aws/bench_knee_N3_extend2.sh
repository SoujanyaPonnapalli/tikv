#!/usr/bin/env bash
# Second N=3 extension at val=4 KB and 16 KB. The previous extension
# (threads up to 4096 for val=4K, up to 2048 for val=16K) didn't show
# metronome plateau cleanly — the curves were still trending. Pushing
# further to find the true saturation point.
#
# Combined with knee-multival + knee-N3-extend, this should give a
# continuous curve from t=16 up to t=16384 at val=4K.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
INNER="$SCRIPT_DIR/bench_knee.sh"
OUT_DIR="/data/disk1/bench-knee-N3-extend2"
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
  cp "$INNER_CSV" "$OUT_DIR/knee-N3-v${val}-extend2.csv"
}

# val=4 KB: previous extension hit t=4096 (metronome=22260, +1.1% over t=2048).
# Could be plateau or still trending. Push to 8192 and 16384 to settle it.
run_set 4096    "8192 16384"     200000 5000

# val=16 KB: previous hit t=2048 (metronome=5331, varied between 4935-5331).
# Continue to 4096, 8192, 16384.
run_set 16384   "4096 8192 16384"  80000  2000

echo
echo "===== second extension ====="
column -t -s, "$COMBINED"
echo "CSV: $COMBINED"
