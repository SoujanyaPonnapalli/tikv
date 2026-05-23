#!/usr/bin/env bash
# Concurrency knee sweep at val=1 MB, N=3.
#
# Goal: locate the throughput plateau ("knee") for baseline vs metronome
# under disk-bandwidth pressure. At val=1 MB / 125 MiB/s gp3 cap:
#   - baseline ceiling      ~ 125 ops/s  (every node writes every op)
#   - metronome ceiling (K=2,N=3) ~ 187 ops/s  (each node writes 2/3 of ops)
# So the knee should sit around thread=8-16 for baseline and a bit higher
# for metronome, and the OPS plateau should differ by ~N/K = 1.5x.
#
# Same per-node, per-disk topology as bench.sh. Region splits disabled in
# the tomls so the whole run stays in one raft group.
set -uo pipefail

HOME_DIR="${HOME:-/home/ubuntu}"
TIDB_VER="${TIDB_VER:-v8.5.6}"

TIKV="$HOME_DIR/tikv/target/release/tikv-server"
PD="$HOME_DIR/.tiup/components/pd/${TIDB_VER}/pd-server"
YCSB="$HOME_DIR/go/bin/go-ycsb"
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
BASE_TOML="$SCRIPT_DIR/tikv-baseline.toml"
METR_TOML="$SCRIPT_DIR/tikv-metronome.toml"

BASE="/data/disk1/bench-knee"
RESULTS="$BASE/knee-results.csv"

for f in "$TIKV" "$PD" "$YCSB" "$BASE_TOML" "$METR_TOML"; do
  if [ ! -e "$f" ]; then echo "missing: $f" >&2; exit 1; fi
done

N="${N:-3}"
VAL="${VAL:-1048576}"     # 1 MB
THREADS_SWEEP=(${THREADS_SWEEP:-1 2 4 8 16 32 64 128})
MODES=(baseline metronome)
RECORDS="${RECORDS:-200}"
RUN_OPS="${RUN_OPS:-2000}"

pkill -9 -f "pd-server|tikv-server|go-ycsb" 2>/dev/null || true
sleep 2
mkdir -p "$BASE/logs"
echo "n,val_bytes,mode,threads,phase,count,takes_ms,avg_us,p50_us,p90_us,p99_us,ops" > "$RESULTS"

run_cell() {
  local n=$1 val=$2 mode=$3 threads=$4
  local TOML="$BASE_TOML"
  [[ "$mode" == "metronome" ]] && TOML="$METR_TOML"

  pkill -9 -f "pd-server|tikv-server" 2>/dev/null || true
  sleep 1

  local PD_DATA="$BASE/pd-n${n}-v${val}-${mode}-t${threads}"
  local LOGS="$BASE/logs/n${n}-v${val}-${mode}-t${threads}"
  rm -rf "$PD_DATA" "$LOGS"
  mkdir -p "$PD_DATA" "$LOGS"

  for i in $(seq 1 "$n"); do
    rm -rf "/data/disk$i/tikv"
    mkdir -p "/data/disk$i/tikv"
  done

  "$PD" --name=pd1 --data-dir="$PD_DATA" \
        --client-urls=http://127.0.0.1:2379 --peer-urls=http://127.0.0.1:2380 \
        --initial-cluster=pd1=http://127.0.0.1:2380 \
        --log-file="$LOGS/pd.log" > /dev/null 2>&1 &
  sleep 4

  for i in $(seq 1 "$n"); do
    local addr=$((20160 + i - 1)) status=$((20180 + i - 1))
    "$TIKV" --pd=127.0.0.1:2379 \
            --addr=127.0.0.1:$addr --advertise-addr=127.0.0.1:$addr \
            --status-addr=127.0.0.1:$status \
            --data-dir="/data/disk$i/tikv" \
            --config="$TOML" \
            --log-file="$LOGS/tikv$i.log" > /dev/null 2>&1 &
    sleep 4
  done
  sleep 6

  for _ in $(seq 1 30); do
    local up
    up=$(curl -s http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null \
        | grep -o '"state_name":"Up"' | wc -l | tr -d ' ')
    [[ "$up" -ge "$n" ]] && break
    sleep 1
  done

  local LOAD="$BASE/m-load-n${n}-v${val}-${mode}-t${threads}.txt"
  local RUN="$BASE/m-run-n${n}-v${val}-${mode}-t${threads}.txt"

  "$YCSB" load tikv \
    -p tikv.pd=127.0.0.1:2379 -p tikv.type=raw \
    -p recordcount=$RECORDS -p operationcount=$RECORDS \
    -p fieldcount=1 -p fieldlength=$val -p threadcount=$threads \
    > "$LOAD" 2>&1

  "$YCSB" run tikv \
    -p tikv.pd=127.0.0.1:2379 -p tikv.type=raw \
    -p recordcount=$RECORDS -p operationcount=$RUN_OPS \
    -p fieldcount=1 -p fieldlength=$val -p threadcount=$threads \
    -p readproportion=0.0 -p updateproportion=1.0 \
    > "$RUN" 2>&1

  parse() {
    local file=$1 phase=$2
    awk -v phase="$phase" '
      $0 ~ "^"phase" - " {
        for (i=1;i<=NF;i++) {
          if ($i=="Takes(s):")   {gsub(",","",$(i+1)); takes_ms=$(i+1)*1000}
          if ($i=="Count:")      {gsub(",","",$(i+1)); count=$(i+1)}
          if ($i=="OPS:")        {gsub(",","",$(i+1)); ops=$(i+1)}
          if ($i=="Avg(us):")    {gsub(",","",$(i+1)); avg=$(i+1)}
          if ($i=="50th(us):")   {gsub(",","",$(i+1)); p50=$(i+1)}
          if ($i=="90th(us):")   {gsub(",","",$(i+1)); p90=$(i+1)}
          if ($i=="99th(us):")   {gsub(",","",$(i+1)); p99=$(i+1)}
        }
        printf "%s,%.0f,%s,%s,%s,%s,%s,%s\n", count, takes_ms, avg, p50, p90, p99, ops, ""
        exit
      }' "$file"
  }
  local L R
  L=$(parse "$LOAD" INSERT)
  R=$(parse "$RUN" UPDATE)
  printf "%d,%d,%s,%d,LOAD,%s\n" "$n" "$val" "$mode" "$threads" "$L" >> "$RESULTS"
  printf "%d,%d,%s,%d,RUN,%s\n"  "$n" "$val" "$mode" "$threads" "$R" >> "$RESULTS"

  local skipped=0
  for i in $(seq 1 "$n"); do
    local metric_port=$((20180 + i - 1))
    local s
    s=$(curl -s --max-time 1 http://127.0.0.1:$metric_port/metrics 2>/dev/null \
        | awk '/^tikv_raftstore_metronome_entries_skipped_total/{print $2}' | head -1)
    [[ -n "$s" ]] && skipped=$(awk -v a="$skipped" -v b="$s" 'BEGIN{print a+b}')
  done
  printf "  [N=%d val=%d %-9s t=%-3d] LOAD: %s    RUN: %s    skipped=%s\n" \
         "$n" "$val" "$mode" "$threads" "$L" "$R" "$skipped"

  pkill -9 -f "pd-server|tikv-server" 2>/dev/null
  sleep 2
}

# Sweep both modes at each thread count so adjacent cells compare directly.
for threads in "${THREADS_SWEEP[@]}"; do
  for mode in "${MODES[@]}"; do
    run_cell "$N" "$VAL" "$mode" "$threads"
  done
done

echo
echo "===== knee results ====="
column -t -s, "$RESULTS"
echo
echo "CSV: $RESULTS"
