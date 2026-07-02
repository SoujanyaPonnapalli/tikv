#!/usr/bin/env bash
# Matrix bench: N ∈ {3,5,7} × val ∈ {256, 4096, 16384} × {baseline, metronome}
# Spins up a fresh PD + N tikv-servers per cell, runs go-ycsb load + run,
# tears down, prints a single results table.
set -uo pipefail

TIKV=/Users/soujanya/Projects/current/tikv/target/release/tikv-server
PD=/Users/soujanya/.tiup/components/pd/v8.5.6/pd-server
YCSB=/tmp/metronome-bench/go-ycsb/go-ycsb
BASE=/tmp/metronome-bench
RESULTS=$BASE/matrix-results.csv

NS=(3 5 7)
VALS=(256 4096 16384)
MODES=(baseline metronome)

# Reset.
pkill -9 -f "pd-server|tikv-server|go-ycsb" 2>/dev/null || true
sleep 2
echo "n,val_bytes,mode,phase,count,takes_ms,avg_us,p50_us,p90_us,p99_us,ops" > "$RESULTS"

run_cell() {
  local n=$1 val=$2 mode=$3
  local DATA=$BASE/data/n${n}-v${val}-${mode}
  local TOML=$BASE/tikv-base.toml
  [[ "$mode" == "metronome" ]] && TOML=$BASE/tikv-metronome.toml

  pkill -9 -f "pd-server|tikv-server" 2>/dev/null || true
  sleep 1
  rm -rf "$DATA"; mkdir -p "$DATA"/{pd,logs}
  for i in $(seq 1 "$n"); do mkdir -p "$DATA/tikv$i"; done

  # PD.
  $PD --name=pd1 --data-dir=$DATA/pd \
      --client-urls=http://127.0.0.1:2379 --peer-urls=http://127.0.0.1:2380 \
      --initial-cluster=pd1=http://127.0.0.1:2380 \
      --log-file=$DATA/logs/pd.log > /dev/null 2>&1 &
  sleep 4

  # Bring TiKVs up serially (avoids the bootstrap-conflict hang).
  for i in $(seq 1 "$n"); do
    local addr=$((20160 + i - 1)) status=$((20180 + i - 1))
    $TIKV --pd=127.0.0.1:2379 --addr=127.0.0.1:$addr --advertise-addr=127.0.0.1:$addr \
          --status-addr=127.0.0.1:$status --data-dir=$DATA/tikv$i --config=$TOML \
          --log-file=$DATA/logs/tikv$i.log > /dev/null 2>&1 &
    sleep 4
  done
  sleep 6

  # Wait until all stores are Up.
  for i in $(seq 1 30); do
    local up
    up=$(curl -s http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null \
        | grep -o '"state_name":"Up"' | wc -l | tr -d ' ')
    [[ "$up" -ge "$n" ]] && break
    sleep 1
  done

  # Workload.
  local LOAD=$BASE/m-load-n${n}-v${val}-${mode}.txt
  local RUN=$BASE/m-run-n${n}-v${val}-${mode}.txt

  $YCSB load tikv \
    -p tikv.pd=127.0.0.1:2379 -p tikv.type=raw \
    -p recordcount=5000 -p operationcount=5000 \
    -p fieldcount=1 -p fieldlength=$val -p threadcount=20 \
    > "$LOAD" 2>&1
  $YCSB run tikv \
    -p tikv.pd=127.0.0.1:2379 -p tikv.type=raw \
    -p recordcount=5000 -p operationcount=10000 \
    -p fieldcount=1 -p fieldlength=$val -p threadcount=20 \
    -p readproportion=0.0 -p updateproportion=1.0 \
    > "$RUN" 2>&1

  # Parse the YCSB summary lines (format: phase - Takes(s): T, Count: C, OPS: O,
  # Avg(us): A, ..., 50th(us): P50, 90th(us): P90, ..., 99th(us): P99, ...).
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
  printf "%d,%d,%s,LOAD,%s\n" "$n" "$val" "$mode" "$L" >> "$RESULTS"
  printf "%d,%d,%s,RUN,%s\n"  "$n" "$val" "$mode" "$R" >> "$RESULTS"

  # Stash metrons skipped count too (informational).
  local skipped=0
  for i in $(seq 1 "$n"); do
    local metric_port=$((20180 + i - 1))
    local s
    s=$(curl -s --max-time 1 http://127.0.0.1:$metric_port/metrics 2>/dev/null \
        | awk '/^tikv_raftstore_metronome_entries_skipped_total/{print $2}' | head -1)
    [[ -n "$s" ]] && skipped=$(awk -v a="$skipped" -v b="$s" 'BEGIN{print a+b}')
  done
  printf "  [N=%d val=%5d %-9s] LOAD: %s    RUN: %s    skipped=%s\n" \
         "$n" "$val" "$mode" "$L" "$R" "$skipped"

  pkill -9 -f "pd-server|tikv-server" 2>/dev/null
  sleep 2
}

for n in "${NS[@]}"; do
  for val in "${VALS[@]}"; do
    for mode in "${MODES[@]}"; do
      run_cell "$n" "$val" "$mode"
    done
  done
done

echo
echo "===== matrix results ====="
column -t -s, "$RESULTS"
