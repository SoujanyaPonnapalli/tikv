#!/usr/bin/env bash
# Disk-bandwidth-bound matrix bench on AWS:
#   N in {3,5,7} x val in {256, 1024, 4096} x {baseline, metronome}
#
# Cluster layout per cell:
#   - 1 EC2 host
#   - 1 PD (control plane only, ignored for bandwidth math)
#   - N tikv-servers, each with its data-dir on its OWN gp3 EBS volume
#     (/data/disk1 .. /data/diskN). Per-volume throughput cap = 125 MiB/s.
#
# Why this is disk-bandwidth bound (and the phase-7 localhost run wasn't):
# every tikv-server in this cell is pinned to a single gp3 volume whose
# write throughput is capped at 125 MiB/s. Under sustained 200-client
# write load with non-trivial value sizes, the raft-log fsync path
# saturates that ceiling well before the network (loopback) or CPU does.
# Metronome cuts the per-follower persist work to K/N of total entries,
# so the K/N byte savings should show up as roughly N/K throughput / latency
# wins (e.g. 5/3x at N=5,K=3).
set -uo pipefail

HOME_DIR="${HOME:-/home/ubuntu}"
TIDB_VER="${TIDB_VER:-v8.5.6}"

TIKV="$HOME_DIR/tikv/target/release/tikv-server"
PD="$HOME_DIR/.tiup/components/pd/${TIDB_VER}/pd-server"
YCSB="$HOME_DIR/go/bin/go-ycsb"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_TOML="$SCRIPT_DIR/tikv-baseline.toml"
METR_TOML="$SCRIPT_DIR/tikv-metronome.toml"

BASE="/data/disk1/bench"
RESULTS="$BASE/matrix-results.csv"

# Sanity-check the prereqs.
for f in "$TIKV" "$PD" "$YCSB" "$BASE_TOML" "$METR_TOML"; do
  if [ ! -e "$f" ]; then
    echo "missing: $f" >&2
    echo "Did setup.sh finish? Look at /var/log/user-data.log and rerun setup.sh." >&2
    exit 1
  fi
done

# Sanity-check the data volumes.
for i in 1 2 3 4 5 6 7; do
  if ! mountpoint -q "/data/disk$i"; then
    echo "missing mount: /data/disk$i" >&2
    echo "Check user_data.sh ran successfully: tail /var/log/user-data.log" >&2
    exit 1
  fi
done

NS=(3 5 7)
VALS=(256 1024 4096)
MODES=(baseline metronome)

THREADS="${THREADS:-200}"
RECORDS="${RECORDS:-10000}"
RUN_OPS="${RUN_OPS:-100000}"

# Reset.
pkill -9 -f "pd-server|tikv-server|go-ycsb" 2>/dev/null || true
sleep 2
mkdir -p "$BASE/logs"
echo "n,val_bytes,mode,phase,count,takes_ms,avg_us,p50_us,p90_us,p99_us,ops" > "$RESULTS"

run_cell() {
  local n=$1 val=$2 mode=$3
  local TOML="$BASE_TOML"
  [[ "$mode" == "metronome" ]] && TOML="$METR_TOML"

  pkill -9 -f "pd-server|tikv-server" 2>/dev/null || true
  sleep 1

  local PD_DATA="$BASE/pd-n${n}-v${val}-${mode}"
  local LOGS="$BASE/logs/n${n}-v${val}-${mode}"
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

  # Bring TiKVs up serially (matches phase-7 pattern — parallel start hits a
  # bootstrap-conflict hang).
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

  # Wait until all N stores are Up.
  for _ in $(seq 1 30); do
    local up
    up=$(curl -s http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null \
        | grep -o '"state_name":"Up"' | wc -l | tr -d ' ')
    [[ "$up" -ge "$n" ]] && break
    sleep 1
  done

  local LOAD="$BASE/m-load-n${n}-v${val}-${mode}.txt"
  local RUN="$BASE/m-run-n${n}-v${val}-${mode}.txt"

  "$YCSB" load tikv \
    -p tikv.pd=127.0.0.1:2379 -p tikv.type=raw \
    -p recordcount=$RECORDS -p operationcount=$RECORDS \
    -p fieldcount=1 -p fieldlength=$val -p threadcount=$THREADS \
    > "$LOAD" 2>&1

  "$YCSB" run tikv \
    -p tikv.pd=127.0.0.1:2379 -p tikv.type=raw \
    -p recordcount=$RECORDS -p operationcount=$RUN_OPS \
    -p fieldcount=1 -p fieldlength=$val -p threadcount=$THREADS \
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
  printf "%d,%d,%s,LOAD,%s\n" "$n" "$val" "$mode" "$L" >> "$RESULTS"
  printf "%d,%d,%s,RUN,%s\n"  "$n" "$val" "$mode" "$R" >> "$RESULTS"

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
echo
echo "CSV: $RESULTS"
echo "scp it back with: scp ubuntu@<host>:$RESULTS ."
