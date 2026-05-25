#!/usr/bin/env bash
# Follower catch-up benchmark: how long does a crashed TiKV follower take to
# rejoin the cluster after a SIGKILL+restart, vanilla TiKV vs Metronome?
#
# Theory: the leader sends all missed entries via MsgAppend; vanilla TiKV
# fsyncs every entry locally, while a Metronome follower filters through its
# persist set and only fsyncs K/N of them. At the disk-bandwidth bottleneck
# this should give Metronome ~N/K = 1.5x faster catch-up at N=3, K=2.
#
# Workflow per cell:
#   1. Bring up N=3 cluster (PD + 3 tikv-server).
#   2. Start a constant-rate go-ycsb UPDATE workload at 4 KB values.
#   3. Quiesce for `WARMUP_S` seconds so all replicas are caught up.
#   4. SIGKILL the LAST tikv-server (deterministic non-leader pick).
#   5. Let the workload keep running for `DOWNTIME_S` seconds.
#   6. Snapshot the leader's last_index — this is the catch-up target.
#   7. Restart the killed follower.
#   8. Poll the killed follower's last_index every 100 ms, plus the per-peer
#      raft_log_lag exposed at the leader's /metrics endpoint, until
#      last_index >= target. Record:
#        - time_to_catchup_s (s)
#        - apply_lag_avg / apply_lag_max during the catch-up window
#        - bytes_fsynced (delta from /proc/diskstats on the follower's disk)
#   9. Stop workload, tear down cluster.
#
# Sweep DOWNTIME_S = {10, 30, 60, 120, 300} for each of baseline / metronome.
set -uo pipefail

HOME_DIR="${HOME:-/home/ubuntu}"
TIDB_VER="${TIDB_VER:-v8.5.6}"

TIKV="$HOME_DIR/tikv/target/release/tikv-server"
PD="$HOME_DIR/.tiup/components/pd/${TIDB_VER}/pd-server"
YCSB="$HOME_DIR/go/bin/go-ycsb"
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
BASE_TOML="$SCRIPT_DIR/tikv-baseline.toml"
METR_TOML="$SCRIPT_DIR/tikv-metronome.toml"

BASE="/data/disk1/bench-recovery"
RESULTS="$BASE/recovery-results.csv"
N=3
VAL="${VAL:-4096}"
RECORDS="${RECORDS:-5000}"
WORKLOAD_THREADS="${WORKLOAD_THREADS:-128}"     # foreground write concurrency
WORKLOAD_OPS="${WORKLOAD_OPS:-10000000}"        # cap; cell stops well before
WARMUP_S="${WARMUP_S:-15}"
DOWNTIMES=(${DOWNTIMES:-10 30 60 120 300})
MODES=(baseline metronome)
POLL_MS=100
POLL_TIMEOUT_S=600

for f in "$TIKV" "$PD" "$YCSB" "$BASE_TOML" "$METR_TOML"; do
  if [ ! -e "$f" ]; then echo "missing: $f" >&2; exit 1; fi
done

pkill -9 -f "pd-server|tikv-server|go-ycsb" 2>/dev/null || true
sleep 2
mkdir -p "$BASE/logs"
echo "mode,downtime_s,target_last_index,start_last_index,entries_to_catchup,time_to_catchup_s,apply_lag_avg,apply_lag_max,disk_bytes_written,disk_mb_per_s" > "$RESULTS"

scrape_last_index() {
  # Args: status_port. Returns leader's view of last_index for region 2 (the
  # one our raw KV ops land on after PD bootstrap; region 2 stays unsplit
  # because region-split-size is huge in our tomls).
  local port=$1
  curl -s --max-time 1 "http://127.0.0.1:$port/metrics" 2>/dev/null \
    | awk '/^tikv_raftstore_raft_log_last_index{/{
        last=$2
      }
      END { print last+0 }'
}

scrape_apply_lag() {
  # Args: leader_status_port follower_store_id.
  # Returns the raft_log_lag (gap between leader and follower) for that
  # follower as seen from the leader's prometheus.
  local port=$1 store=$2
  curl -s --max-time 1 "http://127.0.0.1:$port/metrics" 2>/dev/null \
    | awk -v store="$store" '
        $0 ~ "^tikv_raftstore_raft_log_lag{" && $0 ~ "store_id=\""store"\"" {
          val=$NF
        }
        END { print val+0 }'
}

bytes_written() {
  # Args: device basename (e.g., nvme3n1). Returns total sectors written from
  # /proc/diskstats. Sector = 512B.
  local dev=$1
  awk -v d="$dev" '$3==d { print $10 }' /proc/diskstats
}

run_cell() {
  local mode=$1 downtime=$2
  local TOML="$BASE_TOML"
  [[ "$mode" == "metronome" ]] && TOML="$METR_TOML"

  pkill -9 -f "pd-server|tikv-server|go-ycsb" 2>/dev/null || true
  sleep 2
  local PD_DATA="$BASE/pd-${mode}-d${downtime}"
  local LOGS="$BASE/logs/${mode}-d${downtime}"
  rm -rf "$PD_DATA" "$LOGS"
  mkdir -p "$PD_DATA" "$LOGS"
  for i in $(seq 1 "$N"); do rm -rf "/data/disk$i/tikv" && mkdir -p "/data/disk$i/tikv"; done

  "$PD" --name=pd1 --data-dir="$PD_DATA" \
        --client-urls=http://127.0.0.1:2379 --peer-urls=http://127.0.0.1:2380 \
        --initial-cluster=pd1=http://127.0.0.1:2380 \
        --log-file="$LOGS/pd.log" > /dev/null 2>&1 &
  sleep 4
  for i in $(seq 1 "$N"); do
    local addr=$((20160 + i - 1)) status=$((20180 + i - 1))
    "$TIKV" --pd=127.0.0.1:2379 \
            --addr=127.0.0.1:$addr --advertise-addr=127.0.0.1:$addr \
            --status-addr=127.0.0.1:$status \
            --data-dir="/data/disk$i/tikv" --config="$TOML" \
            --log-file="$LOGS/tikv$i.log" > /dev/null 2>&1 &
    sleep 4
  done
  sleep 6

  # Wait stores Up
  for _ in $(seq 1 30); do
    local up
    up=$(curl -s http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null \
        | grep -o '"state_name":"Up"' | wc -l | tr -d ' ')
    [[ "$up" -ge "$N" ]] && break
    sleep 1
  done

  # Pre-load.
  "$YCSB" load tikv \
    -p tikv.pd=127.0.0.1:2379 -p tikv.type=raw \
    -p recordcount=$RECORDS -p operationcount=$RECORDS \
    -p fieldcount=1 -p fieldlength=$VAL -p threadcount=$WORKLOAD_THREADS \
    > "$BASE/load-${mode}-d${downtime}.txt" 2>&1

  # Start sustained UPDATE workload in background; cell will SIGTERM it later.
  "$YCSB" run tikv \
    -p tikv.pd=127.0.0.1:2379 -p tikv.type=raw \
    -p recordcount=$RECORDS -p operationcount=$WORKLOAD_OPS \
    -p fieldcount=1 -p fieldlength=$VAL -p threadcount=$WORKLOAD_THREADS \
    -p readproportion=0.0 -p updateproportion=1.0 \
    > "$BASE/run-${mode}-d${downtime}.txt" 2>&1 &
  local YCSB_PID=$!

  echo "  [mode=$mode D=${downtime}s] cluster up, workload pid $YCSB_PID — warmup ${WARMUP_S}s..."
  sleep "$WARMUP_S"

  # Identify leader. PD api gives leader peer id per region; we use region 2
  # which is the default region holding raw KV after bootstrap. Map peer id
  # back to store id, then to status port (store $i → port 20180+i-1).
  local victim_idx=3                # always tikv-3 = store 3 → port 20182
  local leader_idx
  leader_idx=$(curl -s http://127.0.0.1:2379/pd/api/v1/regions 2>/dev/null \
    | python3 -c '
import sys, json
data = json.load(sys.stdin)
for r in data.get("regions", []):
  ld = r.get("leader") or {}
  sid = ld.get("store_id")
  if sid:
    print(sid)
    sys.exit(0)
' 2>/dev/null || echo 1)
  if [ "$leader_idx" = "$victim_idx" ]; then
    # Leader happened to land on victim store; swap victim to store 2.
    victim_idx=2
  fi
  local leader_status=$((20180 + leader_idx - 1))
  local victim_status=$((20180 + victim_idx - 1))
  echo "  [mode=$mode D=${downtime}s] leader=store$leader_idx victim=store$victim_idx"

  # Find the victim's data device (nvme*) for bytes-written tracking.
  local victim_dev
  victim_dev=$(df --output=source "/data/disk$victim_idx" | tail -1 | sed 's|/dev/||')
  local bw_before
  bw_before=$(bytes_written "$victim_dev")

  # SIGKILL victim.
  local victim_pid
  victim_pid=$(pgrep -f "addr=127.0.0.1:$((20160 + victim_idx - 1)) ")
  echo "  [mode=$mode D=${downtime}s] SIGKILL victim pid=$victim_pid"
  kill -9 "$victim_pid" 2>/dev/null
  local kill_ts=$(date +%s.%N)

  # Sleep downtime.
  sleep "$downtime"

  # Snapshot target.
  local target_idx
  target_idx=$(scrape_last_index "$leader_status")
  echo "  [mode=$mode D=${downtime}s] downtime over, leader last_index=$target_idx"

  # Restart victim.
  local addr=$((20160 + victim_idx - 1)) status_p=$victim_status
  "$TIKV" --pd=127.0.0.1:2379 \
          --addr=127.0.0.1:$addr --advertise-addr=127.0.0.1:$addr \
          --status-addr=$status_p \
          --data-dir="/data/disk$victim_idx/tikv" --config="$TOML" \
          --log-file="$LOGS/tikv${victim_idx}-restart.log" > /dev/null 2>&1 &
  local restart_pid=$!
  local restart_ts=$(date +%s.%N)

  # Wait for victim's /metrics to come up.
  for _ in $(seq 1 60); do
    if curl -s --max-time 1 "http://127.0.0.1:$victim_status/metrics" >/dev/null 2>&1; then break; fi
    sleep 0.5
  done

  # Poll loop: every POLL_MS, scrape victim's last_index and leader's view of
  # the lag. Stop when victim's last_index >= target_idx.
  local elapsed_ms=0
  local apply_lag_sum=0
  local apply_lag_n=0
  local apply_lag_max=0
  while [ "$elapsed_ms" -lt $((POLL_TIMEOUT_S * 1000)) ]; do
    local victim_idx_li
    victim_idx_li=$(scrape_last_index "$victim_status")
    if [ -n "$victim_idx_li" ] && [ "$victim_idx_li" -ge "$target_idx" ]; then
      break
    fi
    local lag
    lag=$(scrape_apply_lag "$leader_status" "$victim_idx")
    if [ -n "$lag" ] && [ "$lag" != "0" ]; then
      apply_lag_sum=$(awk -v a="$apply_lag_sum" -v b="$lag" 'BEGIN{print a+b}')
      apply_lag_n=$((apply_lag_n + 1))
      if awk -v a="$lag" -v b="$apply_lag_max" 'BEGIN{exit !(a>b)}'; then
        apply_lag_max=$lag
      fi
    fi
    sleep 0.1
    elapsed_ms=$((elapsed_ms + POLL_MS))
  done
  local caught_up_ts=$(date +%s.%N)
  local catchup_s
  catchup_s=$(awk -v a="$caught_up_ts" -v b="$restart_ts" 'BEGIN{printf "%.3f", a-b}')

  local bw_after
  bw_after=$(bytes_written "$victim_dev")
  local bw_delta=$(( (bw_after - bw_before) * 512 ))    # bytes
  local bw_mb=$(awk -v b="$bw_delta" 'BEGIN{printf "%.1f", b/1048576}')
  local bw_rate=$(awk -v b="$bw_delta" -v t="$catchup_s" 'BEGIN{ if (t>0) printf "%.1f", (b/1048576)/t; else print "0" }')

  local apply_lag_avg=0
  if [ "$apply_lag_n" -gt 0 ]; then
    apply_lag_avg=$(awk -v s="$apply_lag_sum" -v n="$apply_lag_n" 'BEGIN{printf "%.1f", s/n}')
  fi

  local start_idx
  start_idx=$((target_idx - apply_lag_max))
  local entries_to_catchup
  entries_to_catchup=$((target_idx - start_idx))

  printf "%s,%d,%d,%d,%d,%.3f,%s,%s,%d,%s\n" \
         "$mode" "$downtime" "$target_idx" "$start_idx" "$entries_to_catchup" \
         "$catchup_s" "$apply_lag_avg" "$apply_lag_max" \
         "$bw_delta" "$bw_rate" >> "$RESULTS"

  echo "  [mode=$mode D=${downtime}s] caught up in ${catchup_s}s | disk_MB=${bw_mb} | apply_lag avg=${apply_lag_avg} max=${apply_lag_max}"

  # Stop workload + cluster.
  kill -TERM "$YCSB_PID" 2>/dev/null || true
  sleep 2
  pkill -9 -f "go-ycsb" 2>/dev/null || true
  pkill -9 -f "pd-server|tikv-server" 2>/dev/null
  sleep 2
}

for d in "${DOWNTIMES[@]}"; do
  for mode in "${MODES[@]}"; do
    run_cell "$mode" "$d"
  done
done

echo
echo "===== recovery results ====="
column -t -s, "$RESULTS"
echo
echo "CSV: $RESULTS"
