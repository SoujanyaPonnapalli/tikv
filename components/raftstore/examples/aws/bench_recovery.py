#!/usr/bin/env python3
"""Follower catch-up benchmark for TiKV vs Metronome.

Per cell:
  1. Spin up PD + N=3 tikv-servers (each on its own /data/disk{i}, fresh).
  2. go-ycsb load + sustained UPDATE workload at 4 KB values.
  3. Warmup, then SIGKILL a non-leader follower.
  4. Wait `downtime` seconds while writes continue at K=2 quorum.
  5. Restart the follower.
  6. Poll the leader's tikv_raftstore_log_lag (sum/count → interval avg) every
     250 ms until 3 consecutive intervals show delta_avg < `LAG_THRESHOLD`.
     Record time-to-catchup, peak/final apply lag, disk bytes written on the
     victim's volume.

Sweeps DOWNTIMES across baseline and metronome. Outputs CSV.

Discovery used at design time:
  - PD `/pd/api/v1/store/{id}` exposes status_address (so we can map id→port).
  - Stores get non-sequential ids: 1, 12, 13 in our 3-node bootstrap.
  - `tikv_raftstore_log_lag_{sum,count}` is exposed only on the LEADER;
    average lag = sum/count. We compute interval delta_avg per poll.
"""
import argparse
import json
import os
import pathlib
import re
import shlex
import signal
import subprocess
import sys
import time
import urllib.request
from typing import Dict, List, Optional, Tuple

HOME = pathlib.Path(os.environ.get("HOME", "/home/ubuntu"))
TIDB_VER = os.environ.get("TIDB_VER", "v8.5.6")
TIKV = HOME / "tikv/target/release/tikv-server"
PD = HOME / f".tiup/components/pd/{TIDB_VER}/pd-server"
YCSB = HOME / "go/bin/go-ycsb"
SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
BASE_TOML = SCRIPT_DIR / "tikv-baseline.toml"
METR_TOML = SCRIPT_DIR / "tikv-metronome.toml"

OUT_DIR = pathlib.Path("/data/disk1/bench-recovery")
RESULTS_CSV = OUT_DIR / "recovery-results.csv"

N = 3
VAL = 4096
RECORDS = 5000
WORKLOAD_THREADS = 128
WORKLOAD_OPS = 10_000_000
WARMUP_S = 15
POLL_S = 0.25
LAG_THRESHOLD = 5.0          # interval avg lag (entries) considered "caught up"
CAUGHT_UP_HITS = 3           # consecutive intervals below threshold
POLL_TIMEOUT_S = 600


def sh(cmd: str) -> str:
    return subprocess.check_output(shlex.split(cmd), text=True, stderr=subprocess.DEVNULL).strip()


def http_get(url: str, timeout: float = 1.0) -> Optional[str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.read().decode()
    except Exception:
        return None


def pd_json(path: str) -> Optional[dict]:
    body = http_get(f"http://127.0.0.1:2379{path}")
    if body is None:
        return None
    try:
        return json.loads(body)
    except Exception:
        return None


def wait_stores_up(n: int, timeout_s: int = 60) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        d = pd_json("/pd/api/v1/stores")
        if d and len(d.get("stores", [])) >= n \
                and all(s["store"]["state_name"] == "Up" for s in d["stores"]):
            return True
        time.sleep(1)
    return False


def get_stores() -> List[dict]:
    d = pd_json("/pd/api/v1/stores")
    return [s["store"] for s in (d or {}).get("stores", [])]


def status_port_for(store: dict) -> int:
    # status_address: "127.0.0.1:20180" → 20180
    return int(store["status_address"].rsplit(":", 1)[1])


def addr_port_for(store: dict) -> int:
    return int(store["address"].rsplit(":", 1)[1])


def get_leader_store_id(region_id: int = 2) -> Optional[int]:
    d = pd_json(f"/pd/api/v1/region/id/{region_id}")
    if not d:
        return None
    return d.get("leader", {}).get("store_id")


_LAG_SUM_RE = re.compile(r"^tikv_raftstore_log_lag_sum\s+([\d\.eE+-]+)$", re.M)
_LAG_COUNT_RE = re.compile(r"^tikv_raftstore_log_lag_count\s+([\d\.eE+-]+)$", re.M)


def scrape_lag(status_port: int) -> Optional[Tuple[float, float]]:
    body = http_get(f"http://127.0.0.1:{status_port}/metrics", timeout=1.0)
    if not body:
        return None
    m_s = _LAG_SUM_RE.search(body)
    m_c = _LAG_COUNT_RE.search(body)
    if not m_s or not m_c:
        return None
    return float(m_s.group(1)), float(m_c.group(1))


def bytes_written(dev_basename: str) -> int:
    # /proc/diskstats field 10 = sectors_written (each sector = 512 bytes).
    with open("/proc/diskstats") as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 11 and parts[2] == dev_basename:
                return int(parts[9]) * 512
    return 0


def dev_for(path: str) -> str:
    out = subprocess.check_output(["df", "--output=source", path], text=True).strip().splitlines()
    src = out[-1]
    return src.split("/")[-1]


def spawn(cmd: List[str], stdout_path: pathlib.Path) -> subprocess.Popen:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    f = open(stdout_path, "ab")
    return subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT,
                            stdin=subprocess.DEVNULL, start_new_session=True)


def killall(patterns: List[str]) -> None:
    # `pkill -9 -f pat` for each; ignore errors.
    for pat in patterns:
        subprocess.run(["pkill", "-9", "-f", pat],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def find_pid_for_addr(addr_port: int) -> Optional[int]:
    try:
        out = subprocess.check_output(
            ["pgrep", "-f", f"addr=127.0.0.1:{addr_port} "], text=True
        ).strip()
    except subprocess.CalledProcessError:
        return None
    pids = [int(x) for x in out.splitlines() if x.strip()]
    return pids[0] if pids else None


def run_cell(mode: str, downtime: int, logs_root: pathlib.Path) -> Dict:
    toml = BASE_TOML if mode == "baseline" else METR_TOML
    cell_dir = logs_root / f"{mode}-d{downtime}"
    cell_dir.mkdir(parents=True, exist_ok=True)

    killall(["pd-server", "tikv-server", "go-ycsb"])
    time.sleep(2)

    # Wipe each /data/disk{i}/tikv (we have at least 3 mounted).
    for i in range(1, N + 1):
        subprocess.run(["rm", "-rf", f"/data/disk{i}/tikv"], check=False)
        subprocess.run(["mkdir", "-p", f"/data/disk{i}/tikv"], check=False)
    pd_data = OUT_DIR / f"pd-{mode}-d{downtime}"
    subprocess.run(["rm", "-rf", str(pd_data)], check=False)
    pd_data.mkdir(parents=True, exist_ok=True)

    # PD.
    pd_proc = spawn(
        [str(PD), "--name=pd1", f"--data-dir={pd_data}",
         "--client-urls=http://127.0.0.1:2379",
         "--peer-urls=http://127.0.0.1:2380",
         "--initial-cluster=pd1=http://127.0.0.1:2380",
         f"--log-file={cell_dir / 'pd.log'}"],
        cell_dir / "pd.out")
    time.sleep(4)

    # N tikv-servers.
    tikv_procs = []
    for i in range(1, N + 1):
        addr = 20160 + i - 1
        status = 20180 + i - 1
        p = spawn(
            [str(TIKV), "--pd=127.0.0.1:2379",
             f"--addr=127.0.0.1:{addr}",
             f"--advertise-addr=127.0.0.1:{addr}",
             f"--status-addr=127.0.0.1:{status}",
             f"--data-dir=/data/disk{i}/tikv",
             f"--config={toml}",
             f"--log-file={cell_dir / f'tikv{i}.log'}"],
            cell_dir / f"tikv{i}.out")
        tikv_procs.append((i, addr, status, p))
        time.sleep(4)
    time.sleep(6)

    if not wait_stores_up(N, timeout_s=60):
        raise RuntimeError("stores never came Up")

    # Pre-load.
    print(f"  [{mode} d={downtime}] load phase")
    subprocess.run(
        [str(YCSB), "load", "tikv",
         "-p", "tikv.pd=127.0.0.1:2379", "-p", "tikv.type=raw",
         "-p", f"recordcount={RECORDS}", "-p", f"operationcount={RECORDS}",
         "-p", "fieldcount=1", "-p", f"fieldlength={VAL}",
         "-p", f"threadcount={WORKLOAD_THREADS}"],
        stdout=open(cell_dir / "load.out", "wb"), stderr=subprocess.STDOUT,
        check=False)

    # Start sustained UPDATE workload.
    print(f"  [{mode} d={downtime}] starting sustained UPDATE workload")
    workload = spawn(
        [str(YCSB), "run", "tikv",
         "-p", "tikv.pd=127.0.0.1:2379", "-p", "tikv.type=raw",
         "-p", f"recordcount={RECORDS}", "-p", f"operationcount={WORKLOAD_OPS}",
         "-p", "fieldcount=1", "-p", f"fieldlength={VAL}",
         "-p", f"threadcount={WORKLOAD_THREADS}",
         "-p", "readproportion=0.0", "-p", "updateproportion=1.0"],
        cell_dir / "run.out")

    time.sleep(WARMUP_S)

    # Identify leader and pick victim.
    leader_id = get_leader_store_id(2)
    if leader_id is None:
        raise RuntimeError("could not get leader_id")
    stores = get_stores()
    by_id = {s["id"]: s for s in stores}
    follower_ids = [sid for sid in by_id if sid != leader_id]
    victim_id = sorted(follower_ids)[0]
    leader_status = status_port_for(by_id[leader_id])
    victim_addr = addr_port_for(by_id[victim_id])
    victim_dir_idx = victim_addr - 20160 + 1  # /data/disk{idx}/tikv

    print(f"  [{mode} d={downtime}] leader=store{leader_id}@:{leader_status}, "
          f"victim=store{victim_id}@addr:{victim_addr} dir=/data/disk{victim_dir_idx}")

    victim_dev = dev_for(f"/data/disk{victim_dir_idx}")
    bw_before = bytes_written(victim_dev)

    # SIGKILL victim.
    pid = find_pid_for_addr(victim_addr)
    if pid is None:
        raise RuntimeError(f"could not find pid for addr={victim_addr}")
    print(f"  [{mode} d={downtime}] SIGKILL pid={pid}")
    os.kill(pid, signal.SIGKILL)
    kill_ts = time.time()

    # Sleep downtime; workload runs on at K=2 quorum.
    time.sleep(downtime)

    # Snapshot leader's lag just before restart.
    pre = scrape_lag(leader_status)
    sum_pre, cnt_pre = pre or (0.0, 0.0)

    # Restart victim.
    addr = victim_addr
    status = 20180 + (victim_dir_idx - 1)
    spawn(
        [str(TIKV), "--pd=127.0.0.1:2379",
         f"--addr=127.0.0.1:{addr}",
         f"--advertise-addr=127.0.0.1:{addr}",
         f"--status-addr=127.0.0.1:{status}",
         f"--data-dir=/data/disk{victim_dir_idx}/tikv",
         f"--config={toml}",
         f"--log-file={cell_dir / f'tikv{victim_dir_idx}-restart.log'}"],
        cell_dir / f"tikv{victim_dir_idx}-restart.out")
    restart_ts = time.time()
    print(f"  [{mode} d={downtime}] restart, polling leader lag")

    # Poll loop.
    prev_sum, prev_cnt = sum_pre, cnt_pre
    hits = 0
    peak_avg = 0.0
    last_avg = 0.0
    trajectory: List[Tuple[float, float]] = []
    deadline = restart_ts + POLL_TIMEOUT_S
    while time.time() < deadline:
        cur = scrape_lag(leader_status)
        if cur is None:
            time.sleep(POLL_S)
            continue
        cur_sum, cur_cnt = cur
        d_cnt = cur_cnt - prev_cnt
        d_sum = cur_sum - prev_sum
        if d_cnt > 0:
            iv_avg = d_sum / d_cnt
            trajectory.append((time.time() - restart_ts, iv_avg))
            peak_avg = max(peak_avg, iv_avg)
            last_avg = iv_avg
            if iv_avg < LAG_THRESHOLD:
                hits += 1
            else:
                hits = 0
            if hits >= CAUGHT_UP_HITS:
                break
        prev_sum, prev_cnt = cur_sum, cur_cnt
        time.sleep(POLL_S)
    caught_up_ts = time.time()
    catchup_s = caught_up_ts - restart_ts - (POLL_S * (CAUGHT_UP_HITS - 1))

    bw_after = bytes_written(victim_dev)
    disk_bytes = bw_after - bw_before
    disk_mb = disk_bytes / (1024 * 1024)
    disk_mb_rate = disk_mb / max(catchup_s, 0.001)

    # Save trajectory CSV per cell.
    traj_csv = cell_dir / "trajectory.csv"
    with open(traj_csv, "w") as f:
        f.write("t_since_restart_s,interval_avg_lag\n")
        for t, v in trajectory:
            f.write(f"{t:.3f},{v:.3f}\n")

    # Stop workload + cluster.
    try:
        workload.terminate()
    except Exception:
        pass
    time.sleep(2)
    killall(["pd-server", "tikv-server", "go-ycsb"])
    time.sleep(2)

    return {
        "mode": mode,
        "downtime_s": downtime,
        "leader_id": leader_id,
        "victim_id": victim_id,
        "victim_addr": victim_addr,
        "time_to_catchup_s": catchup_s,
        "peak_interval_avg_lag": peak_avg,
        "final_interval_avg_lag": last_avg,
        "n_polls": len(trajectory),
        "disk_bytes_written": disk_bytes,
        "disk_mb_per_s": disk_mb_rate,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--downtimes", default="10,30,60,120,300",
                    help="comma-sep downtime seconds")
    ap.add_argument("--modes", default="baseline,metronome",
                    help="comma-sep mode set")
    ap.add_argument("--smoke", action="store_true",
                    help="just one cell (metronome d=10) to validate plumbing")
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    logs_root = OUT_DIR / "logs"
    logs_root.mkdir(parents=True, exist_ok=True)

    if args.smoke:
        downtimes = [10]
        modes = ["metronome"]
    else:
        downtimes = [int(x) for x in args.downtimes.split(",")]
        modes = args.modes.split(",")

    with open(RESULTS_CSV, "w") as f:
        f.write("mode,downtime_s,leader_id,victim_id,victim_addr,"
                "time_to_catchup_s,peak_interval_avg_lag,"
                "final_interval_avg_lag,n_polls,"
                "disk_bytes_written,disk_mb_per_s\n")

    for d in downtimes:
        for m in modes:
            print(f"\n========= cell mode={m} downtime={d}s =========")
            try:
                r = run_cell(m, d, logs_root)
            except Exception as e:
                print(f"  cell failed: {e!r}", file=sys.stderr)
                continue
            with open(RESULTS_CSV, "a") as f:
                f.write(f"{r['mode']},{r['downtime_s']},"
                        f"{r['leader_id']},{r['victim_id']},{r['victim_addr']},"
                        f"{r['time_to_catchup_s']:.3f},"
                        f"{r['peak_interval_avg_lag']:.3f},"
                        f"{r['final_interval_avg_lag']:.3f},"
                        f"{r['n_polls']},"
                        f"{r['disk_bytes_written']},"
                        f"{r['disk_mb_per_s']:.2f}\n")
            print(f"  → catchup={r['time_to_catchup_s']:.2f}s "
                  f"peak_lag={r['peak_interval_avg_lag']:.0f} "
                  f"disk={r['disk_bytes_written']/1e6:.1f}MB "
                  f"({r['disk_mb_per_s']:.1f}MB/s)")

    print(f"\nCSV: {RESULTS_CSV}")


if __name__ == "__main__":
    main()
