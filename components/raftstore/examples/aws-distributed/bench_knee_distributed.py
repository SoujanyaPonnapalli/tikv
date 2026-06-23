#!/usr/bin/env python3
"""Distributed knee bench: one TiKV server per host, single PD/client on
the controller. Same threadcount sweep + measurement semantics as the
colocated bench, but RPC goes over a real (sub-ms, same-rack) network.

Usage on the controller after setup_ctl.sh has pushed tikv-server to all
TiKV hosts:

    python3 bench_knee_distributed.py \\
        --tikv-ips 10.0.0.10 10.0.0.11 10.0.0.12 \\
        --threads 16 32 64 128 256 512 1024 2048 4096 \\
        --val 4096

Outputs /data/bench-knee-dist/knee-results.csv.
"""
import argparse
import csv
import os
import pathlib
import re
import shlex
import signal
import subprocess
import sys
import time
import urllib.request
from typing import List, Optional

HOME = pathlib.Path(os.environ.get("HOME", "/home/ubuntu"))
TIDB_VER = os.environ.get("TIDB_VER", "v8.5.6")
PD = HOME / f".tiup/components/pd/{TIDB_VER}/pd-server"
YCSB = HOME / "go/bin/go-ycsb"
TIKV_REMOTE = "/home/ubuntu/tikv-server"
SSH_KEY = HOME / ".ssh/tikv-bench.pem"
SSH_OPTS = [
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "ConnectTimeout=10",
    "-o", "ServerAliveInterval=20",
]
if SSH_KEY.exists():
    SSH_OPTS = ["-i", str(SSH_KEY)] + SSH_OPTS

BASE = pathlib.Path("/data/bench-knee-dist") if pathlib.Path("/data").exists() else pathlib.Path.home() / "bench-knee-dist"
RESULTS_CSV = BASE / "knee-results.csv"


def ssh_run(ip: str, cmd: str, capture: bool = True, timeout: int = 60) -> str:
    args = ["ssh"] + SSH_OPTS + [f"ubuntu@{ip}", cmd]
    if capture:
        return subprocess.check_output(args, text=True, timeout=timeout, stderr=subprocess.STDOUT)
    subprocess.run(args, check=True, timeout=timeout)
    return ""


def ssh_bg(ip: str, cmd: str) -> None:
    """Run a remote command in the background via nohup + disown.
    The remote process keeps running after this returns.
    """
    wrapped = f"nohup bash -c '{cmd}' > /dev/null 2>&1 < /dev/null & disown"
    subprocess.run(["ssh"] + SSH_OPTS + [f"ubuntu@{ip}", wrapped], check=True, timeout=30)


def scp_to(ip: str, src: str, dst: str) -> None:
    subprocess.run(["scp"] + SSH_OPTS + [src, f"ubuntu@{ip}:{dst}"], check=True, timeout=120)


def http_get(url: str, timeout: float = 1.0) -> Optional[str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.read().decode()
    except Exception:
        return None


def wait_pd_up(pd_host: str, timeout_s: int = 30) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if http_get(f"http://{pd_host}:2379/pd/api/v1/health"):
            return True
        time.sleep(1)
    return False


def wait_stores_up(pd_host: str, n: int, timeout_s: int = 60) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        body = http_get(f"http://{pd_host}:2379/pd/api/v1/stores")
        if body:
            import json
            try:
                d = json.loads(body)
                if len(d.get("stores", [])) >= n and all(
                    s["store"]["state_name"] == "Up" for s in d["stores"]
                ):
                    return True
            except Exception:
                pass
        time.sleep(1)
    return False


def kill_local() -> None:
    subprocess.run(["pkill", "-9", "-f", "pd-server"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["pkill", "-9", "-f", "go-ycsb"],   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def kill_remote(ip: str) -> None:
    try:
        ssh_run(ip, "pkill -9 -f /home/ubuntu/tikv-server; rm -rf /data/disk/tikv && mkdir -p /data/disk/tikv && chown -R ubuntu:ubuntu /data/disk/tikv", timeout=30)
    except Exception as e:
        print(f"  kill_remote({ip}) error: {e}", file=sys.stderr)


def start_pd(pd_data: pathlib.Path, log: pathlib.Path, ctl_ip: str) -> subprocess.Popen:
    """Start PD bound to all interfaces but advertising the controller's
    private IP so the remote TiKV hosts can dial it. The peer-urls /
    initial-cluster pair must match exactly or PD refuses to start.
    """
    log.parent.mkdir(parents=True, exist_ok=True)
    f = open(log, "ab")
    return subprocess.Popen(
        [str(PD), "--name=pd1", f"--data-dir={pd_data}",
         "--client-urls=http://0.0.0.0:2379",
         f"--advertise-client-urls=http://{ctl_ip}:2379",
         f"--peer-urls=http://{ctl_ip}:2380",
         f"--initial-cluster=pd1=http://{ctl_ip}:2380",
         f"--log-file={log.with_suffix('.pdlog')}"],
        stdout=f, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, start_new_session=True,
    )


def start_tikv(ip: str, pd_host: str, toml_remote: str, log: str) -> None:
    cmd = (
        f"{TIKV_REMOTE} --pd={pd_host}:2379 "
        f"--addr=0.0.0.0:20160 --advertise-addr={ip}:20160 "
        f"--status-addr=0.0.0.0:20180 "
        f"--data-dir=/data/disk/tikv --config={toml_remote} "
        f"--log-file={log}"
    )
    ssh_bg(ip, cmd)


def run_ycsb(pd_host: str, threads: int, val: int, records: int, ops: int, phase: str, out: pathlib.Path) -> None:
    args = [
        str(YCSB), phase, "tikv",
        "-p", f"tikv.pd={pd_host}:2379", "-p", "tikv.type=raw",
        "-p", f"recordcount={records}", "-p", f"operationcount={ops}",
        "-p", "fieldcount=1", "-p", f"fieldlength={val}",
        "-p", f"threadcount={threads}",
    ]
    if phase == "run":
        args += ["-p", "readproportion=0.0", "-p", "updateproportion=1.0"]
    with open(out, "wb") as f:
        subprocess.run(args, stdout=f, stderr=subprocess.STDOUT, timeout=2400, check=False)


_PHASE_RE = re.compile(
    r"^(?P<phase>INSERT|UPDATE) - "
    r"Takes\(s\): (?P<takes>[\d\.]+).*?Count: (?P<count>[\d]+).*?"
    r"OPS: (?P<ops>[\d\.]+).*?Avg\(us\): (?P<avg>[\d]+).*?"
    r"50th\(us\): (?P<p50>[\d]+).*?90th\(us\): (?P<p90>[\d]+).*?99th\(us\): (?P<p99>[\d]+)",
    re.M,
)


def parse_ycsb(path: pathlib.Path, phase_label: str) -> Optional[dict]:
    body = path.read_text(errors="ignore")
    for m in _PHASE_RE.finditer(body):
        if m.group("phase") == phase_label:
            return {
                "count":     int(m.group("count")),
                "takes_ms":  int(float(m.group("takes")) * 1000),
                "ops":       float(m.group("ops")),
                "avg_us":    int(m.group("avg")),
                "p50_us":    int(m.group("p50")),
                "p90_us":    int(m.group("p90")),
                "p99_us":    int(m.group("p99")),
            }
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tikv-ips", nargs="+", required=True, help="private IPs of TiKV hosts")
    ap.add_argument("--threads", nargs="+", type=int,
                    default=[16, 32, 64, 128, 256, 512, 1024, 2048, 4096])
    ap.add_argument("--modes", nargs="+", default=["baseline", "metronome"])
    ap.add_argument("--val", type=int, default=4096)
    ap.add_argument("--records", type=int, default=5000)
    ap.add_argument("--run-ops", type=int, default=200000)
    ap.add_argument("--warmup-s", type=int, default=15)
    args = ap.parse_args()

    ctl_ip = subprocess.check_output(
        ["curl", "-s", "--max-time", "2", "http://169.254.169.254/latest/meta-data/local-ipv4"],
        text=True,
    ).strip() or "127.0.0.1"
    pd_host = ctl_ip
    n = len(args.tikv_ips)
    BASE.mkdir(parents=True, exist_ok=True)

    with open(RESULTS_CSV, "w") as f:
        f.write("n,val_bytes,mode,threads,phase,count,takes_ms,avg_us,p50_us,p90_us,p99_us,ops\n")

    for threads in args.threads:
        for mode in args.modes:
            print(f"\n========= mode={mode} threads={threads} =========", flush=True)
            kill_local()
            time.sleep(2)
            for ip in args.tikv_ips:
                kill_remote(ip)

            toml_remote = f"/home/ubuntu/tikv-{mode}.toml"
            cell_logs = BASE / f"logs/{mode}-t{threads}"
            cell_logs.mkdir(parents=True, exist_ok=True)
            pd_data = cell_logs / "pd_data"

            # Wipe any prior PD bootstrap from a previous failed run.
            subprocess.run(["rm", "-rf", str(pd_data)], check=False)
            pd_proc = start_pd(pd_data, cell_logs / "pd.out", ctl_ip)
            if not wait_pd_up(pd_host, 30):
                print(f"  PD did not come up; aborting cell", file=sys.stderr); continue
            for ip in args.tikv_ips:
                tikv_log = f"/data/disk/tikv-{mode}-t{threads}.log"
                start_tikv(ip, pd_host, toml_remote, tikv_log)
                time.sleep(4)
            time.sleep(6)
            if not wait_stores_up(pd_host, n, 60):
                print(f"  stores never came Up; aborting cell", file=sys.stderr); continue

            load_out = cell_logs / "load.txt"
            run_out  = cell_logs / "run.txt"
            run_ycsb(pd_host, threads, args.val, args.records, args.records, "load", load_out)
            run_ycsb(pd_host, threads, args.val, args.records, args.run_ops, "run",  run_out)

            for phase_label, phase_name, path in (
                ("INSERT", "LOAD", load_out),
                ("UPDATE", "RUN",  run_out),
            ):
                r = parse_ycsb(path, phase_label)
                if not r:
                    print(f"  parse failed for {phase_name} of {path}", file=sys.stderr); continue
                with open(RESULTS_CSV, "a") as f:
                    f.write(f"{n},{args.val},{mode},{threads},{phase_name},"
                            f"{r['count']},{r['takes_ms']},{r['avg_us']},"
                            f"{r['p50_us']},{r['p90_us']},{r['p99_us']},{r['ops']}\n")
                if phase_name == "RUN":
                    print(f"  → {phase_name}: ops={r['ops']:.1f} avg={r['avg_us']/1000:.1f}ms p99={r['p99_us']/1000:.1f}ms")

            kill_local()
            time.sleep(1)
            for ip in args.tikv_ips:
                kill_remote(ip)

    print(f"\nCSV: {RESULTS_CSV}")


if __name__ == "__main__":
    main()
