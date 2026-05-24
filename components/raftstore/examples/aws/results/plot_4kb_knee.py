#!/usr/bin/env python3
"""Throughput-latency knee plot for val=4 KB, N ∈ {3, 5, 7}.

Reads the per-run knee CSVs and stitches val=4096 data across all N=3
extensions (knee-multival has t=16..512, extend has 1024-4096, extend2 has
8192-16384, extend3 has 32768-65536). N=5 and N=7 come from knee-N57.

Plots three panels (one per N), with throughput on the x-axis and p50
latency on the y-axis (log scale because latency spans ~4 orders of
magnitude). Two curves per panel: baseline TiKV vs metronome. Thread-count
labels annotate the saturation region.
"""
import csv
import os
from collections import defaultdict
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))

# Files containing val=4096 cells.
SOURCES = [
    "knee-multival-ebs125-N3.csv",  # N=3 t=16..512
    "knee-N3-extend.csv",            # N=3 t=1024..4096
    "knee-N3-extend2.csv",           # N=3 t=8192..16384
    "knee-N3-extend3.csv",           # N=3 t=32768..65536
    "knee-N57-ebs125.csv",           # N=5,7 t=512..4096
]

# data[(n, mode)] = list of (threads, ops, p50_us, p99_us)
data = defaultdict(list)
for fname in SOURCES:
    path = os.path.join(HERE, fname)
    with open(path) as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            if not row.get("ops"):
                continue
            try:
                n = int(row["n"])
                val = int(row["val_bytes"])
                phase = row["phase"]
                mode = row["mode"]
            except (KeyError, ValueError):
                continue
            if val != 4096 or phase != "RUN":
                continue
            threads = int(row["threads"])
            ops = float(row["ops"])
            p50 = float(row["p50_us"])
            p99 = float(row["p99_us"])
            data[(n, mode)].append((threads, ops, p50, p99))

# Sort each series by threads
for k in data:
    data[k].sort()

fig, axes = plt.subplots(1, 3, figsize=(15, 5), sharey=True)
styles = {
    "baseline":  dict(color="#d62728", marker="o", label="TiKV (baseline)"),
    "metronome": dict(color="#1f77b4", marker="s", label="Metronome"),
}

for ax, n in zip(axes, [3, 5, 7]):
    for mode in ("baseline", "metronome"):
        series = data.get((n, mode), [])
        if not series:
            continue
        threads = [r[0] for r in series]
        ops = [r[1] for r in series]
        p50 = [r[2] / 1000.0 for r in series]  # us → ms
        ax.plot(ops, p50, linewidth=1.8, markersize=6, **styles[mode])
        # Annotate every other thread count
        for i, t in enumerate(threads):
            if i % 2 == 0:
                ax.annotate(f"t={t}",
                            (ops[i], p50[i]),
                            textcoords="offset points",
                            xytext=(4, 4),
                            fontsize=7,
                            color=styles[mode]["color"])
    ax.set_title(f"N = {n}")
    ax.set_xlabel("Throughput (ops/s)")
    ax.set_yscale("log")
    ax.grid(True, which="both", linestyle="--", alpha=0.4)
    ax.legend(loc="upper left", fontsize=9)

axes[0].set_ylabel("p50 latency (ms, log scale)")
fig.suptitle("TiKV vs Metronome — 4 KB writes on EBS gp3 (125 MiB/s/vol, c6i.8xlarge)",
             fontsize=12)
fig.tight_layout(rect=[0, 0, 1, 0.95])

out = os.path.join(HERE, "plot-4KB-knee.png")
fig.savefig(out, dpi=150)
print(f"wrote {out}")

# Also dump the underlying data for transparency.
print("\n=== val=4 KB stitched data ===")
print(f"{'N':>2} {'mode':<10} {'threads':>7} {'ops':>10} {'p50_ms':>9} {'p99_ms':>9}")
for n in [3, 5, 7]:
    for mode in ("baseline", "metronome"):
        for threads, ops, p50, p99 in data[(n, mode)]:
            print(f"{n:>2} {mode:<10} {threads:>7} {ops:>10.1f} {p50/1000:>9.2f} {p99/1000:>9.2f}")
    print()
