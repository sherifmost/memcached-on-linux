#!/usr/bin/env python3
"""
Aggregate memtier results (single-Redis runs) into the same format as the mutilate memcached aggregator:
Columns: Rate, QPS, Avg_Latency, P90_Latency, P99_Latency, Power, StdDev, Util, Misses
- Latencies in microseconds (merge HDR histograms if present; fallback to FULL_RUN percentiles in ms -> converted to µs)
- QPS is the sum of Ops/sec across clients
- Misses summed across clients
- Power/StdDev/Util parsed like the mutilate aggregator from powerstat files
"""
import argparse
import csv
import glob
import os
import re
import math
from pathlib import Path
from collections import defaultdict

PCTS = [50, 90, 95, 99]


def parse_log_ops(path: Path):
    ops = None
    misses = None
    with open(path, 'r', errors='ignore') as f:
        for line in f:
            if line.startswith('Totals'):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        ops = float(parts[1])
                    except ValueError:
                        pass
            if 'Misses' in line and '=' in line:
                try:
                    misses = float(line.split('=')[1].split()[0])
                except Exception:
                    pass
    return ops, misses


def parse_full_run(path: Path):
    """Parse memtier *_FULL_RUN.txt percentiles (values in ms)."""
    pct_vals = {p: None for p in PCTS}
    with open(path, 'r', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = re.split(r'\s+', line)
            if len(parts) < 2:
                continue
            try:
                p = float(parts[0]); v = float(parts[1])
            except ValueError:
                continue
            if p in pct_vals:
                pct_vals[p] = v
    return pct_vals


def parse_hdr_txt(path: Path):
    """Parse HdrHistogram text (Value Percentile TotalCount ...) -> counts dict (value->count), total count. Values in µs."""
    with open(path, 'r', errors='ignore') as f:
        tokens = f.read().strip().split()
    try:
        i = tokens.index('Value') + 4
    except ValueError:
        return {}, 0
    counts = defaultdict(int)
    prev = 0
    while i + 3 < len(tokens):
        try:
            v = float(tokens[i])
            tc = int(float(tokens[i+2]))
        except ValueError:
            break
        delta = tc - prev
        if delta < 0:
            delta = 0
        counts[v] += delta
        prev = tc
        i += 4
    return counts, prev


def quantile_from_counts(counts: dict, q: float):
    if not counts:
        return None
    items = sorted(counts.items(), key=lambda x: x[0])
    total = sum(c for _, c in items)
    if total == 0:
        return None
    target = math.ceil(q * total)
    run = 0
    for v, c in items:
        run += c
        if run >= target:
            return v
    return items[-1][0]


def mean_from_counts(counts: dict):
    if not counts:
        return None
    total = sum(counts.values())
    if total == 0:
        return None
    return sum(v * c for v, c in counts.items()) / total


def parse_powerstat_file(path: Path):
    """Parse powerstat like the mutilate aggregator: returns (avg_power, std_power, util)."""
    with open(path, 'r', errors='ignore') as file:
        lines = file.readlines()
    if len(lines) < 25:
        return None, None, None
    start_index = 20
    end_index = next((i for i, line in enumerate(lines[start_index:], start=start_index) if line.startswith('-')), len(lines))
    data_lines = lines[start_index:end_index]
    data_lines = [line.strip() for line in data_lines if line.strip()]
    data_lines = [line.split() for line in data_lines]
    data_lines = [[line[4], line[12]] for line in data_lines if len(line) > 12]

    vals_power = []
    vals_util = []
    for u, p in data_lines:
        try:
            vals_util.append(float(u))
            vals_power.append(float(p))
        except ValueError:
            continue
    if not vals_power:
        return None, None, None
    avg_p = sum(vals_power) / len(vals_power)
    std_p = (sum((x - avg_p) ** 2 for x in vals_power) / len(vals_power)) ** 0.5
    avg_util = sum(vals_util) / len(vals_util) if vals_util else None
    return avg_p, std_p, 100 - avg_util if avg_util is not None else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('run_dir', help='Run directory (qps_* subfolders)')
    ap.add_argument('--out', default=None, help='Output CSV path (default: aggregated_memtier_<run>.csv)')
    ap.add_argument('--outdir', default='.', help='Output directory (default: current dir)')
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    agg_rows = []

    for qdir in sorted(run_dir.glob('qps_*')):
        if not qdir.is_dir():
            continue
        qps = qdir.name.split('_', 1)[-1]
        logs = sorted(qdir.glob('memtier_*_qps_*_c*.log'))
        if not logs:
            continue
        total_ops = 0.0
        total_misses = 0.0
        merged_counts = defaultdict(int)
        pct_max = {p: None for p in PCTS}

        for log in logs:
            base = log.stem
            full_run = log.with_name(base + '_FULL_RUN.txt')
            ops, misses = parse_log_ops(log)
            if ops is not None:
                total_ops += ops
            if misses is not None:
                total_misses += misses
            if full_run.exists():
                pct_fr = parse_full_run(full_run)
                for p in PCTS:
                    if pct_fr[p] is not None:
                        if pct_max[p] is None or pct_fr[p] > pct_max[p]:
                            pct_max[p] = pct_fr[p]
            # Prefer FULL_RUN histograms (all ops). Avoid GET/SET to prevent double-counting.
            hdr_txts = list(log.parent.glob(base + "_FULL_RUN_*.txt"))
            for hdr in hdr_txts:
                counts, _ = parse_hdr_txt(hdr)
                for v, c in counts.items():
                    merged_counts[v] += c

        pct_vals = {p: None for p in PCTS}
        mean_us = None
        if merged_counts:
            mean_us = mean_from_counts(merged_counts)
            for p in PCTS:
                pct_vals[p] = quantile_from_counts(merged_counts, p/100.0)
        else:
            for p in PCTS:
                pct_ms = pct_max[p]
                pct_vals[p] = pct_ms * 1000.0 if pct_ms is not None else None
        avg_latency = mean_us if mean_us is not None else pct_vals[50]

        # convert to microseconds for output
        avg_out = avg_latency * 1000.0 if avg_latency is not None else None
        p90_out = pct_vals[90] * 1000.0 if pct_vals[90] is not None else None
        p99_out = pct_vals[99] * 1000.0 if pct_vals[99] is not None else None

        agg_rows.append({
            'Rate': qps,
            'QPS': total_ops if total_ops else None,
            'Avg_Latency': avg_out,
            'P90_Latency': p90_out,
            'P99_Latency': p99_out,
            'Misses': total_misses if total_misses else None,
        })

    # power
    power_by_qps = {}
    for pfile in run_dir.glob('powerstat_rate_*.txt'):
        qps = pfile.name.split('_')[-1].split('.')[0]
        try:
            avg_p, std_p, util = parse_powerstat_file(pfile)
            power_by_qps[qps] = (avg_p, std_p, util)
        except Exception:
            pass
    for r in agg_rows:
        p = power_by_qps.get(r['Rate'])
        if p:
            r['Power'] = p[0]; r['StdDev'] = p[1]; r['Util'] = p[2]
        else:
            r['Power'] = r['StdDev'] = r['Util'] = None

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    run_tag = run_dir.name.replace('run_','')
    out_path = Path(args.out) if args.out else outdir / f'aggregated_memtier_{run_tag}.csv'
    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['Rate','QPS','Avg_Latency','P90_Latency','P99_Latency','Power','StdDev','Util','Misses'])
        writer.writeheader()
        for r in agg_rows:
            writer.writerow(r)
    print(f"[+] wrote {len(agg_rows)} rows to {out_path}")


if __name__ == '__main__':
    main()
