#!/usr/bin/env python3
"""
Parse memtier results for single-Redis runs.
- Expects run directory containing qps_<QPS>/memtier_* files copied from clients
- For each QPS and client file, extracts actual Ops/sec from .log and latency percentiles from _FULL_RUN.txt (HDR text)
Outputs: aggregated CSV to stdout (or specify --out).
"""
import argparse
import csv
import glob
import os
import re
from pathlib import Path

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
    """
    Parse memtier *_FULL_RUN.txt percentile output.
    Returns dict pct->value (as reported; memtier prints ms).
    """
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
                p = float(parts[0])
                v = float(parts[1])
            except ValueError:
                continue
            if p in pct_vals:
                pct_vals[p] = v
    return pct_vals


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('run_dir', help='Run directory (e.g., PROFILE_GOV_redis_single_logs_machine/run_2026-02-XX_XX-XX-XX)')
    ap.add_argument('--out', default=None, help='Output CSV path (default: aggregated_memtier.csv in run_dir)')
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    agg_rows = []
    # find qps folders
    for qdir in sorted(run_dir.glob('qps_*')):
        if not qdir.is_dir():
            continue
        qps = qdir.name.split('_', 1)[-1]
        logs = sorted(qdir.glob('memtier_*_qps_*_c*.log'))
        if not logs:
            continue
        total_ops = 0.0
        total_misses = 0.0
        pct_max = {p: None for p in PCTS}
        for log in logs:
            base = log.stem  # memtier_PROFILE_qps_<QPS>_cX
            full_run = log.with_name(base + '_FULL_RUN.txt')
            ops, misses = parse_log_ops(log)
            if ops is not None:
                total_ops += ops
            if misses is not None:
                total_misses += misses
            if full_run.exists():
                pct = parse_full_run(full_run)
                for p in PCTS:
                    if pct[p] is not None:
                        if pct_max[p] is None or pct[p] > pct_max[p]:
                            pct_max[p] = pct[p]
        agg_rows.append({
            'QPS_target': qps,
            'Ops_per_sec': total_ops if total_ops else None,
            'Misses': total_misses if total_misses else None,
            'P50_ms': pct_max[50],
            'P90_ms': pct_max[90],
            'P95_ms': pct_max[95],
            'P99_ms': pct_max[99],
        })

    out_path = Path(args.out) if args.out else run_dir / 'aggregated_memtier.csv'
    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['QPS_target','Ops_per_sec','Misses','P50_ms','P90_ms','P95_ms','P99_ms'])
        writer.writeheader()
        for r in agg_rows:
            writer.writerow(r)
    print(f"[+] wrote {len(agg_rows)} rows to {out_path}")


if __name__ == '__main__':
    main()
