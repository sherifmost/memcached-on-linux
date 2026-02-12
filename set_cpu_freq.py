#!/usr/bin/env python3
"""
Set or reset CPU frequency on remote servers via SSH.

Usage:
  python3 run/set_cpu_freq.py --server-idxs 0,1,2 --mode set --freq-khz 2200000
  python3 run/set_cpu_freq.py --server-idxs 0,1,2 --mode reset
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import paramiko

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
import setup.config_remote as cfg
from setup.util import execute_remote


def ssh_host(idx: int) -> str:
    return f"{cfg.USERNAME}@{cfg.NODES[idx]}"


def run_cmd(host: str, cmd: str, key_path: str | None) -> None:
    if "@" in host:
        ssh_user, ssh_host = host.split("@", 1)
    else:
        ssh_user, ssh_host = cfg.USERNAME, host
    conn = paramiko.SSHClient()
    conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    conn.connect(hostname=ssh_host, username=ssh_user, key_filename=key_path or cfg.KEY_LOCATION)
    execute_remote([conn], cmd, True, False)
    conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Set/reset CPU frequency on remote servers.")
    ap.add_argument("--server-idxs", required=True, help="Comma-separated indices into config_remote.NODES.")
    ap.add_argument("--ssh-key", default=cfg.KEY_LOCATION, help="Path to SSH private key.")
    ap.add_argument("--mode", choices=("set", "reset"), required=True, help="Set fixed frequency or reset to defaults.")
    ap.add_argument("--freq-khz", type=int, default=2200000, help="Fixed frequency in kHz for --mode set.")
    ap.add_argument("--set-governor", default="performance",
                    help="Governor to use when setting a fixed freq (default: performance).")
    ap.add_argument("--reset-governor", default="schedutil",
                    help="Governor to use when resetting (default: schedutil).")
    args = ap.parse_args()

    server_indices = [int(x) for x in args.server_idxs.split(",") if x.strip() != ""]
    if not server_indices:
        raise SystemExit("No servers specified.")

    if args.mode == "set":
        cmd = (
            f"sudo sh -c 'echo {args.set_governor} | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor' && "
            f"sudo sh -c 'echo {args.freq_khz} | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_min_freq' && "
            f"sudo sh -c 'echo {args.freq_khz} | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_max_freq'"
        )
    else:
        cmd = (
            f"sudo sh -c 'echo {args.reset_governor} | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor' && "
            "sudo sh -c 'cat /sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_min_freq | "
            "tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_min_freq' && "
            "sudo sh -c 'cat /sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq | "
            "tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_max_freq'"
        )

    for idx in server_indices:
        host = ssh_host(idx)
        print(f"[*] {args.mode} CPU frequency on {host}")
        run_cmd(host, cmd, args.ssh_key)


if __name__ == "__main__":
    main()
