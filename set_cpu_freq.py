#!/usr/bin/env python3
"""
Set or reset CPU frequency on remote servers via SSH.

Examples:
  # Set to 3.4 GHz with performance governor on two hosts
  python3 set_cpu_freq.py --hosts server_mem,client_mem1 --mode set --freq-khz 3400000 --set-governor performance

  # Reset to default min/max and schedutil governor
  python3 set_cpu_freq.py --hosts server_mem --mode reset --reset-governor schedutil
"""

from __future__ import annotations

import argparse
import getpass
import subprocess


def run_cmd(host: str, cmd: str, key_path: str | None, user: str) -> None:
    target = f"{user}@{host}" if "@" not in host else host
    ssh_cmd = ["ssh"]
    if key_path:
        ssh_cmd += ["-i", key_path]
    ssh_cmd += [target, cmd]
    subprocess.run(ssh_cmd, check=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Set/reset CPU frequency on remote servers via SSH.")
    ap.add_argument("--hosts", required=True, help="Comma-separated hostnames/aliases to target.")
    ap.add_argument("--ssh-key", default=None, help="Path to SSH private key.")
    ap.add_argument("--user", default=getpass.getuser(), help="SSH username (default: current user).")
    ap.add_argument("--mode", choices=("set", "reset"), required=True, help="Set fixed frequency or reset to defaults.")
    ap.add_argument("--freq-khz", type=int, default=2200000, help="Fixed frequency in kHz for --mode set.")
    ap.add_argument("--set-governor", default="performance",
                    help="Governor to use when setting a fixed freq (default: performance).")
    ap.add_argument("--reset-governor", default="schedutil",
                    help="Governor to use when resetting (default: schedutil).")
    args = ap.parse_args()

    hosts = [h.strip() for h in args.hosts.split(",") if h.strip()]
    if not hosts:
        raise SystemExit("No hosts specified.")

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

    for host in hosts:
        print(f"[*] {args.mode} CPU frequency on {host}")
        run_cmd(host, cmd, args.ssh_key, args.user)


if __name__ == "__main__":
    main()
