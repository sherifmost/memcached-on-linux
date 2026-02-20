#!/usr/bin/env python3
"""
Sharded Redis experiment orchestrator (Python)
- Starts N Redis shards on server_mem (one port/core each)
- Optionally preloads via memtier on 3 clients
- Runs QPS sweeps with memtier per shard, pinned cores to avoid oversubscription
- Collects logs locally under <PROFILE>_<GOV>_redis_logs_xl170/run_<timestamp>

Requirements: passwordless SSH to aliases server_mem, client_mem1/2/3 and memtier installed at ~/memtier on clients.
"""

import subprocess
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

SERVER_ALIAS = "server_mem"
CLIENTS = ["client_mem1", "client_mem2", "client_mem3"]

REDIS_IP = "10.10.1.1"
REDIS_SHARDS = 20
REDIS_PORT_BASE = 7000
REDIS_CORES_START = 0
REDIS_MAXMEM = "0"
REDIS_MAXMEM_POLICY = "noeviction"
REDIS_IO_THREADS = 4
REDIS_IO_THREADS_DO_READS = "yes"

MEMTIER_THREADS = 2  # per shard
MEMTIER_CLIENTS = 50
MEMTIER_PIPELINE = 32
MEMTIER_KEY_MAX = 5_000_000
MEMTIER_TEST_TIME = 60
MEMTIER_BIN_HINT = "./memtier_benchmark"  # will fall back to PATH if not present

QPS_LIST = [300000]
SYNC_DELAY = 10
POWER_STAT_DELAY = 15

CORE_SLICES = [f"{2*i}-{2*i+1}" for i in range(10)] * 2  # 20 shards -> 20 slices
SSH_OPTS = ["-o","StrictHostKeyChecking=no","-o","UserKnownHostsFile=/dev/null","-o","BatchMode=yes","-o","ConnectTimeout=5"]

def run(cmd):
    return subprocess.run(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def ssh(host, remote_cmd):
    cmd = ["ssh", *SSH_OPTS, host, remote_cmd]
    return run(cmd)

def scp(src, dest):
    cmd = ["scp", *SSH_OPTS, src, dest]
    return run(cmd)

def profile_params(profile):
    if profile.upper() == "USR":
        return {
            "ratio": "1:500",
            "key_pattern": "R:R",
            "zipf": None,
            "data_size": 2,
            "data_list": None,
        }
    # default ETC
    return {
        "ratio": "1:30",
        "key_pattern": "Z:Z",
        "zipf": "1.2",
        "data_size": None,
        "data_list": "1:583,2:17820,3:9239,4:18,5:2740,6:65,7:606,8:23,9:837,10:837,11:8989,12:92,13:326,14:1980,22:4200,45:6821,91:10397,181:12790,362:11421,724:6768,1448:2598,2896:683,5793:136,11585:23,23170:3,46341:1,92682:1,185364:1,370728:1,741455:1",
    }

def start_shard(shard, core):
    port = REDIS_PORT_BASE + shard
    cmd = (
        f"cd logs_{DATE_TAG} && nohup taskset -c {core} redis-server "
        f"--bind {REDIS_IP} --port {port} --save '' --appendonly no "
        f"--maxmemory {REDIS_MAXMEM} --maxmemory-policy {REDIS_MAXMEM_POLICY} "
        f"--io-threads {REDIS_IO_THREADS} --io-threads-do-reads {REDIS_IO_THREADS_DO_READS} "
        f"> redis_shard_{port}.log 2>&1 &"
    )
    return ssh(SERVER_ALIAS, cmd)

def preload_shard(shard, host, prefix, memtier_args):
    port = REDIS_PORT_BASE + shard
    cmd = (
        f"cd ~/memtier && mkdir -p logs_{DATE_TAG} && "
        f"mt=$(command -v memtier_benchmark || echo {MEMTIER_BIN_HINT}); "
        f"[ -x \"$mt\" ] || {{ echo 'memtier_benchmark not found' >&2; exit 127; }}; "
        f"taskset -c {CORE_SLICES[shard]} \"$mt\" --server={REDIS_IP} --port={port} --protocol=redis "
        f"--clients={MEMTIER_CLIENTS} --threads={MEMTIER_THREADS} --ratio=1:0 "
        f"--key-maximum={MEMTIER_KEY_MAX // REDIS_SHARDS} --key-pattern=S:S --key-prefix='{prefix}' "
        f"{memtier_args} --requests=allkeys --hide-histogram "
        f"> logs_{DATE_TAG}/memtier_preload_{PROFILE}_shard{shard}.log 2>&1"
    )
    return ssh(host, cmd)

def run_shard_qps(shard, host, prefix, qps, memtier_args):
    port = REDIS_PORT_BASE + shard
    qps_shard = (qps + REDIS_SHARDS - 1) // REDIS_SHARDS
    total_conns = MEMTIER_THREADS * MEMTIER_CLIENTS
    per_conn = (qps_shard + total_conns - 1) // total_conns
    cmd = (
        f"cd ~/memtier && mkdir -p logs_{DATE_TAG} && "
        f"mt=$(command -v memtier_benchmark || echo {MEMTIER_BIN_HINT}); "
        f"[ -x \"$mt\" ] || {{ echo 'memtier_benchmark not found' >&2; exit 127; }}; "
        f"taskset -c {CORE_SLICES[shard]} \"$mt\" --server={REDIS_IP} --port={port} --protocol=redis "
        f"--clients={MEMTIER_CLIENTS} --threads={MEMTIER_THREADS} --test-time={MEMTIER_TEST_TIME} "
        f"--ratio={memtier_args['ratio']} --pipeline={MEMTIER_PIPELINE} "
        f"--key-maximum={MEMTIER_KEY_MAX // REDIS_SHARDS} --key-pattern={memtier_args['key_pattern']} "
        f"{('--key-zipf-exp='+memtier_args['zipf']) if memtier_args['zipf'] else ''} "
        f"--key-prefix='{prefix}' --distinct-client-seed --rate-limiting={per_conn} "
        f"--hdr-file-prefix=logs_{DATE_TAG}/memtier_{PROFILE}_qps_{qps}_shard{shard} "
        f"{('--data-size-list='+memtier_args['data_list']) if memtier_args['data_list'] else ('--data-size='+str(memtier_args['data_size']))} "
        f"--hide-histogram "
        f"> logs_{DATE_TAG}/memtier_{PROFILE}_qps_{qps}_shard{shard}.log 2>&1"
    )
    return ssh(host, cmd)

def start_powerstat(qps):
    cmd = (
        f"cd logs_{DATE_TAG} && nohup sudo powerstat -aRn -d {POWER_STAT_DELAY} 1 50 "
        f"> powerstat_rate_{qps}.txt 2>&1 & echo $! > powerstat_pid.txt"
    )
    return ssh(SERVER_ALIAS, cmd)

def main():
    global PROFILE, GOV_LABEL
    if len(sys.argv) > 1:
        PROFILE = sys.argv[1]
    else:
        PROFILE = "ETC"
    GOV_LABEL = sys.argv[2] if len(sys.argv) > 2 else "performance"
    params = profile_params(PROFILE)

    global DATE_TAG
    DATE_TAG = time.strftime("%Y-%m-%d_%H-%M-%S")
    log_dir = Path(f"{PROFILE}_{GOV_LABEL}_redis_logs_xl170/run_{DATE_TAG}")
    log_dir.mkdir(parents=True, exist_ok=True)

    # Prep server
    ssh(SERVER_ALIAS, f"mkdir -p logs_{DATE_TAG} && sudo -n pkill redis-server || true")

    # Start shards (sequential to avoid SSH reset storms)
    for shard in range(REDIS_SHARDS):
        core = REDIS_CORES_START + shard
        start_shard(shard, core)
        time.sleep(0.1)

    time.sleep(SYNC_DELAY)

    # Preload (parallel)
    with ThreadPoolExecutor(max_workers=REDIS_SHARDS) as ex:
        futures = []
        for shard in range(REDIS_SHARDS):
            host = CLIENTS[shard % len(CLIENTS)]
            prefix = f"s{shard}:"
            memtier_arg = params['data_list'] and f"--data-size-list={params['data_list']}" or f"--data-size={params['data_size']}"
            fut = ex.submit(preload_shard, shard, host, prefix, memtier_arg)
            futures.append(fut)
        for fut in as_completed(futures):
            fut.result()

    # QPS loop
    for qps in QPS_LIST:
        start_powerstat(qps)
        time.sleep(SYNC_DELAY)
        with ThreadPoolExecutor(max_workers=REDIS_SHARDS) as ex:
            futures = []
            for shard in range(REDIS_SHARDS):
                host = CLIENTS[shard % len(CLIENTS)]
                prefix = f"s{shard}:"
                futures.append(ex.submit(run_shard_qps, shard, host, prefix, qps, params))
            for fut in as_completed(futures):
                fut.result()

    # Copy logs
    scp(f"{SERVER_ALIAS}:logs_{DATE_TAG}/powerstat_rate_*.txt", str(log_dir))
    for client in CLIENTS:
        scp(f"{client}:~/memtier/logs_{DATE_TAG}/*", str(log_dir))

    print(f"[+] Done. Logs in {log_dir}")

if __name__ == "__main__":
    main()
