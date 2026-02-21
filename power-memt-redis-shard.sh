#!/bin/bash
# Sharded Redis experiment: NUM_SHARDS redis-server instances, one per core, clients talk to different shards.
# Outputs:
#  - server: logs_$DATE_TAG/shards/redis_<port>.log + pidfiles
#  - clients: ~/memtier_logs/$DATE_TAG/qps_<QPS>/memtier_<...>.log + HDR *.txt
#  - local: merged_percentiles_<PROFILE>.csv/.txt in $LOG_DIR

set -euo pipefail

PROFILE="${1:-ETC}"
GOV_LABEL="${2:-performance}"
MACHINE_LABEL="${3:-xl170}"
SERVER_ALIAS="${4:-server_mem}"
CLIENT1_ALIAS="${5:-client_mem1}"
CLIENT2_ALIAS="${6:-client_mem2}"
CLIENT3_ALIAS="${7:-client_mem3}"
REDIS_IP="${8:-10.10.1.1}"
BASE_PORT="${9:-6379}"              # NOTE: base port (shards use BASE_PORT+i)
SERVER_CORE_RANGE="${10:-0-19}"     # used only for sanity; shards are pinned per-core
MEMTIER_CORE_RANGE="${11:-0-3}"

# ---------------- Sharding settings ----------------
NUM_SHARDS=20
REDIS_ENABLE_IO_THREADS=false       # recommended false when each shard pinned to 1 core

# ---------------- Redis settings ----------------
REDIS_MAXMEM="0"
REDIS_MAXMEM_POLICY="noeviction"
REDIS_IO_THREADS=4
REDIS_IO_THREADS_DO_READS="yes"

# ---------------- memtier settings (measurement) ----------------
MEMTIER_THREADS=4
MEMTIER_CLIENTS=50
MEMTIER_PIPELINE=32
MEMTIER_PRINT_PCT="50,90,95,99,99.9,99.99"

# 1-based keyspace due to memtier constraint: key-minimum must be > 0
MEMTIER_KEY_COUNT=5000000
MEMTIER_KEY_MIN_ID=1
MEMTIER_KEY_MAX_ID=$MEMTIER_KEY_COUNT

MEMTIER_TEST_TIME=60
MEMTIER_RATIO="1:30"
MEMTIER_KEY_PATTERN="Z:Z"
MEMTIER_ZIPF_EXP="1.2"

# ETC value-size distribution (weights list)
MEMTIER_DATA_SIZE_LIST_ETC="1:583,2:17820,3:9239,4:18,5:2740,6:65,7:606,8:23,9:837,10:837,11:8989,12:92,13:326,14:1980,22:4200,45:6821,91:10397,181:12790,362:11421,724:6768,1448:2598,2896:683,5793:136,11585:23,23170:3,46341:1,92682:1,185364:1,370728:1,741455:1"
MEMTIER_DATA_SIZE_LIST=""
MEMTIER_DATA_SIZE=2

# ---------------- preload tuning ----------------
PRELOAD_THREADS=2
PRELOAD_CLIENTS=4
PRELOAD_PIPELINE=64

KEY_PREFIX="k:"

case "$PROFILE" in
  ETC)
    MEMTIER_RATIO="1:30"
    MEMTIER_KEY_PATTERN="Z:Z"
    MEMTIER_DATA_SIZE_LIST="$MEMTIER_DATA_SIZE_LIST_ETC"
    ;;
  USR)
    MEMTIER_RATIO="1:500"
    MEMTIER_KEY_PATTERN="R:R"
    MEMTIER_DATA_SIZE=2
    MEMTIER_DATA_SIZE_LIST=""
    ;;
  *)
    echo "[!!] Unknown profile '$PROFILE' (use ETC or USR)" >&2
    exit 1
    ;;
esac

QPS_LIST=(300000)
SYNC_DELAY=10
POWER_STAT_DELAY=15
DO_LOADONLY=true

DATE_TAG=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_DIR="${PROFILE}_${GOV_LABEL}_redis_sharded_${NUM_SHARDS}_${MACHINE_LABEL}/run_${DATE_TAG}"
mkdir -p "$LOG_DIR"
SEPARATOR="------------------------------------------------------------"

ssh_opts=(
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o BatchMode=yes
  -o ConnectTimeout=5
  -o LogLevel=ERROR
)

ssh_run() {
  local host="$1"; shift
  ssh "${ssh_opts[@]}" "$host" "$@"
}

ssh_with_retry() {
  local host="$1"; shift
  local tries=5 n=0
  while ! ssh_run "$host" "$@"; do
    n=$((n+1))
    if [ "$n" -ge "$tries" ]; then
      echo "[!!] Remote command failed on $host after $tries attempts:" >&2
      echo "     $*" >&2
      return 1
    fi
    echo "[!] Remote command failed on $host, retrying... ($n/$tries)" >&2
    sleep 2
  done
}

scp_safe() { scp "${ssh_opts[@]}" "$@"; }

# ---------- key range for shard i (1-based) ----------
# shard i owns keys: [ floor(KEY_COUNT*i/NUM_SHARDS)+1 , floor(KEY_COUNT*(i+1)/NUM_SHARDS) ]
shard_kmin() { local i="$1"; echo $(( (MEMTIER_KEY_COUNT * i) / NUM_SHARDS + 1 )); }
shard_kmax() { local i="$1"; echo $(( (MEMTIER_KEY_COUNT * (i+1)) / NUM_SHARDS )); }

# ---------- shard assignment across 3 clients ----------
# split contiguous ranges: client0 gets [0..s0], client1 [..], client2 [..]
base=$((NUM_SHARDS / 3))
rem=$((NUM_SHARDS % 3))
c0=$base; c1=$base; c2=$base
[ $rem -ge 1 ] && c0=$((c0+1))
[ $rem -ge 2 ] && c1=$((c1+1))
s0_end=$((c0-1))
s1_start=$((s0_end+1))
s1_end=$((s1_start + c1 - 1))
s2_start=$((s1_end+1))
s2_end=$((NUM_SHARDS-1))

CLIENTS=("$CLIENT1_ALIAS" "$CLIENT2_ALIAS" "$CLIENT3_ALIAS")
S_STARTS=(0 "$s1_start" "$s2_start")
S_ENDS=("$s0_end" "$s1_end" "$s2_end")

# ---------------- Start Redis shards ----------------
echo "$SEPARATOR"
echo "[*] Starting $NUM_SHARDS Redis shards on server (one per core, ports $BASE_PORT..$((BASE_PORT+NUM_SHARDS-1)))..."

IO_FLAGS=""
if $REDIS_ENABLE_IO_THREADS; then
  IO_FLAGS="--io-threads $REDIS_IO_THREADS --io-threads-do-reads $REDIS_IO_THREADS_DO_READS"
fi

# Start all shards in ONE remote command (faster than 20 ssh calls)
ssh_with_retry "$SERVER_ALIAS" "
  set -e
  mkdir -p logs_$DATE_TAG/shards
  cd logs_$DATE_TAG/shards
  sudo -n pkill redis-server || true
  sleep 1
  for i in \$(seq 0 $((NUM_SHARDS-1))); do
    port=\$(( $BASE_PORT + i ))
    core=\$i
    rm -f redis_\${port}.pid redis_\${port}.log
    taskset -c \${core} redis-server \
      --bind 0.0.0.0 --protected-mode no \
      --port \${port} \
      --save \"\" --appendonly no \
      --maxmemory $REDIS_MAXMEM --maxmemory-policy $REDIS_MAXMEM_POLICY \
      $IO_FLAGS \
      --daemonize yes \
      --pidfile \$(pwd)/redis_\${port}.pid \
      --logfile \$(pwd)/redis_\${port}.log
  done

  # quick sanity check: ping a few shards
  for i in 0 5 10 15 $((NUM_SHARDS-1)); do
    port=\$(( $BASE_PORT + i ))
    redis-cli -h 127.0.0.1 -p \${port} ping | grep -q PONG
  done
"

echo "$SEPARATOR"
echo "[*] Sleeping for warm-up (${SYNC_DELAY}s)..."
sleep "$SYNC_DELAY"

# ---------------- Preflight on clients ----------------
for h in "${CLIENTS[@]}"; do
  ssh_with_retry "$h" "command -v taskset >/dev/null || { echo '[!!] taskset not found'; exit 127; }"
  ssh_with_retry "$h" "command -v memtier_benchmark >/dev/null || [ -x \$HOME/memtier/memtier_benchmark ] || { echo '[!!] memtier_benchmark not found (PATH or ~/memtier/memtier_benchmark)'; exit 127; }"
done

# ---------------- Preload (per shard, distributed across clients) ----------------
if $DO_LOADONLY; then
  echo "$SEPARATOR"
  echo "[*] Preloading all shards (each shard loads only its key-range)..."

  # Decide size args
  if [ -n "$MEMTIER_DATA_SIZE_LIST" ]; then
    PRELOAD_DATA_ARG_PRIMARY="--data-size-list=$MEMTIER_DATA_SIZE_LIST"
    PRELOAD_DATA_ARG_FALLBACK="-d 181"
  else
    PRELOAD_DATA_ARG_PRIMARY="-d $MEMTIER_DATA_SIZE"
    PRELOAD_DATA_ARG_FALLBACK="-d $MEMTIER_DATA_SIZE"
  fi

  # Run one remote command per client: loops over its shard range and runs memtier per shard
  for ci in 0 1 2; do
    host="${CLIENTS[$ci]}"
    sstart="${S_STARTS[$ci]}"
    send="${S_ENDS[$ci]}"

    ssh_with_retry "$host" "
      set -e
      LOGROOT=\$HOME/memtier_logs/$DATE_TAG/preload
      mkdir -p \$LOGROOT
      ulimit -n 200000
      MBIN=\$(command -v memtier_benchmark 2>/dev/null || echo \$HOME/memtier/memtier_benchmark)
      MAX_PAR=4
      pids=()

      for shard in \$(seq $sstart $send); do
        port=\$(( $BASE_PORT + shard ))
        kmin=\$(( ($MEMTIER_KEY_COUNT * shard) / $NUM_SHARDS + 1 ))
        kmax=\$(( ($MEMTIER_KEY_COUNT * (shard+1)) / $NUM_SHARDS ))

        (
          taskset -c $MEMTIER_CORE_RANGE \$MBIN \
            -s $REDIS_IP -p \$port -P redis \
            -c $PRELOAD_CLIENTS -t $PRELOAD_THREADS \
            --pipeline=$PRELOAD_PIPELINE \
            --ratio=1:0 \
            --key-pattern=P:P \
            --key-minimum=\$kmin --key-maximum=\$kmax \
            --key-prefix=\"$KEY_PREFIX\" \
            $PRELOAD_DATA_ARG_PRIMARY \
            -n allkeys \
            --distinct-client-seed \
            --hide-histogram \
          || \
          taskset -c $MEMTIER_CORE_RANGE \$MBIN \
            -s $REDIS_IP -p \$port -P redis \
            -c $PRELOAD_CLIENTS -t $PRELOAD_THREADS \
            --pipeline=$PRELOAD_PIPELINE \
            --ratio=1:0 \
            --key-pattern=P:P \
            --key-minimum=\$kmin --key-maximum=\$kmax \
            --key-prefix=\"$KEY_PREFIX\" \
            $PRELOAD_DATA_ARG_FALLBACK \
            -n allkeys \
            --distinct-client-seed \
            --hide-histogram \
        ) > \$LOGROOT/preload_shard_\${shard}_port_\${port}.log 2>&1 &

        pids+=(\$!)
        if [ \${#pids[@]} -ge \$MAX_PAR ]; then
          for p in \${pids[@]}; do wait \$p; done
          pids=()
        fi
      done

      for p in \${pids[@]}; do wait \$p; done
    " &
  done
  wait

  DO_LOADONLY=false
  echo "[*] Preload done."
fi

# ---------------- QPS loop (per shard) ----------------
echo "$SEPARATOR"
echo "[*] Starting QPS loop (per-shard memtier)..."

for QPS in "${QPS_LIST[@]}"; do
  echo "$SEPARATOR"
  echo "[*] QPS_TOTAL=$QPS across $NUM_SHARDS shards"

  SERVER_QDIR="logs_$DATE_TAG/qps_${QPS}"
  ssh_with_retry "$SERVER_ALIAS" "mkdir -p $SERVER_QDIR"

  # powerstat (one for whole server)
  ssh_with_retry "$SERVER_ALIAS" "cd $SERVER_QDIR && nohup sudo powerstat -aRn -d $POWER_STAT_DELAY 1 50 > powerstat_rate_${QPS}.txt 2>&1 & echo \$! > powerstat_pid.txt"

  echo "[*] Warm-up ${SYNC_DELAY}s..."
  sleep "$SYNC_DELAY"

  # Per-shard rate limiting
  QPS_PER_SHARD=$(( (QPS + NUM_SHARDS - 1) / NUM_SHARDS ))
  TOTAL_CONNS=$((MEMTIER_THREADS * MEMTIER_CLIENTS))
  PER_CONN=$(( (QPS_PER_SHARD + TOTAL_CONNS - 1) / TOTAL_CONNS ))

  if [ -n "$MEMTIER_DATA_SIZE_LIST" ]; then
    RUN_DATA_ARG_PRIMARY="--data-size-list=$MEMTIER_DATA_SIZE_LIST"
    RUN_DATA_ARG_FALLBACK="-d 181"
  else
    RUN_DATA_ARG_PRIMARY="-d $MEMTIER_DATA_SIZE"
    RUN_DATA_ARG_FALLBACK="-d $MEMTIER_DATA_SIZE"
  fi

  ZIPF_ARG=""
  if [[ "$MEMTIER_KEY_PATTERN" == Z:* ]]; then
    ZIPF_ARG="--key-zipf-exp=$MEMTIER_ZIPF_EXP"
  fi

  # Run one remote command per client: launches memtier per shard in parallel on that host
  for ci in 0 1 2; do
    host="${CLIENTS[$ci]}"
    sstart="${S_STARTS[$ci]}"
    send="${S_ENDS[$ci]}"

    ssh_with_retry "$host" "
      set -e
      LOGROOT=\$HOME/memtier_logs/$DATE_TAG/qps_${QPS}
      mkdir -p \$LOGROOT
      ulimit -n 200000
      MBIN=\$(command -v memtier_benchmark 2>/dev/null || echo \$HOME/memtier/memtier_benchmark)
      MAX_PAR=8
      pids=()

      for shard in \$(seq $sstart $send); do
        port=\$(( $BASE_PORT + shard ))
        kmin=\$(( ($MEMTIER_KEY_COUNT * shard) / $NUM_SHARDS + 1 ))
        kmax=\$(( ($MEMTIER_KEY_COUNT * (shard+1)) / $NUM_SHARDS ))

        (
          taskset -c $MEMTIER_CORE_RANGE \$MBIN \
            -s $REDIS_IP -p \$port -P redis \
            -c $MEMTIER_CLIENTS -t $MEMTIER_THREADS \
            --test-time=$MEMTIER_TEST_TIME \
            --ratio=$MEMTIER_RATIO \
            --pipeline=$MEMTIER_PIPELINE \
            --key-minimum=\$kmin --key-maximum=\$kmax \
            --key-pattern=$MEMTIER_KEY_PATTERN $ZIPF_ARG \
            --key-prefix=\"$KEY_PREFIX\" \
            --distinct-client-seed \
            --rate-limiting=$PER_CONN \
            --print-percentiles=$MEMTIER_PRINT_PCT \
            --hdr-file-prefix=\$LOGROOT/memtier_${PROFILE}_qps_${QPS}_sh\${shard}_c${ci} \
            $RUN_DATA_ARG_PRIMARY \
            --hide-histogram \
          || \
          taskset -c $MEMTIER_CORE_RANGE \$MBIN \
            -s $REDIS_IP -p \$port -P redis \
            -c $MEMTIER_CLIENTS -t $MEMTIER_THREADS \
            --test-time=$MEMTIER_TEST_TIME \
            --ratio=$MEMTIER_RATIO \
            --pipeline=$MEMTIER_PIPELINE \
            --key-minimum=\$kmin --key-maximum=\$kmax \
            --key-pattern=$MEMTIER_KEY_PATTERN $ZIPF_ARG \
            --key-prefix=\"$KEY_PREFIX\" \
            --distinct-client-seed \
            --rate-limiting=$PER_CONN \
            --print-percentiles=$MEMTIER_PRINT_PCT \
            --hdr-file-prefix=\$LOGROOT/memtier_${PROFILE}_qps_${QPS}_sh\${shard}_c${ci} \
            $RUN_DATA_ARG_FALLBACK \
            --hide-histogram \
        ) > \$LOGROOT/memtier_${PROFILE}_qps_${QPS}_sh\${shard}_c${ci}.log 2>&1 &

        pids+=(\$!)
        if [ \${#pids[@]} -ge \$MAX_PAR ]; then
          for p in \${pids[@]}; do wait \$p; done
          pids=()
        fi
      done

      for p in \${pids[@]}; do wait \$p; done
    " &
  done
  wait

  echo "[*] Completed QPS_TOTAL=$QPS (each shard target ~${QPS_PER_SHARD} req/s)."
done

# ---------------- Copy logs ----------------
echo "$SEPARATOR"
echo "[*] Copying logs..."
scp_safe -r "$SERVER_ALIAS:logs_$DATE_TAG/qps_*" "$LOG_DIR/" || true
scp_safe -r "$SERVER_ALIAS:logs_$DATE_TAG/shards" "$LOG_DIR/" || true

for h in "${CLIENTS[@]}"; do
  scp_safe -r "$h:~/memtier_logs/$DATE_TAG/qps_*" "$LOG_DIR/" || true
  scp_safe -r "$h:~/memtier_logs/$DATE_TAG/preload" "$LOG_DIR/" || true
done

# ---------------- Merge percentiles across ALL shards+clients (from HDR .txt) ----------------
echo "$SEPARATOR"
echo "[*] Merging HDR histogram .txt files across all shards+clients -> global percentiles..."

LOG_DIR_ABS="$LOG_DIR" PROFILE_ENV="$PROFILE" python3 - <<'PY'
import os, re, math, csv, glob
from collections import defaultdict

log_dir = os.environ["LOG_DIR_ABS"]
profile = os.environ["PROFILE_ENV"].strip()

# Recursive: sharded hdr files are under qps_<QPS>/memtier_<PROFILE>_qps_<QPS>_sh<shard>_c<client>*.txt
pat = os.path.join(log_dir, "**", f"memtier_{profile}_qps_*_sh*_c*.txt")
files = sorted(glob.glob(pat, recursive=True))

if not files:
    print(f"[!!] No HDR .txt files found under: {pat}")
    raise SystemExit(1)

def parse_hdr_txt(path):
    s = open(path, "r", errors="ignore").read()
    toks = re.split(r"\s+", s.strip())
    i0 = toks.index("Value")
    i = i0 + 4
    counts = defaultdict(int)
    prev_tc = 0
    while i + 3 < len(toks):
        t = toks[i]
        if t.startswith("#") or t.startswith("[") or t.startswith("Mean"):
            break
        v = float(toks[i])
        tc = int(float(toks[i+2]))
        delta = tc - prev_tc
        if delta < 0:
            delta = 0
        counts[v] += delta
        prev_tc = tc
        i += 4
    return counts

def quantile(counts_by_value, q):
    items = sorted(counts_by_value.items())
    total = sum(c for _, c in items)
    if total == 0:
        return float("nan")
    target = int(math.ceil(q * total))
    run = 0
    for v, c in items:
        run += c
        if run >= target:
            return v
    return items[-1][0]

qps_re = re.compile(r"_qps_(\d+)_", re.I)

# group by (qps, op)
groups = defaultdict(list)
for f in files:
    base = os.path.basename(f).upper()
    m = qps_re.search(base)
    if not m: 
        continue
    qps = int(m.group(1))
    op = "GET" if "GET" in base else ("SET" if "SET" in base else "ALL")
    groups[(qps, op)].append(f)

quantiles = [(0.50,"p50"),(0.90,"p90"),(0.95,"p95"),(0.99,"p99"),(0.999,"p99.9"),(0.9999,"p99.99")]

out_csv = os.path.join(log_dir, f"merged_percentiles_{profile}.csv")
out_txt = os.path.join(log_dir, f"merged_percentiles_{profile}.txt")

rows=[]
for (qps, op), flist in sorted(groups.items(), key=lambda x:(x[0][0], x[0][1])):
    merged = defaultdict(int)
    for f in flist:
        c = parse_hdr_txt(f)
        for v, cnt in c.items():
            merged[v] += cnt
    samples = sum(merged.values())
    rec = {"qps":qps,"op":op,"samples":samples,"files":len(flist)}
    for q,name in quantiles:
        rec[name]=quantile(merged,q)
    rows.append(rec)

with open(out_csv,"w",newline="") as fp:
    w=csv.writer(fp)
    header=["qps","op","samples","files"]+[n for _,n in quantiles]
    w.writerow(header)
    for r in rows:
        w.writerow([r[h] for h in header])

with open(out_txt,"w") as fp:
    fp.write(f"merged percentiles (profile={profile}) across all shards+clients\n\n")
    for r in rows:
        fp.write(f"QPS={r['qps']} OP={r['op']} samples={r['samples']} files={r['files']}\n")
        fp.write("  "+"  ".join([f\"{n}={r[n]}\" for _,n in quantiles])+"\n\n")

print("[*] Wrote:")
print("   ", out_csv)
print("   ", out_txt)
PY

echo "$SEPARATOR"
echo "[*] Done. Logs in $LOG_DIR"