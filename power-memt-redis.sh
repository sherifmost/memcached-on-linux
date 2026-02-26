#!/bin/bash
# Sharded-Redis experiment (minimal deltas from your single-instance script).
# Key behavior:
#   - NUM_SHARDS = number of cores in REDIS_CORE_RANGE (e.g., 0-19 => 20 shards)
#   - Starts one redis-server per shard pinned to one core, ports REDIS_PORT..REDIS_PORT+NUM_SHARDS-1
#   - Clients talk to different shards; each shard owns a disjoint key-range
# IMPORTANT CHANGE (per your last request):
#   - QPS_LIST values are interpreted as PER-SHARD targets (TOTAL ~= QPS * NUM_SHARDS)

set -euo pipefail

PROFILE="${1:-ETC}"
GOV_LABEL="${2:-performance}"
MACHINE_LABEL="${3:-xl170}"
SERVER_ALIAS="${4:-server_mem}"
CLIENT1_ALIAS="${5:-client_mem1}"
CLIENT2_ALIAS="${6:-client_mem2}"
CLIENT3_ALIAS="${7:-client_mem3}"
REDIS_IP="${8:-10.10.1.1}"
REDIS_PORT="${9:-6379}"          # BASE PORT for shards
REDIS_CORE_RANGE="${10:-0-19}"   # expects "A-B"
MEMTIER_CORE_RANGE="${11:-0-19}" # recommended wide to avoid client bottleneck

# ---------------- Redis settings ----------------
REDIS_MAXMEM="0"
REDIS_MAXMEM_POLICY="noeviction"

# For 1-core-per-shard pinning, disable IO threads (they can create contention on the same core)
REDIS_IO_THREADS=1
REDIS_IO_THREADS_DO_READS="no"

# ---------------- memtier settings (measurement) ----------------
MEMTIER_THREADS=2
MEMTIER_CLIENTS=16
MEMTIER_PIPELINE=16
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
PRELOAD_CLIENTS=8
PRELOAD_PIPELINE=64

# IMPORTANT: same prefix used in preload + measurement
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

# IMPORTANT: interpreted as PER-SHARD QPS targets
QPS_LIST=(10000 30000 50000 70000 90000 100000 200000 300000)

SYNC_DELAY=10
POWER_STAT_DELAY=15
DO_LOADONLY=true

DATE_TAG=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_DIR="${PROFILE}_${GOV_LABEL}_redis_single_logs_${MACHINE_LABEL}/run_${DATE_TAG}"
mkdir -p "$LOG_DIR"
SEPARATOR="------------------------------------------------------------"

ssh_opts=(
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o BatchMode=yes
  -o ConnectTimeout=5
  -o LogLevel=ERROR
)

ssh_run() { local host="$1"; shift; ssh "${ssh_opts[@]}" "$host" "$@"; }

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

PIDS=()
PHOSTS=()
PLOGS=()

wait_jobs_or_report() {
  local phase="$1"
  local fail=0 rc=0
  set +e
  for i in "${!PIDS[@]}"; do
    wait "${PIDS[$i]}"
    rc=$?
    if [ $rc -ne 0 ]; then
      echo "[!!] ${phase} failed on host=${PHOSTS[$i]} (rc=$rc)" >&2
      echo "[!!] Remote log: ${PLOGS[$i]}" >&2
      ssh_run "${PHOSTS[$i]}" "echo '----- LOG HEAD -----'; head -n 60 ${PLOGS[$i]} 2>/dev/null || true; echo '----- LOG TAIL -----'; tail -n 160 ${PLOGS[$i]} 2>/dev/null || true" >&2
      fail=1
    fi
  done
  set -e
  [ $fail -eq 0 ] || exit 1
  PIDS=(); PHOSTS=(); PLOGS=()
}

# ---------------- Sharding derived from REDIS_CORE_RANGE ----------------
CORE_START=0
CORE_END=19
if echo "$REDIS_CORE_RANGE" | grep -Eq '^[0-9]+-[0-9]+$'; then
  CORE_START="${REDIS_CORE_RANGE%-*}"
  CORE_END="${REDIS_CORE_RANGE#*-}"
fi
NUM_SHARDS=$((CORE_END - CORE_START + 1))

# shard key range (1-based): [ floor(K*i/N)+1 , floor(K*(i+1)/N) ]
shard_kmin() { local i="$1"; echo $(( (MEMTIER_KEY_COUNT * i) / NUM_SHARDS + 1 )); }
shard_kmax() { local i="$1"; echo $(( (MEMTIER_KEY_COUNT * (i+1)) / NUM_SHARDS )); }

# shard assignment across 3 clients: contiguous ranges
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

# ---------------- Start Redis (SHARDED) ----------------
echo "$SEPARATOR"
echo "[*] Starting Redis shards on server (daemonize + verify)..."
echo "    NUM_SHARDS=$NUM_SHARDS  BASE_PORT=$REDIS_PORT  CORES=${CORE_START}-${CORE_END}"

ssh_with_retry "$SERVER_ALIAS" "mkdir -p logs_$DATE_TAG && cd logs_$DATE_TAG && sudo -n pkill redis-server || true"
ssh_with_retry "$SERVER_ALIAS" "cd logs_$DATE_TAG && command -v redis-server >/dev/null || { echo 'redis-server not found'; exit 127; }"
ssh_with_retry "$SERVER_ALIAS" "cd logs_$DATE_TAG && redis-server --version || true"

# Disable IO threads for pinned shards
IO_FLAGS=""

ssh_with_retry "$SERVER_ALIAS" "cd logs_$DATE_TAG && \
  rm -rf shards && mkdir -p shards && \
  rm -f redis.pid redis_server.log redis_workers.yml && \
  echo \"servers:\" > redis_workers.yml && \
  ( \
    echo \"Starting $NUM_SHARDS shards at \$(date)\"; \
    for i in \$(seq 0 $((NUM_SHARDS-1))); do \
      port=\$(( $REDIS_PORT + i )); \
      core=\$(( $CORE_START + i )); \
      inst_dir=\$(pwd)/shards/inst_\${i}; \
      mkdir -p \"\$inst_dir\"; \
      conf=\"\$inst_dir/redis.conf\"; \
      pidf=\"\$inst_dir/redis.pid\"; \
      logf=\"\$inst_dir/redis.log\"; \
      sockf=\"\$inst_dir/redis.sock\"; \
      cat > \"\$conf\" <<CONF\n\
bind 0.0.0.0\n\
protected-mode no\n\
port \$port\n\
unixsocket \$sockf\n\
unixsocketperm 775\n\
daemonize yes\n\
supervised no\n\
save \"\"\n\
appendonly no\n\
loglevel notice\n\
maxclients 1000000\n\
pidfile \$pidf\n\
logfile \$logf\n\
maxmemory $REDIS_MAXMEM\n\
maxmemory-policy $REDIS_MAXMEM_POLICY\n\
CONF\n\
      taskset -c \${core} redis-server \"\$conf\" $IO_FLAGS; \
      echo \"shard=\$i core=\$core port=\$port pidfile=\$pidf\"; \
      echo \"  - \$sockf\" >> redis_workers.yml; \
    done \
  ) > redis_server.log 2>&1 && \
  ( for i in \$(seq 0 $((NUM_SHARDS-1))); do port=\$(( $REDIS_PORT + i )); cat \"shards/inst_\${i}/redis.pid\"; done ) > redis.pid"

echo "$SEPARATOR"
echo "[*] Sleeping for warm-up (${SYNC_DELAY}s)..."
sleep "$SYNC_DELAY"

# ---------------- Preflight on clients ----------------
for h in "$CLIENT1_ALIAS" "$CLIENT2_ALIAS" "$CLIENT3_ALIAS"; do
  ssh_with_retry "$h" "command -v taskset >/dev/null || { echo '[!!] taskset not found'; exit 127; }"
  ssh_with_retry "$h" "command -v memtier_benchmark >/dev/null || [ -x \$HOME/memtier/memtier_benchmark ] || { echo '[!!] memtier_benchmark not found (PATH or ~/memtier/memtier_benchmark)'; exit 127; }"
done

# ---------------- Preload (SHARDED) ----------------
if $DO_LOADONLY; then
  echo "$SEPARATOR"
  echo "[*] Preloading Redis dataset (per-shard key ranges; clients talk to different shards)..."

  if [ -n "$MEMTIER_DATA_SIZE_LIST" ]; then
    data_arg_primary="--data-size-list=$MEMTIER_DATA_SIZE_LIST"
    data_arg_fallback="-d 181"
  else
    data_arg_primary="-d $MEMTIER_DATA_SIZE"
    data_arg_fallback="-d $MEMTIER_DATA_SIZE"
  fi

  for ci in 0 1 2; do
    h="${CLIENTS[$ci]}"
    sstart="${S_STARTS[$ci]}"
    send="${S_ENDS[$ci]}"

    remote_log="~/memtier_logs/$DATE_TAG/preload/driver_preload_c${ci}.log"
    remote_cmd="set -e; \
      LOGROOT=\$HOME/memtier_logs/$DATE_TAG/preload; mkdir -p \$LOGROOT; ulimit -n 200000; \
      MBIN=\$(command -v memtier_benchmark 2>/dev/null || echo \$HOME/memtier/memtier_benchmark); \
      echo \"driver client=${ci} shards=${sstart}..${send}\" > \$LOGROOT/driver_preload_c${ci}.log; \
      for shard in \$(seq ${sstart} ${send}); do \
        port=\$(( ${REDIS_PORT} + shard )); \
        kmin=\$(( (${MEMTIER_KEY_COUNT} * shard) / ${NUM_SHARDS} + 1 )); \
        kmax=\$(( (${MEMTIER_KEY_COUNT} * (shard+1)) / ${NUM_SHARDS} )); \
        ( taskset -c ${MEMTIER_CORE_RANGE} \$MBIN \
            -s ${REDIS_IP} -p \$port -P redis \
            -c ${PRELOAD_CLIENTS} -t ${PRELOAD_THREADS} \
            --pipeline=${PRELOAD_PIPELINE} \
            --ratio=1:0 \
            --key-pattern=P:P \
            --key-minimum=\$kmin --key-maximum=\$kmax \
            --key-prefix=\"${KEY_PREFIX}\" \
            ${data_arg_primary} \
            -n allkeys \
            --distinct-client-seed \
            --hide-histogram \
          || \
          taskset -c ${MEMTIER_CORE_RANGE} \$MBIN \
            -s ${REDIS_IP} -p \$port -P redis \
            -c ${PRELOAD_CLIENTS} -t ${PRELOAD_THREADS} \
            --pipeline=${PRELOAD_PIPELINE} \
            --ratio=1:0 \
            --key-pattern=P:P \
            --key-minimum=\$kmin --key-maximum=\$kmax \
            --key-prefix=\"${KEY_PREFIX}\" \
            ${data_arg_fallback} \
            -n allkeys \
            --distinct-client-seed \
            --hide-histogram \
        ) > \$LOGROOT/memtier_preload_${PROFILE}_c${ci}_sh\${shard}.log 2>&1; \
      done; \
      echo \"driver done\" > \$LOGROOT/driver_done_c${ci}.txt"

    ( ssh_with_retry "$h" "$remote_cmd" ) &
    PIDS+=("$!")
    PHOSTS+=("$h")
    PLOGS+=("$remote_log")
  done

  wait_jobs_or_report "preload"
  DO_LOADONLY=false
  echo "[*] Preload done."
fi

# ---------------- QPS loop (SHARDED memtier) ----------------
echo "$SEPARATOR"
echo "[*] Starting QPS loop..."
for QPS in "${QPS_LIST[@]}"; do
  echo "$SEPARATOR"
  echo "[*] QPS (PER SHARD): $QPS  |  TOTAL ≈ $((QPS * NUM_SHARDS)) across $NUM_SHARDS shards"

  echo "$SEPARATOR"
  echo "[*] Starting powerstat on server..."
  ssh_with_retry "$SERVER_ALIAS" "nohup sudo powerstat -aRn -d $POWER_STAT_DELAY 1 50 > logs_$DATE_TAG/powerstat_rate_${QPS}.txt 2>&1 & echo \$! > powerstat_pid.txt"

  echo "$SEPARATOR"
  echo "[*] Sleeping for warm-up and sync (${SYNC_DELAY}s)..."
  sleep "$SYNC_DELAY"

  echo "$SEPARATOR"
  echo "[*] Starting memtier clients (sharded)..."

  # IMPORTANT: interpret QPS_LIST values as per-shard targets
  QPS_PER_SHARD=$QPS

  total_conns=$((MEMTIER_THREADS * MEMTIER_CLIENTS))
  per_conn=$(( (QPS_PER_SHARD + total_conns - 1) / total_conns ))

  if [ -n "$MEMTIER_DATA_SIZE_LIST" ]; then
    data_arg_primary="--data-size-list=$MEMTIER_DATA_SIZE_LIST"
    data_arg_fallback="-d 181"
  else
    data_arg_primary="-d $MEMTIER_DATA_SIZE"
    data_arg_fallback="-d $MEMTIER_DATA_SIZE"
  fi

  if [[ "$MEMTIER_KEY_PATTERN" == Z:* ]]; then
    zipf_arg="--key-zipf-exp=$MEMTIER_ZIPF_EXP"
  else
    zipf_arg=""
  fi

  for ci in 0 1 2; do
    h="${CLIENTS[$ci]}"
    sstart="${S_STARTS[$ci]}"
    send="${S_ENDS[$ci]}"

    remote_log="~/memtier_logs/$DATE_TAG/qps_${QPS}/driver_qps_c${ci}.log"
    remote_cmd="set -e; \
      LOGROOT=\$HOME/memtier_logs/$DATE_TAG/qps_${QPS}; mkdir -p \$LOGROOT; ulimit -n 200000; \
      MBIN=\$(command -v memtier_benchmark 2>/dev/null || echo \$HOME/memtier/memtier_benchmark); \
      echo \"driver client=${ci} shards=${sstart}..${send} qps_per_shard=${QPS_PER_SHARD} per_conn=${per_conn}\" > \$LOGROOT/driver_qps_c${ci}.log; \
      pids=(); \
      for shard in \$(seq ${sstart} ${send}); do \
        port=\$(( ${REDIS_PORT} + shard )); \
        kmin=\$(( (${MEMTIER_KEY_COUNT} * shard) / ${NUM_SHARDS} + 1 )); \
        kmax=\$(( (${MEMTIER_KEY_COUNT} * (shard+1)) / ${NUM_SHARDS} )); \
        ( \
          ( taskset -c ${MEMTIER_CORE_RANGE} \$MBIN \
              -s ${REDIS_IP} -p \$port -P redis \
              -c ${MEMTIER_CLIENTS} -t ${MEMTIER_THREADS} \
              --test-time=${MEMTIER_TEST_TIME} \
              --ratio=${MEMTIER_RATIO} \
              --pipeline=${MEMTIER_PIPELINE} \
              --key-minimum=\$kmin --key-maximum=\$kmax \
              --key-pattern=${MEMTIER_KEY_PATTERN} \
              ${zipf_arg} \
              --key-prefix=\"${KEY_PREFIX}\" \
              --distinct-client-seed \
              --rate-limiting=${per_conn} \
              --print-percentiles=${MEMTIER_PRINT_PCT} \
              --hdr-file-prefix=\$LOGROOT/memtier_${PROFILE}_qps_${QPS}_c${ci}_sh\${shard} \
              ${data_arg_primary} \
              --hide-histogram \
            || \
            taskset -c ${MEMTIER_CORE_RANGE} \$MBIN \
              -s ${REDIS_IP} -p \$port -P redis \
              -c ${MEMTIER_CLIENTS} -t ${MEMTIER_THREADS} \
              --test-time=${MEMTIER_TEST_TIME} \
              --ratio=${MEMTIER_RATIO} \
              --pipeline=${MEMTIER_PIPELINE} \
              --key-minimum=\$kmin --key-maximum=\$kmax \
              --key-pattern=${MEMTIER_KEY_PATTERN} \
              ${zipf_arg} \
              --key-prefix=\"${KEY_PREFIX}\" \
              --distinct-client-seed \
              --rate-limiting=${per_conn} \
              --print-percentiles=${MEMTIER_PRINT_PCT} \
              --hdr-file-prefix=\$LOGROOT/memtier_${PROFILE}_qps_${QPS}_c${ci}_sh\${shard} \
              ${data_arg_fallback} \
              --hide-histogram \
          ) > \$LOGROOT/memtier_${PROFILE}_qps_${QPS}_c${ci}_sh\${shard}.log 2>&1 \
        ) & \
        pids+=(\$!); \
      done; \
      for p in \${pids[@]}; do wait \$p; done; \
      echo \"driver done\" > \$LOGROOT/driver_done_c${ci}.txt"

    ( ssh_with_retry "$h" "$remote_cmd" ) &
    PIDS+=("$!")
    PHOSTS+=("$h")
    PLOGS+=("$remote_log")
  done

  wait_jobs_or_report "qps_run_${QPS}"
done

# ---------------- Copy logs ----------------
echo "$SEPARATOR"
echo "[*] Copying logs..."
scp_safe "$SERVER_ALIAS:logs_$DATE_TAG/powerstat_rate_*.txt" "$LOG_DIR/" || true
scp_safe "$SERVER_ALIAS:logs_$DATE_TAG/redis_server.log" "$LOG_DIR/" || true
scp_safe "$SERVER_ALIAS:logs_$DATE_TAG/redis.pid" "$LOG_DIR/" || true
scp_safe -r "$SERVER_ALIAS:logs_$DATE_TAG/shards" "$LOG_DIR/" || true

for h in "$CLIENT1_ALIAS" "$CLIENT2_ALIAS" "$CLIENT3_ALIAS"; do
  scp_safe -r "$h:~/memtier_logs/$DATE_TAG/qps_*" "$LOG_DIR/" || true
done

# ---------------- Merge percentiles across clients (from HDR .txt) ----------------
echo "$SEPARATOR"
echo "[*] Merging HDR histogram .txt files across all clients -> global percentiles..."

LOG_DIR_ABS="$LOG_DIR"
PROFILE_ENV="$PROFILE"

python3 - <<'PY'
import os, re, math, csv, glob
from collections import defaultdict

log_dir = os.environ.get("LOG_DIR_ABS") or os.environ.get("LOG_DIR") or "."
profile = os.environ.get("PROFILE_ENV","").strip()

pat = os.path.join(log_dir, "**", f"memtier_{profile}_qps_*_c*.txt") if profile else os.path.join(log_dir, "**", "memtier_*_qps_*_c*.txt")
files = sorted(glob.glob(pat, recursive=True))

if not files:
    print(f"[!!] No HDR .txt files found under: {pat}")
    raise SystemExit(0)

def parse_hdr_txt(path):
    s = open(path, "r", errors="ignore").read()
    toks = re.split(r"\s+", s.strip())
    i0 = toks.index("Value")
    i = i0 + 4
    counts = defaultdict(int)
    prev_tc = 0
    while i + 3 < len(toks):
        t = toks[i]
        if t.startswith("#") or t.startswith("Mean") or t.startswith("["):
            break
        v = float(toks[i])
        tc = int(float(toks[i+2]))
        delta = tc - prev_tc
        if delta < 0: delta = 0
        counts[v] += delta
        prev_tc = tc
        i += 4
    return counts

def quantile_from_counts(counts_by_value, q):
    items = sorted(counts_by_value.items())
    total = sum(c for _, c in items)
    if total == 0: return float("nan")
    target = int(math.ceil(q * total))
    run = 0
    for v, c in items:
        run += c
        if run >= target:
            return v
    return items[-1][0]

groups = defaultdict(list)
qps_re = re.compile(r"_qps_(\d+)_", re.I)

for f in files:
    base = os.path.basename(f)
    m = qps_re.search(base)
    if not m: continue
    qps = int(m.group(1))
    up = base.upper()
    op = "GET" if "GET" in up else ("SET" if "SET" in up else "ALL")
    groups[(qps, op)].append(f)

quantiles = [(0.50,"p50"),(0.90,"p90"),(0.95,"p95"),(0.99,"p99"),(0.999,"p99.9"),(0.9999,"p99.99")]

out_csv = os.path.join(log_dir, f"merged_percentiles_{profile or 'ALL'}.csv")
out_txt = os.path.join(log_dir, f"merged_percentiles_{profile or 'ALL'}.txt")

rows=[]
for (qps, op), flist in sorted(groups.items(), key=lambda x:(x[0][0], x[0][1])):
    merged = defaultdict(int)
    for f in flist:
        c = parse_hdr_txt(f)
        for v, cnt in c.items():
            merged[v] += cnt
    samples = sum(merged.values())
    rec={"qps":qps,"op":op,"samples":samples,"files":len(flist)}
    for q,name in quantiles:
        rec[name]=quantile_from_counts(merged,q)
    rows.append(rec)

with open(out_csv,"w",newline="") as fp:
    w=csv.writer(fp)
    header=["qps","op","samples","files"]+[n for _,n in quantiles]
    w.writerow(header)
    for r in rows:
        w.writerow([r.get(h,"") for h in header])

with open(out_txt,"w") as fp:
    fp.write(f"merged percentiles (profile={profile or 'ALL'}) from HDR .txt histograms\n\n")
    for r in rows:
        fp.write(f"QPS={r['qps']}  OP={r['op']}  samples={r['samples']}  files={r['files']}\n")
        fp.write("  "+"  ".join([f"{n}={r[n]}" for _,n in quantiles])+"\n\n")

print(f"[*] Wrote:\n    {out_csv}\n    {out_txt}")
PY
PYTHON_RC=$?

echo "$SEPARATOR"
echo "[*] Parsing memtier client logs into aggregated_memtier.csv ..."
python3 parse_memtier_results.py "$LOG_DIR" --outdir "$(pwd)" || true

echo "$SEPARATOR"
echo "[*] Done. Logs in $LOG_DIR"
exit $PYTHON_RC
