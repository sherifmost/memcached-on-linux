#!/bin/bash
# Single-Redis experiment (no sharding). Uses memtier on up to 3 clients.
# This version adds:
#  - per-process percentiles in memtier logs (--print-percentiles)
#  - correct global percentiles by merging HDR histogram .txt files across clients

set -euo pipefail

PROFILE="${1:-ETC}"
GOV_LABEL="${2:-performance}"
MACHINE_LABEL="${3:-xl170}"
SERVER_ALIAS="${4:-server_mem}"
CLIENT1_ALIAS="${5:-client_mem1}"
CLIENT2_ALIAS="${6:-client_mem2}"
CLIENT3_ALIAS="${7:-client_mem3}"
REDIS_IP="${8:-10.10.1.1}"
REDIS_PORT="${9:-6379}"
REDIS_CORE_RANGE="${10:-0-19}"
MEMTIER_CORE_RANGE="${11:-0-3}"

# ---------------- Redis settings ----------------
REDIS_MAXMEM="0"
REDIS_MAXMEM_POLICY="noeviction"
REDIS_IO_THREADS=4
REDIS_IO_THREADS_DO_READS="yes"

# ---------------- memtier settings (measurement) ----------------
MEMTIER_THREADS=4
MEMTIER_CLIENTS=50
MEMTIER_PIPELINE=32

# Per-process percentiles printed into memtier log (sanity check only)
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

# Global arrays (Bash 3.2 friendly)
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

# ---------------- Start Redis ----------------
echo "$SEPARATOR"
echo "[*] Starting Redis on server (daemonize + verify)..."

ssh_with_retry "$SERVER_ALIAS" "mkdir -p logs_$DATE_TAG && cd logs_$DATE_TAG && sudo -n pkill redis-server || true"
ssh_with_retry "$SERVER_ALIAS" "cd logs_$DATE_TAG && command -v redis-server >/dev/null || { echo 'redis-server not found'; exit 127; }"
ssh_with_retry "$SERVER_ALIAS" "cd logs_$DATE_TAG && redis-server --version || true"

IO_FLAGS="--io-threads $REDIS_IO_THREADS --io-threads-do-reads $REDIS_IO_THREADS_DO_READS"

ssh_with_retry "$SERVER_ALIAS" "cd logs_$DATE_TAG && \
  rm -f redis.pid && \
  taskset -c $REDIS_CORE_RANGE redis-server \
    --bind 0.0.0.0 --protected-mode no \
    --port $REDIS_PORT \
    --save \"\" --appendonly no \
    --maxmemory $REDIS_MAXMEM --maxmemory-policy $REDIS_MAXMEM_POLICY \
    $IO_FLAGS \
    --daemonize yes \
    --pidfile \$(pwd)/redis.pid \
    --logfile \$(pwd)/redis_server.log"

ssh_with_retry "$SERVER_ALIAS" "cd logs_$DATE_TAG && timeout 8 bash -lc '
  for i in {1..40}; do
    redis-cli -h 127.0.0.1 -p $REDIS_PORT ping >/dev/null 2>&1 && exit 0
    sleep 0.2
  done
  echo \"[!!] Redis did not respond to PING\" >&2
  tail -n 120 redis_server.log >&2
  exit 1
'"

echo "$SEPARATOR"
echo "[*] Sleeping for warm-up (${SYNC_DELAY}s)..."
sleep "$SYNC_DELAY"

# ---------------- Preflight on clients ----------------
for h in "$CLIENT1_ALIAS" "$CLIENT2_ALIAS" "$CLIENT3_ALIAS"; do
  ssh_with_retry "$h" "command -v taskset >/dev/null || { echo '[!!] taskset not found'; exit 127; }"
  ssh_with_retry "$h" "command -v memtier_benchmark >/dev/null || [ -x \$HOME/memtier/memtier_benchmark ] || { echo '[!!] memtier_benchmark not found (PATH or ~/memtier/memtier_benchmark)'; exit 127; }"
done

# ---------------- Preload ----------------
if $DO_LOADONLY; then
  echo "$SEPARATOR"
  echo "[*] Preloading Redis dataset (partitioned ranges, P:P, -n allkeys, 1-based keys)..."

  hosts=("$CLIENT1_ALIAS" "$CLIENT2_ALIAS" "$CLIENT3_ALIAS")

  total_keys=$MEMTIER_KEY_COUNT
  base=$((total_keys / 3))
  rem=$((total_keys % 3))

  part0=$base; part1=$base; part2=$base
  [ $rem -ge 1 ] && part0=$((part0 + 1))
  [ $rem -ge 2 ] && part1=$((part1 + 1))

  kmin0=$MEMTIER_KEY_MIN_ID
  kmax0=$((kmin0 + part0 - 1))

  kmin1=$((kmax0 + 1))
  kmax1=$((kmin1 + part1 - 1))

  kmin2=$((kmax1 + 1))
  kmax2=$MEMTIER_KEY_MAX_ID

  KMINS=("$kmin0" "$kmin1" "$kmin2")
  KMAXS=("$kmax0" "$kmax1" "$kmax2")

  for idx in 0 1 2; do
    h="${hosts[$idx]}"
    kmin="${KMINS[$idx]}"
    kmax="${KMAXS[$idx]}"
    keys_in_partition=$((kmax - kmin + 1))

    echo "[*] Preload client $idx on $h: keys [$kmin..$kmax] (N=$keys_in_partition), conns=$((PRELOAD_THREADS*PRELOAD_CLIENTS)), mode=allkeys"

    if [ -n "$MEMTIER_DATA_SIZE_LIST" ]; then
      data_arg_primary="--data-size-list=$MEMTIER_DATA_SIZE_LIST"
      data_arg_fallback="-d 181"
    else
      data_arg_primary="-d $MEMTIER_DATA_SIZE"
      data_arg_fallback="-d $MEMTIER_DATA_SIZE"
    fi

    remote_log="~/memtier_logs/$DATE_TAG/preload/memtier_preload_${PROFILE}_${idx}.log"
    remote_cmd="set -e; \
      LOGROOT=\$HOME/memtier_logs/$DATE_TAG/preload; mkdir -p \$LOGROOT; ulimit -n 200000; \
      MBIN=\$(command -v memtier_benchmark 2>/dev/null || echo \$HOME/memtier/memtier_benchmark); \
      ( taskset -c $MEMTIER_CORE_RANGE \$MBIN \
          -s $REDIS_IP -p $REDIS_PORT -P redis \
          -c $PRELOAD_CLIENTS -t $PRELOAD_THREADS \
          --pipeline=$PRELOAD_PIPELINE \
          --ratio=1:0 \
          --key-pattern=P:P \
          --key-minimum=$kmin --key-maximum=$kmax \
          --key-prefix=\"$KEY_PREFIX\" \
          $data_arg_primary \
          -n allkeys \
          --distinct-client-seed \
          --hide-histogram \
        || \
        taskset -c $MEMTIER_CORE_RANGE \$MBIN \
          -s $REDIS_IP -p $REDIS_PORT -P redis \
          -c $PRELOAD_CLIENTS -t $PRELOAD_THREADS \
          --pipeline=$PRELOAD_PIPELINE \
          --ratio=1:0 \
          --key-pattern=P:P \
          --key-minimum=$kmin --key-maximum=$kmax \
          --key-prefix=\"$KEY_PREFIX\" \
          $data_arg_fallback \
          -n allkeys \
          --distinct-client-seed \
          --hide-histogram \
      ) > \$LOGROOT/memtier_preload_${PROFILE}_${idx}.log 2>&1"

    ( ssh_with_retry "$h" "$remote_cmd" ) &
    PIDS+=("$!")
    PHOSTS+=("$h")
    PLOGS+=("$remote_log")
  done

  wait_jobs_or_report "preload"
  DO_LOADONLY=false
  echo "[*] Preload done."
fi

# ---------------- QPS loop ----------------
echo "$SEPARATOR"
echo "[*] Starting QPS loop..."
for QPS in "${QPS_LIST[@]}"; do
  echo "$SEPARATOR"
  echo "[*] QPS: $QPS"

  echo "$SEPARATOR"
  echo "[*] Starting powerstat on server..."
  ssh_with_retry "$SERVER_ALIAS" "nohup sudo powerstat -aRn -d $POWER_STAT_DELAY 1 50 > logs_$DATE_TAG/powerstat_rate_${QPS}.txt 2>&1 & echo \$! > powerstat_pid.txt"

  echo "$SEPARATOR"
  echo "[*] Sleeping for warm-up and sync (${SYNC_DELAY}s)..."
  sleep "$SYNC_DELAY"

  echo "$SEPARATOR"
  echo "[*] Starting memtier clients..."

  hosts=("$CLIENT1_ALIAS" "$CLIENT2_ALIAS" "$CLIENT3_ALIAS")

  for idx in 0 1 2; do
    h="${hosts[$idx]}"

    total_conns=$((MEMTIER_THREADS * MEMTIER_CLIENTS))
    per_conn=$(( (QPS/3 + total_conns - 1) / total_conns ))

    if [ -n "$MEMTIER_DATA_SIZE_LIST" ]; then
      data_arg_primary="--data-size-list=$MEMTIER_DATA_SIZE_LIST"
      data_arg_fallback="-d 181"
    else
      data_arg_primary="-d $MEMTIER_DATA_SIZE"
      data_arg_fallback="-d $MEMTIER_DATA_SIZE"
    fi

    # If Zipf workload, pass exponent
    if [[ "$MEMTIER_KEY_PATTERN" == Z:* ]]; then
      zipf_arg="--key-zipf-exp=$MEMTIER_ZIPF_EXP"
    else
      zipf_arg=""
    fi

    remote_log="~/memtier_logs/$DATE_TAG/qps_${QPS}/memtier_${PROFILE}_qps_${QPS}_c${idx}.log"
    remote_cmd="set -e; \
      LOGROOT=\$HOME/memtier_logs/$DATE_TAG/qps_${QPS}; mkdir -p \$LOGROOT; ulimit -n 200000; \
      MBIN=\$(command -v memtier_benchmark 2>/dev/null || echo \$HOME/memtier/memtier_benchmark); \
      ( taskset -c $MEMTIER_CORE_RANGE \$MBIN \
          -s $REDIS_IP -p $REDIS_PORT -P redis \
          -c $MEMTIER_CLIENTS -t $MEMTIER_THREADS \
          --test-time=$MEMTIER_TEST_TIME \
          --ratio=$MEMTIER_RATIO \
          --pipeline=$MEMTIER_PIPELINE \
          --key-minimum=$MEMTIER_KEY_MIN_ID --key-maximum=$MEMTIER_KEY_MAX_ID \
          --key-pattern=$MEMTIER_KEY_PATTERN \
          $zipf_arg \
          --key-prefix=\"$KEY_PREFIX\" \
          --distinct-client-seed \
          --rate-limiting=$per_conn \
          --print-percentiles=$MEMTIER_PRINT_PCT \
          --hdr-file-prefix=\$LOGROOT/memtier_${PROFILE}_qps_${QPS}_c${idx} \
          $data_arg_primary \
          --hide-histogram \
        || \
        taskset -c $MEMTIER_CORE_RANGE \$MBIN \
          -s $REDIS_IP -p $REDIS_PORT -P redis \
          -c $MEMTIER_CLIENTS -t $MEMTIER_THREADS \
          --test-time=$MEMTIER_TEST_TIME \
          --ratio=$MEMTIER_RATIO \
          --pipeline=$MEMTIER_PIPELINE \
          --key-minimum=$MEMTIER_KEY_MIN_ID --key-maximum=$MEMTIER_KEY_MAX_ID \
          --key-pattern=$MEMTIER_KEY_PATTERN \
          $zipf_arg \
          --key-prefix=\"$KEY_PREFIX\" \
          --distinct-client-seed \
          --rate-limiting=$per_conn \
          --print-percentiles=$MEMTIER_PRINT_PCT \
          --hdr-file-prefix=\$LOGROOT/memtier_${PROFILE}_qps_${QPS}_c${idx} \
          $data_arg_fallback \
          --hide-histogram \
      ) > \$LOGROOT/memtier_${PROFILE}_qps_${QPS}_c${idx}.log 2>&1"

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

# memtier's HDR .txt files are produced by --hdr-file-prefix. We look recursively in qps_* subfolders.
pat = os.path.join(log_dir, "**", f"memtier_{profile}_qps_*_c*.txt") if profile else os.path.join(log_dir, "**", "memtier_*_qps_*_c*.txt")
files = sorted(glob.glob(pat, recursive=True))

if not files:
    print(f"[!!] No HDR .txt files found under: {pat}")
    print("     Check that memtier created histogram files (needs --hdr-file-prefix).")
    raise SystemExit(0)

def parse_hdr_txt(path):
    """
    Parse HdrHistogram 'text' percentile distribution format (GoogleCharts-compatible),
    reconstruct per-bucket counts by differencing TotalCount.
    Returns: dict[value]->count, total_count
    """
    s = open(path, "r", errors="ignore").read()
    # normalize whitespace
    toks = re.split(r"\s+", s.strip())
    # find header start
    # Expect: Value Percentile TotalCount 1/(1-Percentile)
    try:
        i0 = toks.index("Value")
    except ValueError:
        raise ValueError(f"Not an HDR text file? missing 'Value' header: {path}")
    # move to first data row (skip 4 header tokens)
    i = i0 + 4

    counts = defaultdict(int)
    prev_tc = 0

    while i + 3 < len(toks):
        t = toks[i]
        if t.startswith("#[") or t.startswith("#") or t.startswith("Mean") or t.startswith("["):
            break

        # value, percentile, totalCount, 1/(1-p)  (the 4th can be 'inf')
        v = float(toks[i])
        # p = float(toks[i+1])  # not needed for merging
        tc_raw = toks[i+2]

        # Some implementations print tc as integer; be defensive
        try:
            tc = int(tc_raw)
        except ValueError:
            tc = int(float(tc_raw))

        delta = tc - prev_tc
        if delta < 0:
            # shouldn't happen, but don't crash hard
            delta = 0
        counts[v] += delta
        prev_tc = tc
        i += 4

    return counts, prev_tc

def quantile_from_counts(counts_by_value, q):
    items = sorted(counts_by_value.items(), key=lambda x: x[0])
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

# group by (qps, op)
groups = defaultdict(list)
qps_re = re.compile(r"_qps_(\d+)_", re.IGNORECASE)

for f in files:
    base = os.path.basename(f)
    m = qps_re.search(base)
    if not m:
        continue
    qps = int(m.group(1))
    up = base.upper()
    if "GET" in up:
        op = "GET"
    elif "SET" in up:
        op = "SET"
    else:
        op = "ALL"
    groups[(qps, op)].append(f)

# quantiles to report
quantiles = [
    (0.50, "p50"),
    (0.90, "p90"),
    (0.95, "p95"),
    (0.99, "p99"),
    (0.999, "p99.9"),
    (0.9999, "p99.99"),
]

out_csv = os.path.join(log_dir, f"merged_percentiles_{profile or 'ALL'}.csv")
out_txt = os.path.join(log_dir, f"merged_percentiles_{profile or 'ALL'}.txt")

rows = []
for (qps, op), flist in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1])):
    merged = defaultdict(int)
    total = 0
    for f in flist:
        c, t = parse_hdr_txt(f)
        total += t
        for v, cnt in c.items():
            merged[v] += cnt

    # If totals differ across files (they will), the merged total should equal sum of deltas,
    # not sum of per-file totals. Use sum(merged) as authoritative:
    merged_total = sum(merged.values())

    rec = {"qps": qps, "op": op, "samples": merged_total, "files": len(flist)}
    for q, name in quantiles:
        rec[name] = quantile_from_counts(merged, q)
    rows.append(rec)

# write CSV
with open(out_csv, "w", newline="") as fp:
    w = csv.writer(fp)
    header = ["qps", "op", "samples", "files"] + [name for _, name in quantiles]
    w.writerow(header)
    for r in rows:
        w.writerow([r.get(h,"") for h in header])

# write TXT
with open(out_txt, "w") as fp:
    fp.write(f"merged percentiles (profile={profile or 'ALL'}) from HDR .txt histograms\n")
    fp.write("NOTE: values are in the same units memtier used in its histogram files.\n\n")
    for r in rows:
        fp.write(f"QPS={r['qps']}  OP={r['op']}  samples={r['samples']}  files={r['files']}\n")
        fp.write("  " + "  ".join([f"{name}={r[name]}" for _, name in quantiles]) + "\n\n")

print(f"[*] Wrote:\n    {out_csv}\n    {out_txt}")
PY
PYTHON_RC=$?

# ---------------- Parse memtier logs for per-client actual QPS and percentiles ----------------
echo "$SEPARATOR"
echo "[*] Parsing memtier client logs into aggregated_memtier.csv ..."
python3 parse_memtier_results.py "$LOG_DIR" --outdir "$(pwd)" || true

echo "$SEPARATOR"
echo "[*] Done. Logs in $LOG_DIR"
exit $PYTHON_RC
