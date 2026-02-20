#!/bin/bash

# Sharded Redis power/load experiment (Memtier only, no Memcached/mutilate)
# Usage: ./power-memt-redis.sh [PROFILE] [GOV_LABEL]
#   PROFILE: ETC (default) or USR
#   GOV_LABEL: performance (default) or schedutil, etc. (used in log folder name)

PROFILE="${1:-ETC}"
GOV_LABEL="${2:-performance}"

# Host aliases
SERVER_ALIAS="server_mem"
CLIENT1_ALIAS="client_mem1"
CLIENT2_ALIAS="client_mem2"
CLIENT3_ALIAS="client_mem3"

# Redis sharding config
REDIS_IP="10.10.1.1"
REDIS_SHARDS=20
REDIS_PORT_BASE=7000
REDIS_CORES_START=0           # shard i pins to core (REDIS_CORES_START + i)
REDIS_MAXMEM="0"              # 0 = unlimited; set e.g. 64gb for cache-like
REDIS_MAXMEM_POLICY="noeviction"
REDIS_IO_THREADS=4
REDIS_IO_THREADS_DO_READS="yes"

# Memtier (load generator)
# To reduce client-side oversubscription, run 2 threads per shard and pin each
# memtier process to a unique 2-core slice on the client host.
MEMTIER_THREADS=2
MEMTIER_CLIENTS=50
MEMTIER_PIPELINE=32
MEMTIER_RATIO="1:9"           # SET:GET (overridden per profile)
MEMTIER_KEY_MAX=5000000
MEMTIER_KEY_PATTERN="R:R"
MEMTIER_ZIPF_EXP="1.2"
MEMTIER_DATA_SIZE=2
MEMTIER_DATA_SIZE_LIST=""
MEMTIER_DATA_SIZE_LIST_ETC="1:583,2:17820,3:9239,4:18,5:2740,6:65,7:606,8:23,9:837,10:837,11:8989,12:92,13:326,14:1980,22:4200,45:6821,91:10397,181:12790,362:11421,724:6768,1448:2598,2896:683,5793:136,11585:23,23170:3,46341:1,92682:1,185364:1,370728:1,741455:1"
MEMTIER_TEST_TIME=60

# Workload selection
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
    echo "[!!] Unknown profile '$PROFILE' (use ETC or USR)"
    exit 1
    ;;
esac

# Sweep QPS targets (total across all shards)
QPS_LIST=(
  300000
)

SYNC_DELAY=10
POWER_STAT_DELAY=15
DO_LOADONLY=true

DATE_TAG=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_DIR="${PROFILE}_${GOV_LABEL}_redis_logs_xl170/run_${DATE_TAG}"
mkdir -p "$LOG_DIR"

SEPARATOR="------------------------------------------------------------"

# === SSH Helpers ===
ssh_with_retry() {
  local max_tries=10
  local count=0
  while ! ssh "$@"; do
    count=$((count + 1))
    if [ "$count" -ge "$max_tries" ]; then
      echo "[!!] SSH failed after $count tries: $*" >&2
      return 1
    fi
    echo "[!] SSH failed. Retrying again ..."
    sleep 2
  done
}

scp_with_retry() {
  local max_tries=10
  local count=0
  while ! scp "$@"; do
    count=$((count + 1))
    if [ "$count" -ge "$max_tries" ]; then
      echo "[!!] SCP failed after $count tries: $*" >&2
      return 1
    fi
    echo "[!] SCP failed. Retrying in 2s..."
    sleep 2
  done
}

echo "$SEPARATOR"
echo "[*] Opening persistent SSH connections..."
ssh -fN "$SERVER_ALIAS"
ssh -fN "$CLIENT1_ALIAS"
ssh -fN "$CLIENT2_ALIAS"
ssh -fN "$CLIENT3_ALIAS"

# === Start Redis shards on server ===
echo "$SEPARATOR"
echo "[*] Starting Redis shards on server..."
ssh_with_retry "$SERVER_ALIAS" <<EOF
  set -e
  mkdir -p logs_$DATE_TAG
  cd logs_$DATE_TAG
  sudo -n pkill redis-server || true
  sleep 2
EOF

shard=0
core=$REDIS_CORES_START
while [ $shard -lt $REDIS_SHARDS ]; do
  port=$((REDIS_PORT_BASE + shard))
  ssh_with_retry "$SERVER_ALIAS" <<EOF
    set -e
    cd logs_$DATE_TAG
    nohup taskset -c \$core redis-server \
      --bind $REDIS_IP --port \$port \
      --save '' --appendonly no \
      --maxmemory $REDIS_MAXMEM --maxmemory-policy $REDIS_MAXMEM_POLICY \
      --io-threads $REDIS_IO_THREADS --io-threads-do-reads $REDIS_IO_THREADS_DO_READS \
      > redis_shard_\$port.log 2>&1 &
EOF
  shard=$((shard + 1))
  core=$((core + 1))
done

echo "$SEPARATOR"
echo "[*] Sleeping for warm-up and sync (${SYNC_DELAY}s)..."
sleep "$SYNC_DELAY"

# === Optional preload ===
if $DO_LOADONLY; then
  echo "$SEPARATOR"
  echo "[*] Preloading Redis shards across clients..."
  HOSTS=("$CLIENT1_ALIAS" "$CLIENT2_ALIAS" "$CLIENT3_ALIAS")
  shard=0
  pids=()
  for port in $(seq $REDIS_PORT_BASE $((REDIS_PORT_BASE + REDIS_SHARDS - 1))); do
    h=${HOSTS[$((shard % 3))]}
    prefix="s${shard}:"
    ssh_with_retry "$h" <<EOF &
      set -e
      mkdir -p ~/memtier/logs_$DATE_TAG
      cd ~/memtier
      memtier_benchmark \
        --server=$REDIS_IP --port=$port --protocol=redis \
        --clients=$MEMTIER_CLIENTS --threads=$MEMTIER_THREADS \
        --ratio=1:0 \
        --key-maximum=$((MEMTIER_KEY_MAX / REDIS_SHARDS)) \
        --key-pattern=S:S \
        --key-prefix="$prefix" \
        --requests=allkeys \
        $( [ -n "$MEMTIER_DATA_SIZE_LIST" ] && echo "--data-size-list=$MEMTIER_DATA_SIZE_LIST" || echo "--data-size=$MEMTIER_DATA_SIZE" ) \
        --hide-histogram \
        > logs_$DATE_TAG/memtier_preload_${PROFILE}_shard${shard}.log 2>&1
EOF
    pids+=($!)
    shard=$((shard + 1))
  done
  for pid in "${pids[@]}"; do wait "$pid"; done
  DO_LOADONLY=false
fi

# === QPS sweep ===
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
  echo "[*] Running memtier for sharded Redis... (QPS total=$QPS)"
  HOSTS=("$CLIENT1_ALIAS" "$CLIENT2_ALIAS" "$CLIENT3_ALIAS")
  CORE_SLICES=( "0-1" "2-3" "4-5" "6-7" "8-9" "10-11" "12-13" "14-15" "16-17" "18-19"
                "0-1" "2-3" "4-5" "6-7" "8-9" "10-11" "12-13" "14-15" "16-17" "18-19" )
  pids=()
  shard=0
  for port in $(seq $REDIS_PORT_BASE $((REDIS_PORT_BASE + REDIS_SHARDS - 1))); do
    h=${HOSTS[$((shard % 3))]}
    core_slice=${CORE_SLICES[$shard]}
    prefix="s${shard}:"
    qps_shard=$(( (QPS + REDIS_SHARDS - 1) / REDIS_SHARDS ))
    total_conns=$((MEMTIER_THREADS * MEMTIER_CLIENTS))
    per_conn_rate=$(( (qps_shard + total_conns - 1) / total_conns ))
ssh_with_retry "$h" <<EOF &
      set -e
      mkdir -p ~/memtier/logs_$DATE_TAG
      cd ~/memtier
      taskset -c $core_slice memtier_benchmark \
        --server=$REDIS_IP --port=$port --protocol=redis \
        --clients=$MEMTIER_CLIENTS --threads=$MEMTIER_THREADS \
        --test-time=$MEMTIER_TEST_TIME \
        --ratio=$MEMTIER_RATIO \
        --pipeline=$MEMTIER_PIPELINE \
        --key-maximum=$((MEMTIER_KEY_MAX / REDIS_SHARDS)) \
        --key-pattern=$MEMTIER_KEY_PATTERN \
        --key-zipf-exp=$MEMTIER_ZIPF_EXP \
        --key-prefix="$prefix" \
        --distinct-client-seed \
        --rate-limiting=$per_conn_rate \
        --hdr-file-prefix=logs_$DATE_TAG/memtier_${PROFILE}_qps_${QPS}_shard${shard} \
        --hide-histogram \
        $( [ -n "$MEMTIER_DATA_SIZE_LIST" ] && echo "--data-size-list=$MEMTIER_DATA_SIZE_LIST" || echo "--data-size=$MEMTIER_DATA_SIZE" ) \
        > logs_$DATE_TAG/memtier_${PROFILE}_qps_${QPS}_shard${shard}.log 2>&1
EOF
    pids+=($!)
    shard=$((shard + 1))
  done
  for pid in "${pids[@]}"; do wait "$pid"; done
done

echo "$SEPARATOR"
echo "[*] Copying logs to local directory..."
scp_with_retry "$SERVER_ALIAS":logs_$DATE_TAG/powerstat_rate_*.txt "$LOG_DIR"/
scp_with_retry "$CLIENT1_ALIAS":~/memtier/logs_$DATE_TAG/* "$LOG_DIR"/ || true
scp_with_retry "$CLIENT2_ALIAS":~/memtier/logs_$DATE_TAG/* "$LOG_DIR"/ || true
scp_with_retry "$CLIENT3_ALIAS":~/memtier/logs_$DATE_TAG/* "$LOG_DIR"/ || true

echo "$SEPARATOR"
echo "[*] Running data.py for parsing the logs..."
python3 data-intel.py "$LOG_DIR" "$DATE_TAG"

echo "$SEPARATOR"
echo "[*] Experiment finished successfully!"
