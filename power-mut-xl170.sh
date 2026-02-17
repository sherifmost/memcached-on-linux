#!/bin/bash

# === Configuration selection ===
PROFILE="${1:-ETC}"          # ETC (default) or USR
GOV_LABEL="${2:-performance}" # frequency governor label
ENGINE="${3:-MEMCACHED}"      # Possible Options: MEMCACHED (default), REDIS (single), REDIS_SHARDED

# Common host settings
SERVER_ALIAS="server_mem"
CLIENT1_ALIAS="client_mem1"
CLIENT2_ALIAS="client_mem2"
CLIENT3_ALIAS="client_mem3"   # also used as mutilate master

MEMCACHED_IP="10.10.1.1"
MEMCACHED_PORT=11211
MEMCACHED_THREADS=20
MEMCACHED_CORE_RANGE="0-19"
MEMCACHED_MAX_CONNECTIONS=50000

# Redis settings (used when ENGINE=REDIS)
REDIS_IP="10.10.1.1"
REDIS_PORT=6379
REDIS_CORE_RANGE="0-19"
REDIS_SHARDS=20              # used when ENGINE=REDIS_SHARDED
REDIS_PORT_BASE=7000
REDIS_CORES_START=0          # shards will pin incrementally from here
REDIS_MAXMEM="0"               # 0 = unlimited; set e.g., 64gb for cache-like
REDIS_MAXMEM_POLICY="noeviction"
REDIS_IO_THREADS=4
REDIS_IO_THREADS_DO_READS="yes"

MEMTIER_THREADS=8
MEMTIER_CLIENTS=50
MEMTIER_PIPELINE=32
MEMTIER_RATIO="1:9"      # set:get; adjust per workload
MEMTIER_CORE_RANGE="12-15"
MEMTIER_KEY_MAX=5000000
MEMTIER_KEY_PATTERN="R:R"
MEMTIER_ZIPF_EXP="1.2"
MEMTIER_DATA_SIZE=2
MEMTIER_DATA_SIZE_LIST=""
MEMTIER_DATA_SIZE_LIST_ETC="1:583,2:17820,3:9239,4:18,5:2740,6:65,7:606,8:23,9:837,10:837,11:8989,12:92,13:326,14:1980,22:4200,45:6821,91:10397,181:12790,362:11421,724:6768,1448:2598,2896:683,5793:136,11585:23,23170:3,46341:1,92682:1,185364:1,370728:1,741455:1"

# Agent side (3 hosts) - overridden per profile if needed
MUTILATE_AGENT_THREADS=15
MUTILATE_AGENT_CORE_RANGE="0-14"   # leave upper cores free for master
MUTILATE_AGENT_CLIENTS=96
MUTILATE_AGENT_DEPTH=16
MUTILATE_TEST_DURATION=60
MUTILATE_IADIST="exponential"      # default Poisson arrivals
MUTILATE_KEYSIZE="fb_key"
MUTILATE_VALUESIZE="fb_value"
MUTILATE_UPDATE_RATIO=0.0333       # ETC default
DO_LOADONLY=true
LOAD_THREADS=4

# Profile-specific tweaks
case "$PROFILE" in
  ETC)
    # Facebook ETC: read-heavy, Poisson arrivals (already set)
    MEMTIER_RATIO="1:30"
    MEMTIER_KEY_PATTERN="Z:Z"
    MEMTIER_DATA_SIZE_LIST="$MEMTIER_DATA_SIZE_LIST_ETC"
    REDIS_MAXMEM="0"
    REDIS_MAXMEM_POLICY="noeviction"
    ;;
  USR)
    # USR-like: fixed key/value sizes, low write ratio
    MUTILATE_KEYSIZE="fixed:21"
    MUTILATE_VALUESIZE="fixed:2"
    MUTILATE_UPDATE_RATIO=0.002   # or raise to 0.018 if you want more misses
    # MUTILATE_IADIST="fb_ia"
    MEMTIER_RATIO="1:500"
    MEMTIER_KEY_PATTERN="R:R"
    MEMTIER_DATA_SIZE=2
    MEMTIER_DATA_SIZE_LIST=""
    REDIS_MAXMEM="0"
    REDIS_MAXMEM_POLICY="noeviction"
    ;;
  *)
    echo "[!!] Unknown profile '$PROFILE' (use ETC or USR)"
    exit 1
    ;;
esac

MUTILATE_AGENT_1_ALIAS="10.10.1.2"
MUTILATE_AGENT_2_ALIAS="10.10.1.3"
MUTILATE_AGENT_3_ALIAS="localhost"

# Master side (on CLIENT3_ALIAS)
MUTILATE_MASTER_THREADS=4
MUTILATE_MASTER_CLIENTS=96
MUTILATE_MASTER_QPS=1000           # measurement stream (low-rate)
MUTILATE_MASTER_DEPTH=16           # open-loop measurement pipeline
MASTER_CORE_RANGE="15-18"

# QPS sweep list
QPS_LIST=(
  50000  100000 150000 200000 250000
  300000 350000 400000 450000 500000
  550000 600000 650000 700000 750000
  800000 850000 900000 950000 1000000
  1050000 1100000 1150000 1200000 1250000
  1300000 1350000 1400000 1450000 1500000
)

SYNC_DELAY=10
POWER_STAT_DELAY=15

DATE_TAG=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_DIR="${PROFILE}_${GOV_LABEL}_logs_xl170/run_${DATE_TAG}"
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

echo "$SEPARATOR"
  echo "[*] Starting server process and powerstat on server..."
  if [ "$ENGINE" = "MEMCACHED" ]; then
ssh_with_retry "$SERVER_ALIAS" <<EOF
  set -e
  mkdir -p logs_$DATE_TAG
  cd logs_$DATE_TAG

  sudo -n pkill memcached || true
  sudo -n pkill powerstat || true
  sleep 2

  echo "[*] Launching memcached with retries..."
  MAX_RETRIES=5
  COUNT=0
  while [ \$COUNT -lt \$MAX_RETRIES ]; do
    echo "[*] Attempt \$((COUNT + 1)) to start memcached..."
    nohup taskset -c $MEMCACHED_CORE_RANGE memcached -u nobody -l $MEMCACHED_IP -t $MEMCACHED_THREADS -c $MEMCACHED_MAX_CONNECTIONS > memcached_server.log 2>&1 &
    sleep 2
    if sudo netstat -tuln | grep $MEMCACHED_IP; then
      echo "[*] Memcached is running on port $MEMCACHED_PORT"
      break
    fi
    echo "[!] Memcached failed to start. Retrying..."
    sudo -n pkill memcached || true
    sleep 2
    COUNT=\$((COUNT + 1))
  done
  if [ \$COUNT -eq \$MAX_RETRIES ]; then
    echo "[!!] Memcached failed to start after \$MAX_RETRIES attempts"
    exit 1
  fi
EOF
elif [ "$ENGINE" = "REDIS" ]; then
ssh_with_retry "$SERVER_ALIAS" <<EOF
  set -e
  mkdir -p logs_$DATE_TAG
  cd logs_$DATE_TAG
  sudo -n pkill redis-server || true
  sleep 2
  nohup taskset -c $REDIS_CORE_RANGE redis-server \\
    --bind $REDIS_IP --port $REDIS_PORT \\
    --save '' --appendonly no \\
    --maxmemory $REDIS_MAXMEM --maxmemory-policy $REDIS_MAXMEM_POLICY \\
    --io-threads $REDIS_IO_THREADS --io-threads-do-reads $REDIS_IO_THREADS_DO_READS \\
    > redis_server.log 2>&1 &
  sleep 2
  netstat -tuln | grep $REDIS_PORT || { echo "[!!] Redis failed to start"; exit 1; }
EOF
elif [ "$ENGINE" = "REDIS_SHARDED"]; then
  ssh_with_retry "$SERVER_ALIAS" <<EOF
    set -e
    mkdir -p logs_$DATE_TAG
    cd logs_$DATE_TAG
    sudo -n pkill redis-server || true
    sleep 2
EOF
  shard=0
  core=$REDIS_CORES_START
  while [ \$shard -lt $REDIS_SHARDS ]; do
    port=$((REDIS_PORT_BASE + shard))
    ssh_with_retry "$SERVER_ALIAS" <<EOF
      set -e
      cd logs_$DATE_TAG
      nohup taskset -c \$core redis-server \\
        --bind $REDIS_IP --port \$port \\
        --save '' --appendonly no \\
        --maxmemory $REDIS_MAXMEM --maxmemory-policy $REDIS_MAXMEM_POLICY \\
        --io-threads $REDIS_IO_THREADS --io-threads-do-reads $REDIS_IO_THREADS_DO_READS \\
        > redis_shard_\$port.log 2>&1 &
EOF
    shard=$((shard + 1))
    core=$((core + 1))
  done
fi
fi

echo "$SEPARATOR"
echo "[*] Sleeping for warm-up and sync (${SYNC_DELAY}s)..."
sleep "$SYNC_DELAY"

# Optional preload to avoid cold cache
if $DO_LOADONLY; then
  echo "$SEPARATOR"
  if [ "$ENGINE" = "MEMCACHED" ]; then
    echo "[*] Loading dataset (one-time) from master..."
    ssh_with_retry "$CLIENT3_ALIAS" <<EOF
      set -e
      cd ~/mutilate
      taskset -c ${MASTER_CORE_RANGE:-12-15} ./mutilate \
        -s $MEMCACHED_IP \
        -T $LOAD_THREADS \
        --loadonly \
        -K $MUTILATE_KEYSIZE \
        -V $MUTILATE_VALUESIZE \
        -u $MUTILATE_UPDATE_RATIO \
        --iadist $MUTILATE_IADIST \
        > ~/mutilate/logs_$DATE_TAG/mutilate_load.log 2>&1
EOF
  elif [ "$ENGINE" = "REDIS" ]; then
    echo "[*] Preloading Redis dataset from all clients..."
    HOSTS=("$CLIENT1_ALIAS" "$CLIENT2_ALIAS" "$CLIENT3_ALIAS")
    PREFIXES=("c1:" "c2:" "c3:")
    for idx in 0 1 2; do
      h="${HOSTS[$idx]}"; p="${PREFIXES[$idx]}"
ssh_with_retry "$h" <<EOF &
        set -e
        mkdir -p ~/memtier/logs_$DATE_TAG
        cd ~/memtier
        memtier_benchmark \
          --server=$REDIS_IP --port=$REDIS_PORT --protocol=redis \
          --threads=$MEMTIER_THREADS --clients=$MEMTIER_CLIENTS \
          --ratio=1:0 \
          --key-maximum=$MEMTIER_KEY_MAX \
          --key-pattern=S:S \
          --key-prefix="$p" \
          --requests=allkeys \
          $( [ -n "$MEMTIER_DATA_SIZE_LIST" ] && echo "--data-size-list=$MEMTIER_DATA_SIZE_LIST" || echo "--data-size=$MEMTIER_DATA_SIZE" ) \
          --hide-histogram \
          > logs_$DATE_TAG/memtier_preload_${PROFILE}_${idx}.log 2>&1
EOF
    done
    wait
  elif [ "$ENGINE" = "REDIS_SHARDED"]; then
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
          --threads=$MEMTIER_THREADS --clients=$MEMTIER_CLIENTS \
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
  fi
  DO_LOADONLY=false
fi

echo "$SEPARATOR"
echo "[*] Starting QPS loop on server..."
for QPS in "${QPS_LIST[@]}"; do
  if [ "$ENGINE" = "MEMCACHED" ]; then
    echo "$SEPARATOR"
    echo "[*] Starting mutilate agents on clients..."
    ssh_with_retry "$CLIENT1_ALIAS" <<EOF
      cd ~/mutilate
      ulimit -n 200000
      sudo -n pkill mutilate || true
      nohup taskset -c $MUTILATE_AGENT_CORE_RANGE ./mutilate -T $MUTILATE_AGENT_THREADS -c $MUTILATE_AGENT_CLIENTS -d $MUTILATE_AGENT_DEPTH -A -i $MUTILATE_IADIST > ~/agent1.log 2>&1 </dev/null &
      sleep 2
      pgrep -f "mutilate .* -A" >/dev/null || { echo "[!!] Agent not running on client1"; exit 1; }
EOF

    ssh_with_retry "$CLIENT2_ALIAS" <<EOF
      cd ~/mutilate
      ulimit -n 200000
      sudo -n pkill mutilate || true
      nohup taskset -c $MUTILATE_AGENT_CORE_RANGE ./mutilate -T $MUTILATE_AGENT_THREADS -c $MUTILATE_AGENT_CLIENTS -d $MUTILATE_AGENT_DEPTH -A -i $MUTILATE_IADIST > ~/agent2.log 2>&1 </dev/null &
      sleep 2
      pgrep -f "mutilate .* -A" >/dev/null || { echo "[!!] Agent not running on client2"; exit 1; }
EOF

    ssh_with_retry "$CLIENT3_ALIAS" <<EOF
      cd ~/mutilate
      ulimit -n 200000
      sudo -n pkill mutilate || true
      nohup taskset -c $MUTILATE_AGENT_CORE_RANGE ./mutilate -T $MUTILATE_AGENT_THREADS -c $MUTILATE_AGENT_CLIENTS -d $MUTILATE_AGENT_DEPTH -A -i $MUTILATE_IADIST > ~/agent3.log 2>&1 </dev/null &
      sleep 2
      pgrep -f "mutilate .* -A" >/dev/null || { echo "[!!] Agent not running on client3"; exit 1; }
EOF
  fi

  echo "$SEPARATOR"
  echo "[*] QPS: $QPS"

  echo "$SEPARATOR"
  echo "[*] Starting powerstat on server..."
  ssh_with_retry "$SERVER_ALIAS" "nohup sudo powerstat -aRn -d $POWER_STAT_DELAY 1 50 > logs_$DATE_TAG/powerstat_rate_${QPS}.txt 2>&1 & echo \$! > powerstat_pid.txt"

  echo "$SEPARATOR"
  echo "[*] Sleeping for warm-up and sync (${SYNC_DELAY}s)..."
  sleep "$SYNC_DELAY"

  if [ "$ENGINE" = "MEMCACHED" ]; then
    echo "$SEPARATOR"
    echo "[*] Starting mutilate master on master client..."
    ssh_with_retry "$CLIENT3_ALIAS" <<EOF
      set -e
      mkdir -p ~/mutilate/logs_$DATE_TAG
      cd ~/mutilate
      timeout $((MUTILATE_TEST_DURATION + 10)) taskset -c ${MASTER_CORE_RANGE:-12-15} ./mutilate \
        -s $MEMCACHED_IP \
        -T $MUTILATE_MASTER_THREADS \
        -C $MUTILATE_MASTER_CLIENTS \
        -D $MUTILATE_MASTER_DEPTH \
        -Q $MUTILATE_MASTER_QPS \
        -a $MUTILATE_AGENT_1_ALIAS -a $MUTILATE_AGENT_2_ALIAS -a $MUTILATE_AGENT_3_ALIAS \
        -c $MUTILATE_AGENT_CLIENTS \
        -u $MUTILATE_UPDATE_RATIO \
        -K $MUTILATE_KEYSIZE \
        -V $MUTILATE_VALUESIZE \
        -q $QPS \
        -t $MUTILATE_TEST_DURATION \
        --noload \
        --iadist $MUTILATE_IADIST \
        > logs_$DATE_TAG/mutilate_master_qps_${QPS}.log 2>&1
EOF
  elif [ "$ENGINE" = "REDIS" ]; then
    echo "$SEPARATOR"
    echo "[*] Running memtier for Redis..."
    HOSTS=("$CLIENT1_ALIAS" "$CLIENT2_ALIAS" "$CLIENT3_ALIAS")
    PREFIXES=("c1:" "c2:" "c3:")
    NUM_HOSTS=3
    TOTAL_CONNS=$((NUM_HOSTS * MEMTIER_THREADS * MEMTIER_CLIENTS))
    PER_CONN_RATE=$(( (QPS + TOTAL_CONNS - 1) / TOTAL_CONNS ))

    pids=()
    for idx in 0 1 2; do
      h="${HOSTS[$idx]}"; p="${PREFIXES[$idx]}"
ssh_with_retry "$h" <<EOF &
        set -e
        mkdir -p ~/memtier/logs_$DATE_TAG
        cd ~/memtier
        memtier_benchmark \
          --server=$REDIS_IP --port=$REDIS_PORT --protocol=redis \
          --clients=$MEMTIER_CLIENTS --threads=$MEMTIER_THREADS \
          --test-time=$MUTILATE_TEST_DURATION \
          --ratio=$MEMTIER_RATIO \
          --pipeline=$MEMTIER_PIPELINE \
          --key-maximum=$MEMTIER_KEY_MAX \
          --key-pattern=$MEMTIER_KEY_PATTERN \
          --key-zipf-exp=$MEMTIER_ZIPF_EXP \
          --key-prefix="$p" \
          --distinct-client-seed \
          --rate-limiting=$PER_CONN_RATE \
          --hdr-file-prefix=logs_$DATE_TAG/memtier_${PROFILE}_qps_${QPS}_c${idx} \
          --hide-histogram \
          $( [ -n "$MEMTIER_DATA_SIZE_LIST" ] && echo "--data-size-list=$MEMTIER_DATA_SIZE_LIST" || echo "--data-size=$MEMTIER_DATA_SIZE" ) \
          > logs_$DATE_TAG/memtier_${PROFILE}_qps_${QPS}_c${idx}.log 2>&1
EOF
      pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid"; done
  elif [ "$ENGINE" = "REDIS_SHARDED"]; then
    echo "$SEPARATOR"
    echo "[*] Running memtier for sharded Redis... (QPS total=$QPS)"
    HOSTS=("$CLIENT1_ALIAS" "$CLIENT2_ALIAS" "$CLIENT3_ALIAS")
    pids=()
    shard=0
    for port in $(seq $REDIS_PORT_BASE $((REDIS_PORT_BASE + REDIS_SHARDS - 1))); do
      h=${HOSTS[$((shard % 3))]}
      prefix="s${shard}:"
      # target per-shard QPS
      qps_shard=$(( (QPS + REDIS_SHARDS - 1) / REDIS_SHARDS ))
      total_conns=$((MEMTIER_THREADS * MEMTIER_CLIENTS))
      per_conn_rate=$(( (qps_shard + total_conns - 1) / total_conns ))
ssh_with_retry "$h" <<EOF &
        set -e
        mkdir -p ~/memtier/logs_$DATE_TAG
        cd ~/memtier
        memtier_benchmark \
          --server=$REDIS_IP --port=$port --protocol=redis \
          --clients=$MEMTIER_CLIENTS --threads=$MEMTIER_THREADS \
          --test-time=$MUTILATE_TEST_DURATION \
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
  fi
done

echo "$SEPARATOR"
echo "[*] Copying logs from server to local directory..."
scp_with_retry "$SERVER_ALIAS":logs_$DATE_TAG/powerstat_rate_*.txt "$LOG_DIR"/
scp_with_retry "$CLIENT3_ALIAS":~/mutilate/logs_$DATE_TAG/mutilate_master_qps_*.log "$LOG_DIR"/
scp_with_retry "$CLIENT1_ALIAS":~/memtier/logs_$DATE_TAG/* "$LOG_DIR"/ || true
scp_with_retry "$CLIENT2_ALIAS":~/memtier/logs_$DATE_TAG/* "$LOG_DIR"/ || true
scp_with_retry "$CLIENT3_ALIAS":~/memtier/logs_$DATE_TAG/* "$LOG_DIR"/ || true

echo "$SEPARATOR"
echo "[*] Running data.py for parsing the logs..."
python3 data-intel.py "$LOG_DIR" "$DATE_TAG"

echo "$SEPARATOR"
echo "[*] Experiment finished successfully!"
