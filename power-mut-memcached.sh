#!/bin/bash

# === Configuration selection ===
PROFILE="${1:-ETC}"   # ETC (default) or USR
GOV_LABEL="${2:-performance}"  # frequency governor label

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
# MEMCACHED_MEMORY_MB=64000

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
# MUTILATE_RECORDS=10000             # default mutilate record count
DO_LOADONLY=true                  # legacy flag
FORCE_PRELOAD=true                # always warm cache before measurements
LOAD_THREADS=4

# Profile-specific tweaks
case "$PROFILE" in
  ETC)
    # Facebook ETC: read-heavy, Poisson arrivals (already set by default)
    ;;
  USR)
    # USR-like: fixed key/value sizes, low write ratio
    MUTILATE_KEYSIZE="fixed:21"
    MUTILATE_VALUESIZE="fixed:2"
    MUTILATE_UPDATE_RATIO=0.002   # or raise to 0.018 if you want more misses
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
  50000 
  # 75000 100000 125000 150000 175000 200000 225000 250000
  # 300000 325000 350000 375000 400000 425000 450000 475000 500000 
  # 525000 550000 575000 600000 
  # 650000 700000 750000
  # 800000 850000 900000 950000 1000000
  # 1050000 1100000 1150000 1200000 1250000
  # 1300000 1350000 1400000 1450000 1500000
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
echo "[*] Starting memcached and powerstat on server..."
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

echo "$SEPARATOR"
echo "[*] Sleeping for warm-up and sync (${SYNC_DELAY}s)..."
sleep "$SYNC_DELAY"

LOADED=false

echo "$SEPARATOR"
echo "[*] Starting QPS loop on server..."
for QPS in "${QPS_LIST[@]}"; do
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

  if [ "$LOADED" = false ]; then
    echo "$SEPARATOR"
    echo "[*] Loading dataset (write-only) from master right before measurement..."
    ssh_with_retry "$CLIENT3_ALIAS" <<EOF
      set -e
      cd ~/mutilate
      mkdir -p logs_$DATE_TAG
      {
        echo "LOAD_START \$(date)"
        taskset -c ${MASTER_CORE_RANGE:-12-15} ./mutilate \
          -s $MEMCACHED_IP \
          -T $LOAD_THREADS \
          -K $MUTILATE_KEYSIZE \
          -V $MUTILATE_VALUESIZE \
          --loadonly
        rc=\$?
        echo "LOAD_EXIT \$rc \$(date)"
      } > ~/mutilate/logs_$DATE_TAG/mutilate_load.log 2>&1
EOF
    LOADED=true
  fi

  echo "$SEPARATOR"
  echo "[*] QPS: $QPS"

  echo "$SEPARATOR"
  echo "[*] Starting powerstat on server..."
  ssh_with_retry "$SERVER_ALIAS" "nohup sudo powerstat -aRn -d $POWER_STAT_DELAY 1 50 > logs_$DATE_TAG/powerstat_rate_${QPS}.txt 2>&1 & echo \$! > powerstat_pid.txt"

  echo "$SEPARATOR"
  echo "[*] Sleeping for warm-up and sync (${SYNC_DELAY}s)..."
  sleep "$SYNC_DELAY"

  echo "$SEPARATOR"
  echo "[*] Starting mutilate master on master client..."
  ssh_with_retry "$CLIENT3_ALIAS" <<EOF
    set -e
    mkdir -p ~/mutilate/logs_$DATE_TAG
    cd ~/mutilate
    LOGFILE="logs_$DATE_TAG/mutilate_master_qps_${QPS}.log"
    LATFILE="logs_$DATE_TAG/mutilate_lat_qps_${QPS}.lat"
    cat <<META > "\$LOGFILE"
profile=$PROFILE
governor=$GOV_LABEL
engine=MEMCACHED
keysize=$MUTILATE_KEYSIZE
valuesize=$MUTILATE_VALUESIZE
update_ratio=$MUTILATE_UPDATE_RATIO
iadist=$MUTILATE_IADIST
qps_target=$QPS
test_duration=$MUTILATE_TEST_DURATION
threads_master=$MUTILATE_MASTER_THREADS
clients_master=$MUTILATE_MASTER_CLIENTS
depth_master=$MUTILATE_MASTER_DEPTH
threads_agent=$MUTILATE_AGENT_THREADS
clients_agent=$MUTILATE_AGENT_CLIENTS
depth_agent=$MUTILATE_AGENT_DEPTH
META

    timeout $((MUTILATE_TEST_DURATION + 20)) taskset -c ${MASTER_CORE_RANGE:-12-15} ./mutilate \
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
      -w 10 \
      --save "\$LATFILE" \
      --noload \
      --iadist $MUTILATE_IADIST \
      >> "\$LOGFILE" 2>&1
EOF
done

echo "$SEPARATOR"
echo "[*] Copying logs from server to local directory..."
scp_with_retry "$SERVER_ALIAS":logs_$DATE_TAG/powerstat_rate_*.txt "$LOG_DIR"/
scp_with_retry "$CLIENT3_ALIAS":~/mutilate/logs_$DATE_TAG/mutilate_master_qps_*.log "$LOG_DIR"/
scp_with_retry "$CLIENT3_ALIAS":~/mutilate/logs_$DATE_TAG/mutilate_lat_qps_*.lat "$LOG_DIR"/ || true
scp_with_retry "$CLIENT3_ALIAS":~/mutilate/logs_$DATE_TAG/mutilate_load.log "$LOG_DIR"/ || true

echo "$SEPARATOR"
echo "[*] Running data.py for parsing the logs..."
python3 data-intel.py "$LOG_DIR" "$DATE_TAG"

echo "$SEPARATOR"
echo "[*] Experiment finished successfully!"
