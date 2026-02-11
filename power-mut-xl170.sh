#!/bin/bash

# === Configuration ===
SERVER_ALIAS="client1xl170"
CLIENT1_ALIAS="client2xl170"
CLIENT2_ALIAS="client3xl170"
CLIENT3_ALIAS="client4xl170"

MEMCACHED_IP="10.10.0.1"
MEMCACHED_PORT=11211
MEMCACHED_THREADS=20
MEMCACHED_CORE_RANGE="0-19"

MUTILATE_AGENT_THREADS=10
MUTILATE_AGENT_CORE_RANGE="0-9"
MUTILATE_AGENT_CLIENTS=8
MUTILATE_TEST_DURATION=60
# MUTILATE_TEST_DURATION=10
# Use exponential inter-arrival times for Poisson arrivals (vs fb_ia Pareto bursty load)
MUTILATE_IADIST="exponential"

MUTILATE_AGENT_1_ALIAS="10.10.0.2"
MUTILATE_AGENT_2_ALIAS="10.10.0.3"
MUTILATE_AGENT_3_ALIAS="localhost"

MUTILATE_MASTER_THREADS=4
MUTILATE_MASTER_CLIENTS=4
MUTILATE_MASTER_QPS=1000


QPS_LIST=(
  50000 100000 150000 200000 250000
  300000 350000 400000 450000 500000
  550000 600000 650000 700000 750000
  800000 850000 900000 950000 1000000
  1050000 1100000 1150000 1200000 1250000
  1300000 1350000 1400000 1450000 1500000
)


SYNC_DELAY=10
POWER_STAT_DELAY=15

DATE_TAG=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_DIR="experiment_logs_xl170/run_${DATE_TAG}"
mkdir -p "$LOG_DIR"

SEPARATOR="------------------------------------------------------------"

# === SSH Helpers ===
ssh_with_retry() {
  local max_tries=10
  local count=0
  while ! ssh "$@"; do
    count=$((count+1))
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
    count=$((count+1))
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
ssh -fN $SERVER_ALIAS
ssh -fN $CLIENT1_ALIAS
ssh -fN $CLIENT2_ALIAS
ssh -fN $CLIENT3_ALIAS

echo "$SEPARATOR"
echo "[*] Starting memcached and powerstat on server..."

ssh_with_retry $SERVER_ALIAS <<EOF
  set -e
  mkdir -p logs_$DATE_TAG
  cd logs_$DATE_TAG

  sudo pkill memcached || true
  sudo pkill powerstat || true
  sleep 2

  echo "[*] Launching memcached with retries..."
  MAX_RETRIES=5
  COUNT=0

  while [ \$COUNT -lt \$MAX_RETRIES ]; do
    echo "[*] Attempt \$((COUNT + 1)) to start memcached..."
    nohup taskset -c $MEMCACHED_CORE_RANGE memcached -u nobody -l $MEMCACHED_IP -t $MEMCACHED_THREADS > memcached_server.log 2>&1 &

    sleep 2

    if sudo netstat -tuln | grep $MEMCACHED_IP; then
      echo "[*] Memcached is running on port $MEMCACHED_PORT"
      break
    else
      echo "[!] Memcached failed to start. Retrying..."
      sudo pkill memcached || true
      sleep 2
      COUNT=\$((COUNT + 1))
    fi
  done

  if [ \$COUNT -eq \$MAX_RETRIES ]; then
    echo "[!!] Memcached failed to start after \$MAX_RETRIES attempts"
    exit 1
  fi
EOF

echo "$SEPARATOR"
echo "[*] Sleeping for warm-up and sync (${SYNC_DELAY}s)..."
sleep $SYNC_DELAY

echo "$SEPARATOR"
echo "[*] Starting QPS loop on server..."
for QPS in "${QPS_LIST[@]}"; do
    echo "[*] Starting mutilate agents on clients..."
    ssh_with_retry $CLIENT1_ALIAS <<EOF
        cd ~/mutilate
        sudo pkill mutilate || true
        nohup taskset -c $MUTILATE_AGENT_CORE_RANGE ./mutilate -T $MUTILATE_AGENT_THREADS -A > /dev/null 2>&1 &
EOF

    ssh_with_retry $CLIENT2_ALIAS <<EOF
        cd ~/mutilate
        sudo pkill mutilate || true
        nohup taskset -c $MUTILATE_AGENT_CORE_RANGE ./mutilate -T $MUTILATE_AGENT_THREADS -A > /dev/null 2>&1 &
EOF

    ssh_with_retry $CLIENT3_ALIAS <<EOF
        cd ~/mutilate
        sudo pkill mutilate || true
        nohup taskset -c $MUTILATE_AGENT_CORE_RANGE ./mutilate -T $MUTILATE_AGENT_THREADS -A -i $MUTILATE_IADIST > /dev/null 2>&1 &
EOF
    
    echo "$SEPARATOR"
    echo "[*] QPS: $QPS"
    
    echo "[*] Starting powerstat on server..."
    ssh_with_retry $SERVER_ALIAS "nohup sudo powerstat -aRn -d $POWER_STAT_DELAY 1 50 > logs_$DATE_TAG/powerstat_rate_${QPS}.txt 2>&1 & echo \$! > powerstat_pid.txt"

    echo "$SEPARATOR"
    echo "[*] Sleeping for warm-up and sync (${SYNC_DELAY}s)..."
    sleep $SYNC_DELAY

    echo "$SEPARATOR"
    echo "[*] Starting mutilate master on master client..."
    ssh_with_retry $CLIENT3_ALIAS <<EOF
        mkdir -p ~/mutilate/logs_$DATE_TAG
        cd ~/mutilate
        taskset -c 0-3 ./mutilate -s $MEMCACHED_IP -T $MUTILATE_MASTER_THREADS -C $MUTILATE_MASTER_CLIENTS -Q $MUTILATE_MASTER_QPS -a $MUTILATE_AGENT_1_ALIAS -a $MUTILATE_AGENT_2_ALIAS -a $MUTILATE_AGENT_3_ALIAS -c $MUTILATE_AGENT_CLIENTS -q $QPS -t $MUTILATE_TEST_DURATION --iadist $MUTILATE_IADIST > logs_$DATE_TAG/mutilate_master_qps_${QPS}.log
EOF

    # echo "$SEPARATOR"
    # echo "[*] Stopping powerstat on server..."
    # ssh_with_retry $SERVER_ALIAS "sudo pkill powerstat || true"
done

echo "$SEPARATOR"
echo "[*] Stopping mutilate agents on clients..."
# ssh_with_retry $CLIENT1_ALIAS "sudo pkill mutilate || true"
# ssh_with_retry $CLIENT2_ALIAS "sudo pkill mutilate || true"
# ssh_with_retry $CLIENT3_ALIAS "sudo pkill mutilate || true"

echo "$SEPARATOR"
echo "[*] Copying logs from server to local directory..."
scp_with_retry $SERVER_ALIAS:logs_$DATE_TAG/powerstat_rate_*.txt $LOG_DIR/
scp_with_retry $CLIENT3_ALIAS:~/mutilate/logs_$DATE_TAG/mutilate_master_qps_*.log $LOG_DIR/
echo "$SEPARATOR"
echo "[*] Experiment completed. Logs are saved in $LOG_DIR"
echo "$SEPARATOR"
echo "[*] Running data.py for parsing the logs..."
python3 data-intel.py $LOG_DIR $DATE_TAG
echo "$SEPARATOR"
echo "[*] Experiment finished successfully!"
