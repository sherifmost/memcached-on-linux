#!/bin/bash

# Install Redis server and memtier_benchmark on remote hosts.
# Usage: ./install-redis-memtier.sh host1 [host2 ...]
# Defaults: server_mem client_mem1 client_mem2 client_mem3

hosts=("$@")
if [ "${#hosts[@]}" -eq 0 ]; then
  hosts=(server_mem client_mem1 client_mem2 client_mem3)
fi

ssh_opts=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=10)

install_cmd='set -e
sudo apt-get update -y
sudo apt-get install -y redis-server git build-essential autoconf automake libpcre3-dev libevent-dev pkg-config zlib1g-dev libssl-dev
if sudo apt-get install -y memtier-benchmark 2>/dev/null; then
  echo "[*] memtier-benchmark installed from apt."
else
  echo "[*] Building memtier-benchmark from source..."
  tmpdir=$(mktemp -d)
  cd "$tmpdir"
  git clone https://github.com/RedisLabs/memtier_benchmark.git
  cd memtier_benchmark
  autoreconf -ivf
  ./configure
  make -j"$(nproc)"
  sudo make install
  echo "[*] memtier-benchmark built and installed."
fi
'

for h in "${hosts[@]}"; do
  echo "[*] Installing on $h ..."
  ssh "${ssh_opts[@]}" "$h" "$install_cmd" || {
    echo "[!] Install failed on $h"
  }
done

echo "[✓] Done."
