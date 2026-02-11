sudo apt update
sudo apt install -y memcached powerstat htop scons libevent-dev gengetopt libzmq3-dev cpufrequtils
git clone https://github.com/zeromq/cppzmq.git
cd cppzmq
cp zmp.hpp /usr/local/include
cd ..

unzip mutilate.zip
cd mutilate
scons -c
scons

-----------
scp mutilate.zip client3c220g1:~


    echo off | sudo tee /sys/devices/system/cpu/smt/control

for cpu in {8..15} {24..31}; do
    echo 0 | sudo tee /sys/devices/system/cpu/cpu$cpu/online
done


echo "1" | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo