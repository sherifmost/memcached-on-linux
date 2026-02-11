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