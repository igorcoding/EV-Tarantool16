#!/usr/bin/env bash

wget -q -O - http://tarantool.org/dist/public.key | sudo apt-key add -
release=`lsb_release -c -s`

sudo bash -c 'cat > /etc/apt/sources.list.d/tarantool.list <<- EOF
deb http://tarantool.org/dist/1.6/ubuntu/ `lsb_release -c -s` main
deb-src http://tarantool.org/dist/1.6/ubuntu/ `lsb_release -c -s` main
EOF'

sudo apt-get update
sudo apt-get install -y tarantool libexpat1-dev

USR_SRC=/usr/local/src
wget http://c-ares.haxx.se/download/c-ares-1.9.1.tar.gz -O - | sudo tar -C ${USR_SRC} -xzvf -
cd ${USR_SRC}/c-ares-1.9.1

sudo ./configure
sudo make
sudo make install
