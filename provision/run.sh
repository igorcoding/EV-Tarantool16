#!/usr/bin/env bash

wget -q -O - http://tarantool.org/dist/public.key | sudo apt-key add -
release=`lsb_release -c -s`

sudo bash -c 'cat > /etc/apt/sources.list.d/tarantool.list <<- EOF
deb http://tarantool.org/dist/master/ubuntu/ `lsb_release -c -s` main
deb-src http://tarantool.org/dist/master/ubuntu/ `lsb_release -c -s` main
EOF'

sudo apt-get update
sudo apt-get install -y tarantool

USR_SRC=/usr/local/src
wget http://c-ares.haxx.se/download/c-ares-1.9.1.tar.gz -O - | sudo tar -C ${USR_SRC} -xzvf -
cd ${USR_SRC}/c-ares-1.9.1

sudo ./configure
sudo make
sudo make install

cd -

curl -L https://cpanmin.us | sudo perl - App::cpanminus
sudo cpanm Types::Serialiser
sudo cpanm EV
sudo cpanm AnyEvent
sudo cpanm Test::Deep

if [ ${TRAVIS} == true ]; then
	echo "TRAVIS"
else
    sudo apt-get install -y \
        build-essential \
        libcurl4-openssl-dev \
        tarantool
fi

sudo service tarantool restart
