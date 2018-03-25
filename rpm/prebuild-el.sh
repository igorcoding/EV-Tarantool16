#! /usr/bin/env bash

echo "Installing cpanm"
sudo yum install -y perl-CPAN
curl -L http://cpanmin.us | perl - --sudo App::cpanminus

echo "Installing Test::Tarantool"
TestTarantool_VER=0.033
TestTarantool_URL=https://github.com/igorcoding/Test-Tarantool16/releases/download/v${TestTarantool_VER}/Test-Tarantool16-${TestTarantool_VER}.tar.gz
cpanm --sudo --notest ${TestTarantool_URL}

echo "Prebuild finished"
