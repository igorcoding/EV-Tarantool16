#! /usr/bin/env bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

echo "Travis OS: ${TRAVIS_OS_NAME}"

TestTarantool_VER=0.033
TestTarantool_URL=https://github.com/igorcoding/Test-Tarantool16/releases/download/v${TestTarantool_VER}/Test-Tarantool16-${TestTarantool_VER}.tar.gz
TestTarantool_LOCATION=/tmp/test-tarantool16.tar.gz
wget ${TestTarantool_URL} -O ${TestTarantool_LOCATION}

if [ -z "$TRAVIS_OS_NAME" ] || [ ${TRAVIS_OS_NAME} == 'linux' ]; then
	sudo apt-get install -y curl
	curl https://packagecloud.io/tarantool/1_7/gpgkey | sudo apt-key add -

	sudo apt-get -y install apt-transport-https
	release=`lsb_release -c -s`

	sudo rm -f /etc/apt/sources.list.d/*tarantool*.list
	sudo bash -c 'cat > /etc/apt/sources.list.d/tarantool.list <<- EOF
	deb https://packagecloud.io/tarantool/1_7/ubuntu/ `lsb_release -c -s` main
	deb-src https://packagecloud.io/tarantool/1_7/ubuntu/ `lsb_release -c -s` main
	EOF'

	sudo apt-get update
	sudo apt-get install -y tarantool

	USR_SRC=/usr/local/src
	wget http://c-ares.haxx.se/download/c-ares-1.10.0.tar.gz -O - | sudo tar -C ${USR_SRC} -xzvf -
	cd ${USR_SRC}/c-ares-1.10.0
	sudo ./configure
	sudo make
	sudo make install
	
elif [ ${TRAVIS_OS_NAME} == 'osx' ]; then
	echo "Mac OS X build is not supported"
	# sudo sh -c 'echo "127.0.0.1 localhost" >> /etc/hosts'
	# sudo ifconfig lo0 alias 127.0.0.2 up
	# brew update
	# brew install curl
	# brew install https://raw.githubusercontent.com/Homebrew/homebrew-core/master/Formula/cpanminus.rb
	# brew install https://raw.githubusercontent.com/Homebrew/homebrew-core/master/Formula/c-ares.rb
	# cpanm --version
	# cpanm --local-lib=~/perl5 local::lib && eval $(perl -I ~/perl5/lib/perl5/ -Mlocal::lib)
	
	# brew install tarantool
	# tarantool -V
	
	# # cat ~/.cpanm/work/**/*.log
fi

cpanm Types::Serialiser
cpanm EV

cpanm Test::More
cpanm Test::Deep
cpanm AnyEvent
cpanm Proc::ProcessTable
cpanm Time::HiRes
cpanm Scalar::Util
cpanm Data::Dumper
cpanm Carp
cpanm ${TestTarantool_LOCATION}
