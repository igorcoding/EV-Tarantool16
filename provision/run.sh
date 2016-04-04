#!/usr/bin/env bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
TNTCARES=$DIR"/tnt-cares.sh"

echo ${TRAVIS_OS_NAME}

mkdir -p $HOME/temp
cd $HOME/temp
TestTarantool_VER=0.032
TestTarantool_URL=https://github.com/igorcoding/Test-Tarantool16/releases/download/v${TestTarantool_VER}/Test-Tarantool16-${TestTarantool_VER}.tar.gz
wget ${TestTarantool_URL} -O $HOME/temp/test-tarantool16.tar.gz
cd -

if [ -z "$TRAVIS_OS_NAME" ] || [ ${TRAVIS_OS_NAME} == 'linux' ]; then
	source $TNTCARES
	
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
	cpanm $HOME/temp/test-tarantool16.tar.gz
elif [ ${TRAVIS_OS_NAME} == 'osx' ]; then
	echo "Mac OS X detected"
	brew update
	brew install curl
	brew install https://raw.githubusercontent.com/Homebrew/homebrew/master/Library/Formula/cpanminus.rb
	cpanm --version
	# brew install https://raw.githubusercontent.com/Homebrew/homebrew/master/Library/Formula/c-ares.rb
	
	USR_SRC=/usr/local/src
	sudo mkdir -p ${USR_SRC}
	wget http://c-ares.haxx.se/download/c-ares-1.10.0.tar.gz -O $HOME/temp/c-ares-1.10.0.tar.gz
	sudo tar -C ${USR_SRC} -xzvf $HOME/temp/c-ares-1.10.0.tar.gz
	cd ${USR_SRC}/c-ares-1.10.0
	sudo ./configure
	sudo make
	sudo make install
	cd -
	
	sudo cp $DIR/macos_ares_rules.h /usr/local/include/ares_rules.h
	
	brew update
	brew install tarantool
	tarantool -V
	mkdir -p ${HOME}/perl5
	# cpanm --local-lib=${HOME}/perl5 local::lib && eval $(perl -I ~/perl5/lib/perl5/ -Mlocal::lib)
	export PERL5LIB=${HOME}/perl5
	
	cpanm --sudo Types::Serialiser
	cpanm --sudo EV
	
	cpanm --sudo Test::More
	cpanm --sudo Test::Deep
	cpanm --sudo AnyEvent
	cpanm --sudo Proc::ProcessTable
	cpanm --sudo Time::HiRes
	cpanm --sudo Scalar::Util
	cpanm --sudo Data::Dumper
	cpanm --sudo Carp
	cpanm --sudo $HOME/temp/test-tarantool16.tar.gz
	
	cat ~/.cpanm/work/**/*.log
fi

cd -



PREV_HOME=${HOME}

if [ ${TRAVIS} == true ]; then
	echo "TRAVIS"

	# sudo ln -s ${TRAVIS_BUILD_DIR}/provision/evtnt.lua /etc/tarantool/instances.enabled/
	# HOME=${TRAVIS_BUILD_DIR}/../ sudo tarantoolctl start evtnt
	# export HOME=${PREV_HOME}
else
	sudo apt-get install -y valgrind perl-doc
    curl -L https://cpanmin.us | sudo perl - App::cpanminus
    
	sudo cpanm Types::Serialiser
	sudo cpanm EV
	
	sudo cpanm Test::More
	sudo cpanm Test::Deep
	sudo cpanm AnyEvent
	sudo cpanm Proc::ProcessTable
	sudo cpanm Time::HiRes
	sudo cpanm Scalar::Util
	sudo cpanm Data::Dumper
	sudo cpanm Carp
	sudo cpanm $HOME/temp/test-tarantool16.tar.gz
	
	sudo cpanm Test::Valgrind
	sudo cpanm List::BinarySearch

	# sudo ln -s ${HOME}/EV-Tarantool1.6/provision/evtnt.lua /etc/tarantool/instances.enabled/

	echo 'Build Perl 5.16.3...'

	mkdir -p ${HOME}/perl
	mkdir -p ${HOME}/perl-src

	cd ${HOME}/perl-src

	wget http://www.cpan.org/src/5.0/perl-5.16.3.tar.gz -O - | tar -xzvf -
	cd perl-5.16.3/
	./Configure -des -Dprefix=${HOME}/perl -Duselargefiles -Duse64bitint -DUSEMYMALLOC -DDEBUGGING -DDEBUG_LEAKING_SCALARS -DPERL_MEM_LOG -Dinc_version_list=none -Doptimize="-march=athlon64 -fomit-frame-pointer -pipe -ggdb -g3 -O2" -Dccflags="-DPIC -fPIC -O2 -march=athlon64 -fomit-frame-pointer -pipe -ggdb -g3" -D locincpth="${HOME}/perl/include /usr/local/include" -D loclibpth="${HOME}/perl/lib /usr/local/lib" -D privlib=${HOME}/perl/lib/perl5/5.16.3 -D archlib=${HOME}/perl/lib/perl5/5.16.3 -D sitelib=${HOME}/perl/lib/perl5/5.16.3 -D sitearch=${HOME}/perl/lib/perl5/5.16.3 -Uinstallhtml1dir= -Uinstallhtml3dir= -Uinstallman1dir= -Uinstallman3dir= -Uinstallsitehtml1dir= -Uinstallsitehtml3dir= -Uinstallsiteman1dir= -Uinstallsiteman3dir=
	# CLANG: # ./Configure -des -Dprefix=${HOME}/perl-llvm -Dcc=clang -Duselargefiles -Duse64bitint -DUSEMYMALLOC -DDEBUGGING -DDEBUG_LEAKING_SCALARS -DPERL_MEM_LOG -Dinc_version_list=none -Doptimize="-march=athlon64 -fomit-frame-pointer -pipe -ggdb -g3 -O2" -Dccflags="-DPIC -fPIC -O2 -march=athlon64 -fomit-frame-pointer -pipe -ggdb -g3" -D locincpth="${HOME}/perl-llvm/include /usr/local/include" -D loclibpth="${HOME}/perl-llvm/lib /usr/local/lib" -D privlib=${HOME}/perl-llvm/lib/perl5/5.16.3 -D archlib=${HOME}/perl-llvm/lib/perl5/5.16.3 -D sitelib=${HOME}/perl-llvm/lib/perl5/5.16.3 -D sitearch=${HOME}/perl-llvm/lib/perl5/5.16.3 -Uinstallhtml1dir= -Uinstallhtml3dir= -Uinstallman1dir= -Uinstallman3dir= -Uinstallsitehtml1dir= -Uinstallsitehtml3dir= -Uinstallsiteman1dir= -Uinstallsiteman3dir=

	make
	make install

	sudo ${HOME}/perl/bin/perl `which cpanm` Types::Serialiser
	sudo ${HOME}/perl/bin/perl `which cpanm` EV
	sudo ${HOME}/perl/bin/perl `which cpanm` EV::MakeMaker
	sudo ${HOME}/perl/bin/perl `which cpanm` AnyEvent
	sudo ${HOME}/perl/bin/perl `which cpanm` Test::Deep
	sudo ${HOME}/perl/bin/perl `which cpanm` Test::Valgrind
	sudo ${HOME}/perl/bin/perl `which cpanm` Devel::Leak

	mkdir -p ${HOME}/tnt

	# sudo tarantoolctl start evtnt
fi

# sudo service tarantool restart
tarantool --version
