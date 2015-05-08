#!/usr/bin/env bash

wget -q -O - http://tarantool.org/dist/public.key | sudo apt-key add -
release=`lsb_release -c -s`

sudo bash -c 'cat > /etc/apt/sources.list.d/tarantool.list <<- EOF
deb http://tarantool.org/dist/master/ubuntu/ `lsb_release -c -s` main
deb-src http://tarantool.org/dist/master/ubuntu/ `lsb_release -c -s` main
EOF'

sudo apt-get update
sudo apt-get install -y tarantool libexpat1-dev

USR_SRC=/usr/local/src
wget http://c-ares.haxx.se/download/c-ares-1.9.1.tar.gz -O - | sudo tar -C ${USR_SRC} -xzvf -
cd ${USR_SRC}/c-ares-1.9.1

sudo ./configure
sudo make
sudo make install

cd -

PREV_HOME=${HOME}

if [ ${TRAVIS} == true ]; then
	echo "TRAVIS"
	cpanm Types::Serialiser
	cpanm EV
	cpanm AnyEvent
	cpanm Test::Deep

	echo ${TRAVIS_BUILD_DIR}
	sudo ln -s ${TRAVIS_BUILD_DIR}/EV-Tarantool1.6/provision/evtnt.lua /etc/tarantool/instances.enabled/
	HOME=${TRAVIS_BUILD_DIR} sudo tarantoolctl start evtnt
	export HOME=${PREV_HOME}
else
	sudo apt-get install -y valgrind
    curl -L https://cpanmin.us | sudo perl - App::cpanminus
	sudo cpanm Types::Serialiser
	sudo cpanm EV
	sudo cpanm EV::MakeMaker
	sudo cpanm AnyEvent
	sudo cpanm Test::Deep
	sudo cpanm Test::Valgrind
	sudo cpanm List::BinarySearch

	sudo ln -s ${HOME}/EV-Tarantool1.6/provision/evtnt.lua /etc/tarantool/instances.enabled/

	echo 'Build Perl 5.16.3...'

	mkdir -p ${HOME}/perl
	mkdir -p ${HOME}/perl-src

	cd ${HOME}/perl-src

	wget http://www.cpan.org/src/5.0/perl-5.16.3.tar.gz -O - | tar -xzvf -
	cd perl-5.16.3/
	./Configure -des -Dprefix=${HOME}/perl -Duselargefiles -Duse64bitint -DUSEMYMALLOC -DDEBUGGING -DDEBUG_LEAKING_SCALARS -DPERL_MEM_LOG -Dinc_version_list=none -Doptimize="-march=athlon64 -fomit-frame-pointer -pipe -ggdb -g3 -O2" -Dccflags="-DPIC -fPIC -O2 -march=athlon64 -fomit-frame-pointer -pipe -ggdb -g3" -D locincpth="${HOME}/perl/include /usr/local/include" -D loclibpth="${HOME}/perl/lib /usr/local/lib" -D privlib=${HOME}/perl/lib/perl5/5.16.3 -D archlib=${HOME}/perl/lib/perl5/5.16.3 -D sitelib=${HOME}/perl/lib/perl5/5.16.3 -D sitearch=${HOME}/perl/lib/perl5/5.16.3 -Uinstallhtml1dir= -Uinstallhtml3dir= -Uinstallman1dir= -Uinstallman3dir= -Uinstallsitehtml1dir= -Uinstallsitehtml3dir= -Uinstallsiteman1dir= -Uinstallsiteman3dir=
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

	sudo tarantoolctl start evtnt
fi

# sudo service tarantool restart
tarantool --version
