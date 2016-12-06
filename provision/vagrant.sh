#!/usr/bin/env bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export LANG=en_US.UTF-8
export LANGUAGE=en_US:en
export LC_ALL=en_US.UTF-8
sudo locale-gen en_US.UTF-8

sudo bash -c 'cat >> ~/.bashrc <<- EOF
export LANG=en_US.UTF-8
export LANGUAGE=en_US:en
export LC_ALL=en_US.UTF-8
EOF'

function install_tarantool {
	curl http://download.tarantool.org/tarantool/1.7/gpgkey | sudo apt-key add -
	release=`lsb_release -c -s`
	# install https download transport for APT
	sudo apt-get -y install apt-transport-https

	# append two lines to a list of source repositories
	sudo rm -f /etc/apt/sources.list.d/*tarantool*.list
	sudo tee /etc/apt/sources.list.d/tarantool_1_7.list <<- EOF
	deb http://download.tarantool.org/tarantool/1.7/ubuntu/ $release main
	deb-src http://download.tarantool.org/tarantool/1.7/ubuntu/ $release main
	EOF

	# install
	sudo apt-get update
	sudo apt-get -y install tarantool
	tarantool --version
	
	mkdir -p ${HOME}/tnt
}

function install_cares {
	USR_SRC=/usr/local/src
	wget http://c-ares.haxx.se/download/c-ares-1.10.0.tar.gz -O - | sudo tar -C ${USR_SRC} -xzvf -
	cd ${USR_SRC}/c-ares-1.10.0
	sudo ./configure
	sudo make
	sudo make install
}

install_tarantool
install_cares

TestTarantool_VER=0.033
TestTarantool_URL=https://github.com/igorcoding/Test-Tarantool16/releases/download/v${TestTarantool_VER}/Test-Tarantool16-${TestTarantool_VER}.tar.gz
TestTarantool_LOCATION=/tmp/test-tarantool16.tar.gz
wget ${TestTarantool_URL} -O ${TestTarantool_LOCATION}


sudo apt-get install -y valgrind perl-doc libexpat1-dev
curl -L https://cpanmin.us | sudo perl - App::cpanminus
sudo chown -R vagrant:vagrant ~/.cpanm
cpanm --local-lib=~/perl5 local::lib && eval $(perl -I ~/perl5/lib/perl5/ -Mlocal::lib)
echo 'eval $(perl -I ~/perl5/lib/perl5/ -Mlocal::lib)' >> $HOME/.bashrc

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

cpanm Test::Valgrind
cpanm List::BinarySearch


# echo 'Building Perl 5.16.3...'
# mkdir -p ${HOME}/perl
# mkdir -p ${HOME}/perl-src
# cd ${HOME}/perl-src
# wget http://www.cpan.org/src/5.0/perl-5.16.3.tar.gz -O - | tar -xzvf -
# cd perl-5.16.3/
# ./Configure -des -Dprefix=${HOME}/perl -Duselargefiles -Duse64bitint -DUSEMYMALLOC -DDEBUGGING -DDEBUG_LEAKING_SCALARS -DPERL_MEM_LOG -Dinc_version_list=none -Doptimize="-march=athlon64 -fomit-frame-pointer -pipe -ggdb -g3 -O2" -Dccflags="-DPIC -fPIC -O2 -march=athlon64 -fomit-frame-pointer -pipe -ggdb -g3" -D locincpth="${HOME}/perl/include /usr/local/include" -D loclibpth="${HOME}/perl/lib /usr/local/lib" -D privlib=${HOME}/perl/lib/perl5/5.16.3 -D archlib=${HOME}/perl/lib/perl5/5.16.3 -D sitelib=${HOME}/perl/lib/perl5/5.16.3 -D sitearch=${HOME}/perl/lib/perl5/5.16.3 -Uinstallhtml1dir= -Uinstallhtml3dir= -Uinstallman1dir= -Uinstallman3dir= -Uinstallsitehtml1dir= -Uinstallsitehtml3dir= -Uinstallsiteman1dir= -Uinstallsiteman3dir=
# # CLANG: # ./Configure -des -Dprefix=${HOME}/perl-llvm -Dcc=clang -Duselargefiles -Duse64bitint -DUSEMYMALLOC -DDEBUGGING -DDEBUG_LEAKING_SCALARS -DPERL_MEM_LOG -Dinc_version_list=none -Doptimize="-march=athlon64 -fomit-frame-pointer -pipe -ggdb -g3 -O2" -Dccflags="-DPIC -fPIC -O2 -march=athlon64 -fomit-frame-pointer -pipe -ggdb -g3" -D locincpth="${HOME}/perl-llvm/include /usr/local/include" -D loclibpth="${HOME}/perl-llvm/lib /usr/local/lib" -D privlib=${HOME}/perl-llvm/lib/perl5/5.16.3 -D archlib=${HOME}/perl-llvm/lib/perl5/5.16.3 -D sitelib=${HOME}/perl-llvm/lib/perl5/5.16.3 -D sitearch=${HOME}/perl-llvm/lib/perl5/5.16.3 -Uinstallhtml1dir= -Uinstallhtml3dir= -Uinstallman1dir= -Uinstallman3dir= -Uinstallsitehtml1dir= -Uinstallsitehtml3dir= -Uinstallsiteman1dir= -Uinstallsiteman3dir=
# make
# make install

# ${HOME}/perl/bin/perl `which cpanm` Types::Serialiser
# ${HOME}/perl/bin/perl `which cpanm` EV
# ${HOME}/perl/bin/perl `which cpanm` EV::MakeMaker
# ${HOME}/perl/bin/perl `which cpanm` AnyEvent
# ${HOME}/perl/bin/perl `which cpanm` Test::Deep
# ${HOME}/perl/bin/perl `which cpanm` Test::Valgrind
# ${HOME}/perl/bin/perl `which cpanm` Devel::Leak
