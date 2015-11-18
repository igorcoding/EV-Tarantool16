use 5.008008;
use ExtUtils::MakeMaker;

=for rpm
BuildRequires: c-ares19-devel
BuildRequires: libev-devel
Requires: c-ares19
Requires: perl-EV
=cut

my $preop =
    'perldoc -uT $(VERSION_FROM) | tee $(DISTVNAME)/README.pod > README.pod;' .
    'pod2text README.pod | tee $(DISTVNAME)/README > README';

my $ARES_CFLAGS = $ENV{ARES_CFLAGS} // '-I/usr/include';
my $ARES_LDFLAGS = $ENV{ARES_LDFLAGS} // '-lcares';
WriteMakefile(
	NAME              => 'EV::Tarantool16',
	AUTHOR            => ['Mons Anderson <mons@cpan.org>', 'igorcoding <igorcoding@gmail.com>'],
	VERSION_FROM      => 'lib/EV/Tarantool16.pm',
	ABSTRACT_FROM     => 'lib/EV/Tarantool16.pm',
	PREREQ_PM         => {
		'EV'                => 4,
		'Types::Serialiser' => 0
	},
	BUILD_REQUIRES    => {
		'Test::Tarantool16'  => 0,
		'Test::More'         => 0,
		'Test::Deep'         => 0,
		'AnyEvent'           => 0,
		'Proc::ProcessTable' => 0,
		'Time::HiRes'        => 0,
		'Scalar::Util'       => 0,
		'Data::Dumper'       => 0,
		'Carp'               => 0,
	},
	LIBS              => [ $ARES_LDFLAGS ],
	DEFINE            => '-g -ggdb -O0 -std=c99 -Wall',
	LICENSE           => 'GPL',
	dist              => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', PREOP => $preop },
	CONFIGURE         => sub {
		require EV::MakeMaker;
		return {EV::MakeMaker::ev_args(
			INC => '-I. -I./xstnt -I./xsmy -I./libs/evcnn/src -I./libs/msgpuck -I./libs/crypto '. $ARES_CFLAGS,
		)};
	},
);
