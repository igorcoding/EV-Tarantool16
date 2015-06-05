use 5.008008;
use ExtUtils::MakeMaker;

=for rpm
BuildRequires: c-ares19-devel
BuildRequires: libev-devel
Requires: c-ares19
Requires: perl-EV
=cut

my $ARES_CFLAGS=$ENV{ARES_CFLAGS}//'-I/usr/include';
my $ARES_LDFLAGS=$ENV{ARES_LD_FLAGS}//'-lcares';
WriteMakefile(
	NAME              => 'EV::Tarantool16',
	AUTHOR            => ['Mons Anderson <mons@cpan.org>', 'igorcoding <igorcoding@gmail.com>'],
	VERSION_FROM      => 'lib/EV/Tarantool16.pm',
	ABSTRACT_FROM     => 'lib/EV/Tarantool16.pm',
	PREREQ_PM         => {
		'EV' => 4,
		'Types::Serialiser' => 0
	},
	BUILD_REQUIRES     => {
		'Test::More' => 0,
		'AnyEvent'   => 0,
	},
	LIBS              => [ $ARES_LD_FLAGS ],
	DEFINE            => '-g -ggdb -O0 -std=c99 -Wall',
	CONFIGURE => sub {
		require EV::MakeMaker;
		return {EV::MakeMaker::ev_args(
			INC => '-I. -I./xstnt -I./xsmy -I./libs/evcnn/src -I./libs/msgpuck -I./libs/crypto '. $ARES_CFLAGS,
		)};
	},
);
