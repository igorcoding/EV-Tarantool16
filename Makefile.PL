use 5.008008;
use ExtUtils::MakeMaker;

=for rpm
BuildRequires: c-ares19-devel
BuildRequires: libev-devel
Requires: c-ares19
Requires: perl-EV
=cut

WriteMakefile(
	NAME              => 'EV::Tarantool',
	AUTHOR            => 'Mons Anderson <mons@cpan.org>',
	VERSION_FROM      => 'lib/EV/Tarantool.pm',
	ABSTRACT_FROM     => 'lib/EV/Tarantool.pm',
	PREREQ_PM         => { EV => 4 },
	LIBS              => ['-lev -lcares19'],
	DEFINE            => '-g -ggdb -O2',
	CONFIGURE => sub {
		require EV::MakeMaker;
		return {EV::MakeMaker::ev_args(
			INC               => '-I/usr/include/c-ares19 -I. -I./evcnn/src -I./xstnt -I./xsmy',
		)};
	},
);
