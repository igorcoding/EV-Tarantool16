use 5.008008;
use ExtUtils::MakeMaker;

WriteMakefile(
	NAME              => 'EV::Tarantool',
	AUTHOR            => 'Mons Anderson <mons@cpan.org>',
	VERSION_FROM      => 'lib/EV/Tarantool.pm',
	ABSTRACT_FROM     => 'lib/EV/Tarantool.pm',
	PREREQ_PM         => { EV => 4 },
	LIBS              => ['-lev -lcares'],
	DEFINE            => '-g -ggdb -O0',
	CONFIGURE => sub {
		require EV::MakeMaker;
		return {EV::MakeMaker::ev_args(
			INC               => '-I. -I./evcnn/src -I./xstnt -I./xsmy', # e.g., '-I. -I/usr/include/other'
		)};
	},
);
