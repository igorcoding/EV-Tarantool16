package main;

use 5.010;
use strict;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV;
use Time::HiRes 'sleep','time';
use Scalar::Util 'weaken';
use Errno;
use EV::Tarantool;
use Test::More;
use Test::Deep;
use Data::Dumper;
use Renewer;
# use Devel::Leak;
# use AE;

my %test_exec = (
	ping => 1,
	eval => 1,
	call => 1,
	select => 1,
	insert => 1,
	delete => 1,
	update => 1,
	RTREE => 1,
	memtest => 1
);

sub meminfo () {
	my $stat = do { open my $f,'<:raw',"/proc/$$/stat"; local $/; <$f> };
	$stat =~ m{ ^ \d+ \s+ \((.+?)\) \s+ ([RSDZTW]) \s+}gcx;
	my %s;
	@s{qw(ppid pgrp session tty_nr tpgid flags minflt cminflt majflt cmajflt utime stime cutime cstime priority nice threads itrealvalue starttime vsize rss rsslim )} = split /\s+/,substr($stat,pos($stat));
	$s{rss} *= 4096;
	return (@s{qw(rss vsize)});
}

sub memcheck ($$$$) {
	my ($n,$obj,$method,$args) = @_;
	my ($rss1,$vsz1) = meminfo();
	my $cnt = 0;
	my $start = time;
	my $do;$do = sub {
		#warn "[$cnt/$n] call $method(@$args): @_";
		# diag Dumper \@_;
		return EV::unloop if ++$cnt >= $n;
		$obj->$method(@$args,$do);
	};$do->();
	EV::loop;
	my ($rss2,$vsz2) = meminfo();
	my $run = time - $start;
	warn sprintf "$method: %0.6fs/%d; %0.2f rps (%+0.2fk/%+0.2fk)",$run,$cnt, $cnt/$run, ($rss2-$rss1)/1024, ($vsz2 - $vsz1)/1024;
	if ($rss2 > $rss1 or $vsz2 > $vsz1) {
		warn sprintf "%0.2fM/%0.2fM -> %0.2fM/%0.2fM", $rss1/1024/1024,$vsz1/1024/1024, $rss2/1024/1024,$vsz2/1024/1024;
	}
	is 1, 1;
}

# my $tnt = tnt_run();
my $cfs = 0;
my $connected;
my $disconnected;

my $tnt = {
	port => 3301,
	host => '127.0.0.1'
};


my $SPACE_NAME = 'tester';


my $c; $c = EV::Tarantool->new({
	host => $tnt->{host},
	port => $tnt->{port},
	# spaces => $realspaces,
	reconnect => 0.2,
	connected => sub {
		warn "connected: @_";
		$connected++;
		EV::unloop;
	},
	connfail => sub {
		my $err = 0+$!;
		is $err, Errno::ECONNREFUSED, 'connfail - refused' or diag "$!, $_[1]";
		# $nc->(@_) if $cfs == 0;
		$cfs++;
		# and
		EV::unloop;
	},
	disconnected => sub {
		warn "discon: @_ / $!";
		$disconnected++;
		EV::unloop;
	},
});

$c->connect;
EV::loop;

my $t; $t = EV::timer 1.0, 0, sub {
	# diag Dumper $c->spaces;
	EV::unloop;
	undef $t;
};
EV::loop;

Renewer::renew_tnt($c, $SPACE_NAME, sub {
	EV::unloop;
});
EV::loop;

ok $connected > 0, "Connection is ok";

subtest 'Ping tests', sub {
	plan( skip_all => 'skip') if !$test_exec{ping};
	diag '==== Ping tests ===';
	$c->ping(sub {
		my $a = @_[0];
		diag Dumper \@_ if !$a;
		is $a->{code}, 0;
		EV::unloop;
	});
	EV::loop;
};

subtest 'Eval tests', sub {
	plan( skip_all => 'skip') if !$test_exec{eval};
	diag '==== Eval tests ====';
	$c->eval("return {'hey'}", [], sub {
		my $a = @_[0];
		diag Dumper \@_ if !$a;
		cmp_deeply $a, {
			count => 1,
			tuples => [ ['hey'] ],
			status => 'ok',
			code => 0,
			sync => ignore()
		};
		EV::unloop;
	});
	EV::loop;
};

subtest 'Call tests', sub {
	plan( skip_all => 'skip') if !$test_exec{call};
	diag '==== Call tests ====';
	$c->call('string_function', [], sub {
		my $a = @_[0];
		diag Dumper \@_ if !$a;
		cmp_deeply $a, {
			count => 1,
			tuples => [ ['hello world'] ],
			status => 'ok',
			code => 0,
			sync => ignore()
		};
		EV::unloop;
	});
	EV::loop;
};

subtest 'Select tests', sub {
	plan( skip_all => 'skip') if !$test_exec{select};
	diag '==== Select tests ====';


	my $_plan = [
		[[], {hash => 0}, {
			count => 4,
			tuples => [
						  ['t1','t2',2,[ 1, 2, 3, 'str1', 4 ]],
						  ['t1','t2',3,{35 => Types::Serialiser::false,33 => Types::Serialiser::true,key2 => 42,key1 => 'value1'}],
						  ['t1','t2',17,-745,'heyo'],
						  ['tt1','tt2',456, 5]
						],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],

		[{_t1=>'t1', _t2=>'t2'}, {hash => 1, iterator => 'LE'}, {
			count => 3,
			tuples => [
						{
						  '' => ['heyo'],
						  _t4 => -745,
						  _t1 => 't1',
						  _t2 => 't2',
						  _t3 => 17
						},
						{
						  _t4 => {35 => Types::Serialiser::false,33 => Types::Serialiser::true,key2 => 42,key1 => 'value1'},
						  _t1 => 't1',
						  _t2 => 't2',
						  _t3 => 3
						},
						{
						  _t4 => [1,2,3,'str1',4],
						  _t1 => 't1',
						  _t2 => 't2',
						  _t3 => 2
						},

					  ],
			status => 'ok',
			code => 0,
			sync => ignore()
		}]
	];

	for my $p (@$_plan) {
		$c->select($SPACE_NAME, $p->[0], $p->[1], sub {
			my $a = @_[0];
			diag Dumper \@_ if !$a;
			cmp_deeply $a, $p->[2];
			EV::unloop;
		});
		EV::loop;
	}
};


subtest 'Insert tests', sub {
	plan( skip_all => 'skip') if !$test_exec{insert};
	diag '==== Insert tests ====';

	my $_plan = [
		[["t1", "t2", 101, '-100', { a => 11, b => 12, c => 13 }], { replace => 0, hash => 0 }, {
			count => 1,
			tuples => [
						['t1', 't2', 101, -100, { a => 11, b => 12, c => 13 }]
					  ],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		[["t1", "t2", 17, '-100', { a => 11, b => 12, c => 13 }], { replace => 1, hash => 0 }, {
			count => 1,
			tuples => [
						['t1', 't2', 17, -100, { a => 11, b => 12, c => 13 }]
					  ],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		[{_t1 => "t1", _t2 => "t2", _t3 => 18, _t4 => '-100' }, { replace => 0, hash => 0 }, {
			count => 1,
			tuples => [
						['t1', 't2', 18, -100]
					  ],
			status => 'ok',
			code => 0,
			sync => ignore()
		}]
	];
	for my $p (@$_plan) {
		$c->insert($SPACE_NAME, $p->[0], $p->[1], sub {
			my $a = @_[0];
			diag Dumper \@_ if !$a;
			cmp_deeply $a, $p->[2];

			Renewer::renew_tnt($c, $SPACE_NAME, sub {
				EV::unloop;
			});
		});

		EV::loop;
	}
};


subtest 'Delete tests', sub {
	plan( skip_all => 'skip') if !$test_exec{delete};
	diag '==== Delete tests ====';

	my $_plan = [
		[['tt1', 'tt2', 456], {}, {
			count => 1,
			tuples => [
						{
						  _t1 => 'tt1',
						  _t2 => 'tt2',
						  _t3 => 456,
						  _t4 => 5
						}
					  ],
			status => 'ok',
			code => 0,
			sync => ignore()
		}]
	];

	for my $p (@$_plan) {
		$c->delete($SPACE_NAME, $p->[0], $p->[1], sub {
			my $a = @_[0];
			diag Dumper \@_ if !$a;
			cmp_deeply $a, $p->[2];

			Renewer::renew_tnt($c, $SPACE_NAME, sub {
				EV::unloop;
			});
		});
		EV::loop;
	}


};

subtest 'Update tests', sub {
	plan( skip_all => 'skip') if !$test_exec{update};
	diag '==== Update tests ====';


	my $_plan = [
		[{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [3 => '+', 50] ], { hash => 1 }, {
			count => 1,
			tuples => [
						{
							'' => [ 'heyo' ],
							_t1 => 't1',
							_t2 => 't2',
							_t3 => 17,
							_t4 => -695,
						}
					  ],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [3 => '+', -50] ], { hash => 0 }, {
			count => 1,
			tuples => [['t1', 't2', 17, -795, 'heyo']],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		[{_t1 => 'tt1',_t2 => 'tt2',_t3 => 456}, [ [3 => '&', 4] ], { hash => 0 }, {
			count => 1,
			tuples => [['tt1', 'tt2', 456, 4]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		[{_t1 => 'tt1',_t2 => 'tt2',_t3 => 456}, [ [3 => '^', 4] ], { hash => 0 }, {
			count => 1,
			tuples => [['tt1', 'tt2', 456, 1]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		[{_t1 => 'tt1',_t2 => 'tt2',_t3 => 456}, [ [3 => '|', 3] ], { hash => 0 }, {
			count => 1,
			tuples => [['tt1', 'tt2', 456, 7]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		[{_t1 => 'tt1',_t2 => 'tt2',_t3 => 456}, [ [3 => '#', 2] ], { hash => 0 }, {
			count => 1,
			tuples => [['tt1', 'tt2', 456]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [3 => '=', 12] ],  { hash => 1 }, {
			count => 1,
			tuples => [
						{
							'' => [ 'heyo' ],
							_t1 => 't1',
							_t2 => 't2',
							_t3 => 17,
							_t4 => 12,
						}
					  ],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [4 => '!', {a => 1, b => 2, c => 3}] ],  { hash => 1 }, {
			count => 1,
			tuples => [
						{
							'' => [ {a => 1, b => 2, c => 3}, 'heyo' ],
							_t1 => 't1',
							_t2 => 't2',
							_t3 => 17,
							_t4 => -745,
						}
					  ],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [4 => ':', 0, 3, 'romy'] ],  { hash => 1 }, {
			count => 1,
			tuples => [
						{
							'' => [ 'romyo' ],
							_t1 => 't1',
							_t2 => 't2',
							_t3 => 17,
							_t4 => -745,
						}
					  ],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [3 => '+', -50], [4 => '=', 'another_heyo'] ], { hash => 0 }, {
			count => 1,
			tuples => [['t1', 't2', 17, -795, 'another_heyo']],
			status => 'ok',
			code => 0,
			sync => ignore()
 		}]
	];

	for my $p (@$_plan) {
		$c->update($SPACE_NAME, $p->[0], $p->[1], $p->[2], sub {
			my $a = @_[0];
			diag Dumper \@_ if !$a;

			cmp_deeply($a, $p->[3]);

			Renewer::renew_tnt($c, $SPACE_NAME,sub {
				EV::unloop;
			});
		});
		EV::loop;
	}
};

subtest 'RTREE tests', sub {
	plan( skip_all => 'skip') if !$test_exec{RTREE};
	diag '==== RTREE tests ====';
	my $space = "rtree";

	Renewer::renew_tnt($c, $space, sub {
		EV::unloop;
	});
	EV::loop;


	my $_plan = [
		["select", [], {hash=>0}, {
			count => 0,
			tuples => [],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		["insert", ['a1', [1,2]], {hash=>0}, {
			count => 1,
			tuples => [['a1', [1,2]]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		["insert", ['a2', [5,6,7,8]], {hash=>0}, {
			count => 1,
			tuples => [['a2', [5,6,7,8]]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		["insert", ['a3', [9,10,11,12]], {hash=>0}, {
			count => 1,
			tuples => [['a3', [9,10,11,12]]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		["insert", ['a4', [5,6,10,15]], {hash=>0}, {
			count => 1,
			tuples => [['a4', [5,6,10,15]]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		["select", ['a1'], {hash=>0}, {
			count => 1,
			tuples => [['a1', [1,2]]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		["select", ['a2'], {hash=>0}, {
			count => 1,
			tuples => [['a2', [5,6,7,8]]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		["select", ['a3'], {hash=>0}, {
			count => 1,
			tuples => [['a3', [9,10,11,12]]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		["select", ['a4'], {hash=>0}, {
			count => 1,
			tuples => [['a4', [5,6,10,15]]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		["select", [[5,6,7,8]], {hash=>0, index=>'spatial', iterator=>'EQ'}, {
			count => 1,
			tuples => [['a2', [5,6,7,8]]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		["select", [[5,6,7,8]], {hash=>0, index=>'spatial', iterator=>'OVERLAPS'}, {
			count => 2,
			tuples => [['a2', [5,6,7,8]], ['a4', [5,6,10,15]]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		["select", [[5,6,7,8]], {hash=>0, index=>'spatial', iterator=>'GT'}, {
			count => 0,
			tuples => [],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		["select", [[5,6,7,8]], {hash=>0, index=>'spatial', iterator=>'GE'}, {
			count => 2,
			tuples => [['a2', [5,6,7,8]], ['a4', [5, 6, 10, 15]]],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
		["select", [[5,6,7,8]], {hash=>0, index=>'spatial', iterator=>'LT'}, {
			count => 0,
			tuples => [],
			status => 'ok',
			code => 0,
			sync => ignore()
		}],
	];

	for my $p (@$_plan) {
		my $op = $p->[0];
		$c->$op($space, $p->[1], $p->[2], sub {
			my $a = @_[0];
			diag Dumper \@_ if !$a;
			cmp_deeply $a, $p->[3];

			EV::unloop;
		});
		EV::loop;
	}


};

subtest 'Memory tests', sub {
	plan( skip_all => 'skip') if !$test_exec{memtest};
	diag '==== Memory tests ===';
	memcheck 50000, $c, "ping",[];
	memcheck 50000, $c, "call",["string_function",[]];
	memcheck 50000, $c, "select",[$SPACE_NAME,['t1']];
	memcheck 50000, $c, "select",[$SPACE_NAME,{ _t1 => 't1' }];
	memcheck 50000, $c, "insert",[$SPACE_NAME,['t1', 't2', 12, 100 ], { hash => 1, replace => 1 }];
};

done_testing()
