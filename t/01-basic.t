package main;

use 5.010;
use strict;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV;
use Time::HiRes 'sleep','time';
use Scalar::Util 'weaken';
use Errno;
use EV::Tarantool16;
use Test::More;
BEGIN{ $ENV{TEST_FAST} and plan 'skip_all'; }
use Test::Deep;
use Data::Dumper;
use Renewer;
use Carp;
use Test::Tarantool16;

$EV::DIED = sub {
	warn "@_";
	EV::unloop;
	exit;
};

my %test_exec = (
	ping => 1,
	eval => 1,
	call => 1,
	lua => 1,
	select => 1,
	insert => 1,
	replace => 1,
	delete => 1,
	update => 1,
	upsert => 1,
	RTREE => 1,
);

my $cfs = 0;
my $connected;
my $disconnected;

my $w = AnyEvent->signal (signal => "INT", cb => sub { exit 0 });

my $tnt = {
	name => 'tarantool_tester',
	port => 11723,
	host => '127.0.0.1',
	username => 'test_user',
	password => 'test_pass',
	initlua => do {
		my $file = 't/tnt/app.lua';
		local $/ = undef;
		open my $f, "<", $file
			or die "could not open $file: $!";
		my $d = <$f>;
		close $f;
		$d;
	}
};

$tnt = Test::Tarantool16->new(
	title   => $tnt->{name},
	host    => $tnt->{host},
	port    => $tnt->{port},
	logger  => sub { diag ( $tnt->{title},' ', @_ ) if $ENV{TEST_VERBOSE}; },
	initlua => $tnt->{initlua},
	wal_mode => 'write',
	on_die  => sub { my $self = shift; fail "tarantool $self->{title} is dead!: $!"; exit 1; },
);

$tnt->start(timeout => 10, sub {
	my ($status, $desc) = @_;
	if ($status == 1) {
		EV::unloop;
	} else {
		diag Dumper \@_;
	}
});
EV::loop;

$tnt->{cnntrace} = 0;
my $SPACE_NAME = 'tester';


my $c; $c = EV::Tarantool16->new({
	host => $tnt->{host},
	port => $tnt->{port},
	username => $tnt->{username},
	password => $tnt->{password},
	cnntrace => $tnt->{cnntrace},
	reconnect => 0.2,
	log_level => $ENV{TEST_VERBOSE} ? 4 : 0,
	connected => sub {
		diag Dumper \@_ unless $_[0];
		diag "connected: @_" if $ENV{TEST_VERBOSE};
		$connected++ if defined $_[0];
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
		diag "discon: @_ / $!" if $ENV{TEST_VERBOSE};
		$disconnected++;
		EV::unloop;
	},
});

$c->connect;
EV::loop;

# my $t; $t = EV::timer 1.0, 0, sub {
# 	# diag Dumper $c->spaces;
# 	EV::unloop;
# 	undef $t;
# };
# EV::loop;

Renewer::renew_tnt($c, $SPACE_NAME, sub {
	EV::unloop;
});
EV::loop;

ok $connected > 0, "Connection ok";
croak "Not connected normally" unless $connected > 0;



subtest 'Ping tests', sub {
	plan( skip_all => 'skip') if !$test_exec{ping};
	diag '==== Ping tests ===' if $ENV{TEST_VERBOSE};

	my $_plan = [
		[{}, [
			{
				schema_id => ignore(),
				sync => ignore(),
				code => 0
			}
		]],
		[2, [
			undef,
			"Opts must be a HASHREF"
		]],
		[[], [
			undef,
			"Opts must be a HASHREF"
		]],
		["", [
			undef,
			"Opts must be a HASHREF"
		]],
	];

	for my $p (@$_plan) {
		my $finished = 0;
		$c->ping($p->[0], sub {
			my $a = $_[0];
			cmp_deeply \@_, $p->[1];
			EV::unloop;
			$finished = 1;
		});
		if (!$finished) {
			EV::loop;
		}
	}


};

subtest 'Eval tests', sub {
	plan( skip_all => 'skip') if !$test_exec{eval};
	diag '==== Eval tests ====' if $ENV{TEST_VERBOSE};
	$c->eval("return {'hey'}", [], sub {
		my $a = $_[0];
		diag Dumper \@_ if !$a;
		cmp_deeply $a, {
			count => 1,
			tuples => [ ['hey'] ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		};
		EV::unloop;
	});
	EV::loop;
};

subtest 'Call tests', sub {
	plan( skip_all => 'skip') if !$test_exec{call};
	diag '==== Call tests ====' if $ENV{TEST_VERBOSE};
	$c->call('string_function', [], sub {
		my $a = $_[0];
		diag Dumper \@_ if !$a;
		cmp_deeply $a, {
			count => 1,
			tuples => [ ['hello world'] ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		};
		EV::unloop;
	});
	EV::loop;
};

subtest 'Lua tests', sub {
	plan( skip_all => 'skip') if !$test_exec{lua};
	diag '==== Lua tests ====' if $ENV{TEST_VERBOSE};
	$c->lua('string_function', [], sub {
		my $a = $_[0];
		diag Dumper \@_ if !$a;
		cmp_deeply $a, {
			count => 1,
			tuples => [ ['hello world'] ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		};
		EV::unloop;
	});
	EV::loop;
};

subtest 'Select tests', sub {
	plan( skip_all => 'skip') if !$test_exec{select};
	diag '==== Select tests ====' if $ENV{TEST_VERBOSE};


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
			sync => ignore(),
			schema_id => ignore(),
		}],

		[{_t1=>'t1', _t2=>'t2'}, {hash => 1, iterator => EV::Tarantool16::INDEX_LE}, {
			count => 3,
			tuples => [
						{
						  _t5 => 'heyo',
						  _t4 => -745,
						  _t1 => 't1',
						  _t2 => 't2',
						  _t3 => 17,
						},
						{
						  _t4 => {35 => Types::Serialiser::false,33 => Types::Serialiser::true,key2 => 42,key1 => 'value1'},
						  _t1 => 't1',
						  _t2 => 't2',
						  _t3 => 3,
						},
						{
						  _t4 => [1,2,3,'str1',4],
						  _t1 => 't1',
						  _t2 => 't2',
						  _t3 => 2,
						},

					  ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}]
	];

	for my $p (@$_plan) {
		$c->select($SPACE_NAME, $p->[0], $p->[1], sub {
			my $a = $_[0];
			diag Dumper \@_ if !$a;
			cmp_deeply $a, $p->[2];
			EV::unloop;
		});
		EV::loop;
	}
};


subtest 'Insert tests', sub {
	plan( skip_all => 'skip') if !$test_exec{insert};
	diag '==== Insert tests ====' if $ENV{TEST_VERBOSE};

	my $_plan = [
		[["t1", "t2", 101, '-100', { a => 11, b => 12, c => 13 }], { replace => 0, hash => 0 }, {
			count => 1,
			tuples => [
						['t1', 't2', 101, -100, { a => 11, b => 12, c => 13 }]
					  ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[["t1", "t2", 17, '-100', { a => 11, b => 12, c => 13 }], { replace => 1, hash => 0 }, {
			count => 1,
			tuples => [
						['t1', 't2', 17, -100, { a => 11, b => 12, c => 13 }]
					  ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => "t1", _t2 => "t2", _t3 => 18, _t4 => '-100' }, { replace => 0, hash => 0 }, {
			count => 1,
			tuples => [
						['t1', 't2', 18, -100, undef]
					  ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		
		# Not all fields supplied tests
		[{_t1 => "t1", _t2 => "t2", _t3 => 18, _t5 => '-100' }, { replace => 0, hash => 1 }, {
			count => 1,
			tuples => [
						{
							_t1 => 't1',
							_t2 => 't2',
							_t3 => 18,
							_t4 => undef,
							_t5 => '-100',
						}
					  ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => "t1", _t2 => "t2", _t3 => 18, }, { replace => 0, hash => 1 }, {
			count => 1,
			tuples => [
						{
							_t1 => 't1',
							_t2 => 't2',
							_t3 => 18,
							_t4 => undef,
							_t5 => undef
						}
					  ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
	];
	for my $p (@$_plan) {
		$c->insert($SPACE_NAME, $p->[0], $p->[1], sub {
			my $a = $_[0];
			diag Dumper \@_ if !$a;
			cmp_deeply $a, $p->[2];

			Renewer::renew_tnt($c, $SPACE_NAME, sub {
				EV::unloop;
			});
		});

		EV::loop;
	}
};

subtest 'Replace tests', sub {
	plan( skip_all => 'skip') if !$test_exec{replace};
	diag '==== Replace tests ====' if $ENV{TEST_VERBOSE};

	my $_plan = [
		[["t1", "t2", 101, '-100', { a => 11, b => 12, c => 13 }], { hash => 0 }, {
			count => 1,
			tuples => [
						['t1', 't2', 101, -100, { a => 11, b => 12, c => 13 }]
					  ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[["t1", "t2", 17, '-100', { a => 11, b => 12, c => 13 }], { hash => 0 }, {
			count => 1,
			tuples => [
						['t1', 't2', 17, -100, { a => 11, b => 12, c => 13 }]
					  ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => "t1", _t2 => "t2", _t3 => 18, _t4 => '-100' }, { hash => 0 }, {
			count => 1,
			tuples => [
						['t1', 't2', 18, -100, undef]
					  ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}]
	];
	for my $p (@$_plan) {
		$c->replace($SPACE_NAME, $p->[0], $p->[1], sub {
			my $a = $_[0];
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
	diag '==== Delete tests ====' if $ENV{TEST_VERBOSE};

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
			sync => ignore(),
			schema_id => ignore(),
		}]
	];

	for my $p (@$_plan) {
		$c->delete($SPACE_NAME, $p->[0], $p->[1], sub {
			my $a = $_[0];
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
	diag '==== Update tests ====' if $ENV{TEST_VERBOSE};


	my $_plan = [
		[{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [3 => '+', 50] ], { hash => 1 }, {
			count => 1,
			tuples => [
						{
							_t1 => 't1',
							_t2 => 't2',
							_t3 => 17,
							_t4 => -695,
							_t5 => 'heyo',
						}
					  ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [3 => '+', -50] ], { hash => 0 }, {
			count => 1,
			tuples => [['t1', 't2', 17, -795, 'heyo']],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 'tt1',_t2 => 'tt2',_t3 => 456}, [ [3 => '&', 4] ], { hash => 0 }, {
			count => 1,
			tuples => [['tt1', 'tt2', 456, 4]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 'tt1',_t2 => 'tt2',_t3 => 456}, [ [3 => '^', 4] ], { hash => 0 }, {
			count => 1,
			tuples => [['tt1', 'tt2', 456, 1]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 'tt1',_t2 => 'tt2',_t3 => 456}, [ [3 => '|', 3] ], { hash => 0 }, {
			count => 1,
			tuples => [['tt1', 'tt2', 456, 7]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 'tt1',_t2 => 'tt2',_t3 => 456}, [ [3 => '#', 2] ], { hash => 0 }, {
			count => 1,
			tuples => [['tt1', 'tt2', 456]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [3 => '=', 12] ],  { hash => 1 }, {
			count => 1,
			tuples => [
						{
							_t1 => 't1',
							_t2 => 't2',
							_t3 => 17,
							_t4 => 12,
							_t5 => 'heyo',
						}
					  ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [4 => '!', {a => 1, b => 2, c => 3}] ],  { hash => 1 }, {
			count => 1,
			tuples => [
						{
							_t5 => {a => 1, b => 2, c => 3},
							_t1 => 't1',
							_t2 => 't2',
							_t3 => 17,
							_t4 => -745,
							'' => ['heyo']
						}
					  ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [4 => ':', 0, 3, 'romy'] ],  { hash => 1 }, {
			count => 1,
			tuples => [
						{
							_t5 => 'romyo',
							_t1 => 't1',
							_t2 => 't2',
							_t3 => 17,
							_t4 => -745,
						}
					  ],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [3 => '+', -50], [4 => '=', 'another_heyo'] ], { hash => 0 }, {
			count => 1,
			tuples => [['t1', 't2', 17, -795, 'another_heyo']],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
 		}]
	];

	for my $p (@$_plan) {
		$c->update($SPACE_NAME, $p->[0], $p->[1], $p->[2], sub {
			my $a = $_[0];
			diag Dumper \@_ if !$a;

			cmp_deeply($a, $p->[3]);

			Renewer::renew_tnt($c, $SPACE_NAME,sub {
				EV::unloop;
			});
		});
		EV::loop;
	}
};


subtest 'Upsert tests', sub {
	plan( skip_all => 'skip') if !$test_exec{upsert};
	diag '==== Upsert tests ====' if $ENV{TEST_VERBOSE};

	my $_plan = [
		[{_t1 => 't1',_t2 => 't2',_t3 => 1}, [ [3 => '=', 10] ], { hash => 0 }, {
			count => 1,
			tuples => [['t1', 't2', 1, undef, undef]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 1}, [ [3 => '=', 10] ], { hash => 0 }, {
			count => 1,
			tuples => [['t1', 't2', 1, 10, undef]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 1}, [ [3 => '+', 4] ], { hash => 0 }, {
			count => 1,
			tuples => [['t1', 't2', 1, 14, undef]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 1}, [ [3 => '-', 3] ], { hash => 0 }, {
			count => 1,
			tuples => [['t1', 't2', 1, 11, undef]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 1}, [ [3 => '=', 8] ], { hash => 0 }, {
			count => 1,
			tuples => [['t1', 't2', 1, 8, undef]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 1}, [ [4 => '=', 17] ], { hash => 0 }, {
			count => 1,
			tuples => [['t1', 't2', 1, 8, 17]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		[{_t1 => 't1',_t2 => 't2',_t3 => 2}, [ [3 => '=', 17] ], { hash => 0 }, {
			count => 2,
			tuples => [
				['t1', 't2', 1, 8, 17],
				['t1', 't2', 2, undef, undef],
			],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
	];
	
	Renewer::renew_tnt($c, $SPACE_NAME, 0, sub {
		EV::unloop;
	});
	EV::loop;

	for my $p (@$_plan) {
		$c->upsert($SPACE_NAME, $p->[0], $p->[1], $p->[2], sub {
			$c->select($SPACE_NAME, [], { hash => 0 }, sub {
				my $a = $_[0];
				# diag Dumper \@_;# if !$a;
				cmp_deeply($a, $p->[3]);
				EV::unloop;
			});
		});
		EV::loop;
	}
	
	Renewer::renew_tnt($c, $SPACE_NAME, 1, sub {
		EV::unloop;
	});
	EV::loop;
};

subtest 'RTREE tests', sub {
	plan( skip_all => 'skip') if !$test_exec{RTREE};
	diag '==== RTREE tests ====' if $ENV{TEST_VERBOSE};
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
			sync => ignore(),
			schema_id => ignore(),
		}],
		["insert", ['a1', [1,2]], {hash=>0}, {
			count => 1,
			tuples => [['a1', [1,2]]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		["insert", ['a2', [5,6,7,8]], {hash=>0}, {
			count => 1,
			tuples => [['a2', [5,6,7,8]]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		["insert", ['a3', [9,10,11,12]], {hash=>0}, {
			count => 1,
			tuples => [['a3', [9,10,11,12]]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		["insert", ['a4', [5,6,10,15]], {hash=>0}, {
			count => 1,
			tuples => [['a4', [5,6,10,15]]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		["select", ['a1'], {hash=>0}, {
			count => 1,
			tuples => [['a1', [1,2]]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		["select", ['a2'], {hash=>0}, {
			count => 1,
			tuples => [['a2', [5,6,7,8]]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		["select", ['a3'], {hash=>0}, {
			count => 1,
			tuples => [['a3', [9,10,11,12]]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		["select", ['a4'], {hash=>0}, {
			count => 1,
			tuples => [['a4', [5,6,10,15]]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		["select", [[5,6,7,8]], {hash=>0, index=>'spatial', iterator=>'EQ'}, {
			count => 1,
			tuples => [['a2', [5,6,7,8]]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		["select", [[5,6,7,8]], {hash=>0, index=>'spatial', iterator=>EV::Tarantool16::INDEX_OVERLAPS}, {
			count => 2,
			tuples => [['a2', [5,6,7,8]], ['a4', [5,6,10,15]]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		["select", [[5,6,7,8]], {hash=>0, index=>'spatial', iterator=>'GT'}, {
			count => 0,
			tuples => [],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		["select", [[5,6,7,8]], {hash=>0, index=>'spatial', iterator=>EV::Tarantool16::INDEX_GE}, {
			count => 2,
			tuples => [['a2', [5,6,7,8]], ['a4', [5, 6, 10, 15]]],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
		["select", [[5,6,7,8]], {hash=>0, index=>'spatial', iterator=>EV::Tarantool16::INDEX_LT}, {
			count => 0,
			tuples => [],
			status => 'ok',
			code => 0,
			sync => ignore(),
			schema_id => ignore(),
		}],
	];

	for my $p (@$_plan) {
		my $op = $p->[0];
		$c->$op($space, $p->[1], $p->[2], sub {
			my $a = $_[0];
			diag Dumper \@_ if !$a;
			cmp_deeply $a, $p->[3];
			EV::unloop;
		});
		EV::loop;
	}


};

done_testing()
