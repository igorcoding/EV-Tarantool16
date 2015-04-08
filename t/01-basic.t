package main;

use 5.010;
use strict;
use Test::More;
use Test::Deep;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV;
use EV::Tarantool;
use Time::HiRes 'sleep','time';
use Data::Dumper;
use Errno;
use Scalar::Util 'weaken';
use Renewer;
# use AE;

my %test_exec = (
	ping => 1,
	eval => 1,
	call => 1,
	select => 1,
	insert => 1,
	delete => 1,
	update => 1
);

# my $tnt = tnt_run();
my $cfs = 0;
my $connected;
my $disconnected;

my $tnt = {
	port => 3301,
	host => '127.0.0.1'
};

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

Renewer::renew_tnt($c, sub {
	EV::unloop;
});
EV::loop;

ok $connected > 0, "Connection is ok";

subtest 'Ping tests', sub {
	diag '==== Ping tests ===';
	plan( skip_all => 'skip') if !$test_exec{ping};
	$c->ping(sub {
		my $a = @_[0];
		diag Dumper @_ if !$a;
		is $a->{code}, 0;
		EV::unloop;
	});
	EV::loop;
};

subtest 'Eval tests', sub {
	diag '==== Eval tests ===';
	plan( skip_all => 'skip') if !$test_exec{eval};
	$c->eval("return {'hey'}", [], sub {
		my $a = @_[0];
		diag Dumper @_ if !$a;
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
	diag '==== Call tests ===';
	plan( skip_all => 'skip') if !$test_exec{call};
	$c->call('string_function', [], sub {
		my $a = @_[0];
		diag Dumper @_ if !$a;
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
	diag '==== Select tests ===';
	plan( skip_all => 'skip') if !$test_exec{select};


	my $_plan = [
		[[], {hash => 0}, {
			count => 4,
			tuples => [
						  ['t1','t2',2,[ 1, 2, 3, 'str1', 4 ]],
						  ['t1','t2',3,{35 => Types::Serialiser::false,33 => Types::Serialiser::true,key2 => 42,key1 => 'value1'}],
						  ['t1','t2',17,-745,'heyo'],
						  ['tt1','tt2',456]
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
		$c->select('tester', $p->[0], $p->[1], sub {
			my $a = @_[0];
			diag Dumper @_ if !$a;
			cmp_deeply $a, $p->[2];
			EV::unloop;
		});
		EV::loop;
	}
};


subtest 'Insert tests', sub {
	diag '==== Insert tests ===';
	plan( skip_all => 'skip') if !$test_exec{insert};

	my $_plan = [
		[["t1", "t2", 101, '-100', { a => 11, b => 12, c => 13 }], { replace => 0, hash => 0 }, {
			count => 1,
			tuples => [
						['t1', 't2', 101, -100, { a => 11, b => 12, c => 13 }]
					  ],
			status => 'ok',
			code => 0,
			sync => ignore()
		}]
	];
	for my $p (@$_plan) {
		$c->insert('tester', $p->[0], $p->[1], sub {
			my $a = @_[0];
			diag Dumper @_ if !$a;
			cmp_deeply $a, $p->[2];

			Renewer::renew_tnt($c, sub {
				EV::unloop;
			});
		});

		EV::loop;
	}
};


subtest 'Delete tests', sub {
	diag '==== Delete tests ===';
	plan( skip_all => 'skip') if !$test_exec{delete};
	my $_plan = [
		[['tt1', 'tt2', 456], {}, {
			count => 1,
			tuples => [
						{
						  _t1 => 'tt1',
						  _t2 => 'tt2',
						  _t3 => 456
						}
					  ],
			status => 'ok',
			code => 0,
			sync => ignore()
		}]
	];

	for my $p (@$_plan) {
		$c->delete('tester', $p->[0], $p->[1], sub {
			my $a = @_[0];
			diag Dumper @_ if !$a;
			cmp_deeply $a, $p->[2];

			Renewer::renew_tnt($c, sub {
				EV::unloop;
			});
		});
	}


	EV::loop;
};

subtest 'Update tests', sub {
	diag '==== Update tests ===';
	plan( skip_all => 'skip') if !$test_exec{update};


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
		}]
	];

	for my $p (@$_plan) {
		$c->update('tester', $p->[0], $p->[1], $p->[2], sub {
			my $a = @_[0];
			diag Dumper @_ if !$a;

			cmp_deeply($a, $p->[3]);

			Renewer::renew_tnt($c, sub {
				EV::unloop;
			});
		});
		EV::loop;
	}
};

done_testing()
