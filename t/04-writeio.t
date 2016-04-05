#!/usr/bin/env perl

use 5.010;
use strict;
use Test::More;# skip_all => "TODO";
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV;
use EV::Tarantool16;
use Time::HiRes 'sleep','time';
use Data::Dumper;
use Errno;
use Scalar::Util 'weaken';
use Test::Tarantool16;

$EV::DIED = sub {
	warn "@_";
	EV::unloop;
	exit;
};

my $tnt = {
	name => 'tarantool_tester',
	port => 11723,
	host => '127.0.0.1',
	username => 'test_user',
	password => 'test_pass',
	initlua => do {
		my $file = 'provision/init.lua';
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
	logger  => sub { diag ( $tnt->{title},' ', @_ )},
	initlua => $tnt->{initlua},
	on_die  => sub { fail "tarantool $tnt->{name} is dead!: $!"; exit 1; },
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

my $w;$w = EV::timer 15,0,sub { undef $w; fail "Timed out"; exit; };

my $cfs = 0;
my $c = EV::Tarantool16->new({
	host => $tnt->{host},
	port => $tnt->{port},
	reconnect => 1,
	timeout => 10,
	connected => sub {
		my $c = shift;
		my %start;
		
		$start{$c->sync + 1} = 1;
		$c->call('dummy',['x'x(2**20)], sub {
			if ($_[0]) {
				delete $start{ $_[0]{sync} };
				pass "First big";
			} else {
				fail "First big $_[1]";
			}
		});
		my $start_sync = $c->sync;
		for (1..2) {
			my $id = $start_sync + $_;
			$start{$id} = ();
			$c->call('dummy',['x', $_], sub {
				if ($_[0]) {
					if (exists $start{ $_[0]{sync} }) {
						delete $start{ $_[0]{sync} };
						if (not %start) {
							pass "All done";
							$c->disconnect;
						}
					} else {
						fail "Duplicate response for $_[0]{sync}";
						EV::unloop;
					}
				} else {
					shift;
					fail "Request failed: @_";
					EV::unloop;
				}
			});
		}
	},
	connfail => sub {
		my $err = 0+$!;
		is $err, Errno::ECONNREFUSED, 'connfail - refused' or diag "$!, $_[1]";
		$cfs++
		and
			EV::unloop;
	},
	disconnected => sub {
		my $c = shift;
		warn "PL: Disconnected: @_";
		pass "Disconnected";
		EV::unloop;
	},
});

$c->connect;

EV::loop;
done_testing();
