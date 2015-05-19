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
use Carp;
# use Devel::Leak;
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

my $cfs = 0;
my $connected;
my $disconnected;

my $tnt = {
	port => 3301,
	host => '127.0.0.1',
	username => 'test_user',
	password => 'test_pass',
};


my $SPACE_NAME = 'tester';


my $c; $c = EV::Tarantool->new({
	host => $tnt->{host},
	port => $tnt->{port},
	username => $tnt->{username},
	password => $tnt->{password},
	# spaces => $realspaces,
	reconnect => 0.2,
	connected => sub {
		diag Dumper \@_ unless $_[0];
		warn "connected: @_";
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
		warn "discon: @_ / $!";
		$disconnected++;
		EV::unloop;
	},
});

$c->connect;
EV::loop;

ok $connected > 0, "Connection is ok";
croak "Not connected normally" unless $connected > 0;


subtest 'Ping tests', sub {
	plan( skip_all => 'skip') if !$test_exec{ping};
	diag '==== Ping timeout tests ===';

	my $f = sub {
		my ($opt, $cmp) = @_;
		$c->ping($opt, sub {
			cmp_deeply \@_, $cmp;
			EV::unloop;
		});
		EV::loop;
	};


	my $plan = [
		[{timeout => 0.00001}, [
			undef,
			"Request timed out"
		]],
		[{}, [
			{
				sync => ignore(),
				code => 0
			}
		]],
		[{timeout => 0.1}, [
			{
				sync => ignore(),
				code => 0
			}
		]],
	];

	for my $p (@$plan) {
		$f->($p->[0], $p->[1]);
	}

};


subtest 'Select tests', sub {
	plan( skip_all => 'skip') if !$test_exec{ping};
	diag '==== Select timeout tests ===';

	my $f = sub {
		my ($key, $opt, $cmp) = @_;
		$c->select($SPACE_NAME, $key, $opt, sub {
			cmp_deeply \@_, $cmp;
			EV::unloop;
		});
		EV::loop;
	};


	my $plan = [
		[[], {timeout => 0.00001}, [
			undef,
			"Request timed out"
		]],
		[{}, {}, [
			{
				sync => ignore(),
				code => 0,
				status => "ok",
				count => ignore(),
				tuples => ignore()
			}
		]],
		[{}, {timeout => 0.1}, [
			{
				sync => ignore(),
				code => 0,
				status => "ok",
				count => ignore(),
				tuples => ignore()
			}
		]],
	];

	for my $p (@$plan) {
		$f->($p->[0], $p->[1], $p->[2]);
	}

};


done_testing();
