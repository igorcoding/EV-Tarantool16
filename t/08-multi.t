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
use EV::Tarantool16::Multi;
use Test::More;
use Test::Deep;
use Data::Dumper;
use Renewer;
use Carp;
use Test::Tarantool16;
# use Devel::Leak;
use AE;

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
	# memtest => 0
);

my $cfs = 0;
my $connected;
my $disconnected;

my $w = AnyEvent->signal (signal => "INT", cb => sub { exit 0 });

my $port = 11723;
my @required_tnts = (
	'127.0.0.1',
	'127.0.0.2',
);

my @tnts = map { {
	name => 'tarantool_tester',
	port => $port,
	host => $_,
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
} } @required_tnts;


@tnts = map { my $tnt = $_; Test::Tarantool16->new(
	title   => $tnt->{name},
	host    => $tnt->{host},
	port    => $tnt->{port},
	logger  => sub { diag ( $tnt->{title},' ', @_ )},
	initlua => $tnt->{initlua},
	on_die  => sub { fail "tarantool $tnt->{name} is dead!: $!"; exit 1; },
) } @tnts;

for (@tnts) {
	$_->start(timeout => 10, sub {
		my ($status, $desc) = @_;
		if ($status == 1) {
			EV::unloop;
		} else {
			diag Dumper \@_;
		}
	});
	EV::loop;
}

my $timeout = 5;
my $w; $w = AE::timer $timeout, 0, sub {
	undef $w;
	
	fail "Couldn't connect to Multi in $timeout seconds";
	EV::unloop;
};

my $c; $c = EV::Tarantool16::Multi->new(
	cnntrace => 0,
	reconnect => 0.2,
	log_level => 4,
	servers => [
		"127.0.0.1:$port",
		"127.0.0.2:$port",
	],
	connected => sub {
		diag Dumper \@_ unless $_[0];
		warn "connected: @_";
	},
	one_connected => sub {
		diag Dumper \@_ unless $_[0];
		$connected++ if defined $_[0];
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
	},
	all_connected => sub {
		diag Dumper \@_ unless $_[0];
		warn "all_connected: @_";
		is $connected, scalar @required_tnts, 'Connected to correct number of nodes';
		EV::unloop;
	},
	all_disconnected => sub {
		diag Dumper \@_ unless $_[0];
		warn "all_disconnected: @_";
		EV::unloop;
	}
);

$c->connect;
EV::loop;

for (1..100) {
	$c->ping(sub {
		isnt shift, undef, 'ping request successful';
		EV::unloop;
	});
	EV::loop;
}
done_testing;
