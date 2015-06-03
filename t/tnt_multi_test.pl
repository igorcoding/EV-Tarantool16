use 5.010;
use strict;
use Test::More;
use Test::Deep;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV;
use EV::Tarantool16;
use EV::Tarantool16::Multi;
use Time::HiRes 'sleep','time';
use Data::Dumper;
use Errno;
use Scalar::Util 'weaken';
use Renewer;
use Devel::Leak;
use Devel::Peek;

my $servers = [
	'rw:|localhost:3302|',
	'rw:|localhost:3303|',
];

my $c = EV::Tarantool16::Multi->new(
	timeout             => 2.0,
	status_wait_timeout => 5.0,
	connected_mode      => 'rw',
	servers             => $servers,
	connected           => sub {
		warn "connected: @_";
		# EV::unloop;
	},
	all_connected       => sub {
		warn "all connected: @_";
		EV::unloop;
	},
	connfail            => sub {
		warn "connfail: @_ / $!";
		# EV::unloop;
	},
	disconnected        => sub {
		warn "discon: @_ / $!";
		# EV::unloop;
	},
);

$c->connect();

EV::loop;

$c->select('tester', [], {}, sub {
	say Dumper \@_;
});

EV::loop;
