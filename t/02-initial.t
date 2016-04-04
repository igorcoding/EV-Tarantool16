#!/usr/bin/env perl

use strict;
use EV;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV::Tarantool16;
use Test::More;

my $c = EV::Tarantool16->new({
	host => 'localhost',
	port => 3301,
	reconnect => 1/3,
	connected => sub {
		fail "No call connected";
	},
	connfail => sub {
		fail "No call connfail";
	},
	disconnected => sub {
		fail "No call disconnected";
	},
});

$EV::DIED = sub {
	warn "@_";
	EV::unloop;
	exit;
};

is $c->state, 'INITIAL', 'Started from INITIAL';

# disconnect is a no-op
$c->disconnect;
$c->disconnect;
$c->disconnect;

$c->lua('',[],sub {
	is_deeply \@_, [undef, "Not connected"], 'Call failed';
});

{
local $SIG{ALRM} = sub { fail "loop locked"; exit; };
alarm 1;
EV::loop;
alarm 0;
}

$c->connect;

is $c->state, 'RESOLVING', 'Switched to RESOLVING';

$c->connect for 1..10;

$c->disconnect;

is $c->state, 'DISCONNECTED';

$c->connect;

is $c->state, 'RESOLVING';

$c->connect for 1..10;

$c->disconnect;

# while (1) {
# 	$c->connect;
# 	$c->disconnect;
# }

is $c->state, 'DISCONNECTED';

my $w;$w = EV::timer 1,0,sub { undef $w; fail "Timed out"; exit; };

$c->connect;
EV::run( EV::RUN_ONCE )
	while $c->state ne 'CONNECTING';

undef $w;


is $c->state, 'CONNECTING';

$c->connect for 1..10;

$c->disconnect;

is $c->state, 'DISCONNECTED';

my $w;$w = EV::timer 1,0,sub { undef $w; fail "Timed out"; exit; };

$c->connect;
EV::run( EV::RUN_ONCE )
	while $c->state ne 'CONNECTING';

undef $w;

is $c->state, 'CONNECTING';

diag "do reconnect";
$c->reconnect;

# Resolve is skipped on this step
is $c->state, 'CONNECTING';

undef $c;
done_testing;
