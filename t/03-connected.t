#!/usr/bin/env perl

use strict;
use EV;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV::Tarantool16;
use Test::More;

my $dis_call = 0;
my $c = EV::Tarantool16->new({
	host => 'localhost',
	port => 3301,
	reconnect => 0,
	connected => sub {
		pass "Connected";
	},
	connfail => sub {
		fail "No call";
	},
	disconnected => sub {
		pass "Disconnected";
		$dis_call = 1;
	},
});

is $c->state, 'INITIAL';
$c->connect;
is $c->state, 'RESOLVING';

my $w;$w = EV::timer 1,0,sub { undef $w; fail "Timed out"; exit; };

$c->connect;
EV::run( EV::RUN_ONCE )
	while $c->state ne 'CONNECTING';

undef $w;

if ($c->state eq 'CONNECTED') {
	$c->connect;
	$c->connect;
	$c->connect;

	is $dis_call, 0;

	$c->disconnect;

	is $dis_call, 1;

	is $c->state, 'DISCONNECTED';
}

undef $c;
done_testing;
