#!/usr/bin/env perl

use 5.010;
use strict;
use Test::More;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV;
use EV::Tarantool;
use Time::HiRes 'sleep','time';
use Data::Dumper;
use Errno;
use Scalar::Util 'weaken';
use TestTarantool;
# use AE;

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
	reconnect => 0.2,
	connected => sub {
		warn "connected: @_";
		$connected++;
		my $t; $t = EV::timer 0.5, 0, sub {
			$c->ping(sub {
				say "Pong";
			});
			undef $t;
		};
		# EV::unloop;
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
