#!/usr/bin/env perl

package ttt;
use 5.010;
use strict;

$ttt::true  = do { bless \(my $dummy = 1) };

use overload (
   "0+"     => sub { ${$_[0]} },
   "++"     => sub { $_[0] = ${$_[0]} + 1 },
   "--"     => sub { $_[0] = ${$_[0]} - 1 },
   fallback => 1,
);


package main;

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
# use AE;

# my $tnt = tnt_run();
my $cfs = 0;
my $connected;
my $disconnected;

my $tnt = {
	port => 3301,
	host => '127.0.0.1'
};

my $s = $ttt::true + 1;
say Dumper($s);

my $realspaces = {
	1 => {
		name => 'test1',
		fields => [qw( id a b c d e f )],
		types  => [qw(STR STR NUM )],
		indexes => {
			0 => { name => 'id', fields => ['id', 'a', 'b'] },
			# 1 => { name => 'ax', fields => ['a'] },
			# 2 => { name => 'bx', fields => ['b'] },
		}
	},
	2 => {
		name => 'test2',
		fields => [qw( id a b c d e f )],
		types  => [qw(STR STR NUM64 )],
		indexes => {
			0 => { name => 'id', fields => ['id','a'] },
			1 => { name => 'ax', fields => ['e'] },
			2 => { name => 'bx', fields => ['b'] },
		}
	},
};

my $c; $c = EV::Tarantool->new({
	host => $tnt->{host},
	port => $tnt->{port},
	spaces => $realspaces,
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

my $t; $t = EV::timer 0.5, 0, sub {
	# $c->ping(sub {
	# 	my $a = @_[0];
	# 	say Dumper $a;
	# 	say "Pong";
	# 	EV::unloop;
	# });

	$c->select(1, ["t1", "t2"], sub {
		my $a = \@_;
		say Dumper $a;
		say "done;";
		EV::unloop;
	});
	undef $t;
};

EV::loop;
