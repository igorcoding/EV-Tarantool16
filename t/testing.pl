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
	# $c->ping(sub {
	# 	my $a = @_[0];
	# 	say Dumper $a;
	# 	say "Pong";
	# 	EV::unloop;
	# });

	say Dumper $c->spaces;

	# $c->eval("return {'hey'}", [], sub {
	# 	my $a = \@_;
	# 	say Dumper $a;
	# 	say "done eval;";
	# 	EV::unloop;
	# });

	# return;

	# $c->call('string_function', [], sub {
	# 	my $a = \@_;
	# 	say Dumper $a;
	# 	say "done call;";
	# 	EV::unloop;
	# });

	# EV::loop;

	# $c->insert('tester', ["t1", "t2", 5, 47653], {replace => 0}, sub {
	# 	my $a = \@_;
	# 	say Dumper $a;
	# 	say "done insert;";

	# 	$c->select('tester', ["t1", "t2"], { hash => 1 }, sub {
	# 		my $a = \@_;
	# 		say Dumper $a;
	# 		say "done select;";

	# 		$c->delete('tester', ["t1", "t2", 5], { index => 2 }, sub {
	# 			my $a = \@_;
	# 			say Dumper $a;
	# 			say "done delete;";
	# 			EV::unloop;
	# 		});
	# 	});
	# });

# EV::loop;

	$c->select('tester', {
			_t1 => "t1",
			_t2 => "t2"
		}, { hash => 1, iterator => 'LE' }, sub {
		my $a = \@_;
		say Dumper $a;
		say "done select;";
		EV::unloop;
	});

	# $c->select('tester', [], { hash => 1, iterator => 'LE' }, sub {
	# 	my $a = \@_;
	# 	say Dumper $a;
	# 	say "done select;";
	# 	EV::unloop;
	# });

	# $c->update('tester', {
	# 		_t1 => 't1',
	# 		_t2 => 't2',
	# 		_t3 => 1
	# 	}, [
	# 		{
	# 			op => ':',
	# 			field_no => 4,
	# 			position => 1,
	# 			offset => 0,
	# 			argument => 'hello'
	# 		},
	# 		{
	# 			op => '+',
	# 			field_no => 3,
	# 			argument => '50'
	# 		}
	# 	],  { hash => 1 }, sub {
	# 	my $a = \@_;
	# 	say Dumper $a;
	# 	say "done update;";
	# 	EV::unloop;
	# });


	undef $t;
};

EV::loop;
