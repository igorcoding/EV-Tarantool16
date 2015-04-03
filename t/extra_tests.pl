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
	# diag Dumper $c->spaces;
	EV::unloop;
	undef $t;
};
EV::loop;

ok $connected > 0, "Connection is ok";

# sub deletion {
# 	my ($args, $current, $cb) = @_;

# 	if ($current > scalar(@$args)) {
# 	# if ($current >= 1) {
# 		say Dumper $cb;
# 		$cb->();
# 	}

# 	say Dumper [@{@$args[$current]}[0..3]];

# 	$c->delete('tester', [@{@$args[$current]}[0..2]], sub {
# 		my $a = @_[0];
# 		say "deleted";
# 		# say Dumper \@_;
# 		deletion($args, $current + 1, $cb);
# 	});
# }

# $c->select('tester', [], { hash => 0 }, sub {
# 	my $a = @_[0];
# 	# say Dumper \@_;

# 	deletion $a->{tuples}, 0, sub {
# 		EV::unloop;
# 	};

# });

$c->eval("return {arg}", [120, 1, 22], sub {
	my $a = @_[0];
	say Dumper \@_;

	EV::unloop;
});

EV::loop;
