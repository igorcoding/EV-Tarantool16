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
use Renewer;
use Devel::Leak;
use Devel::Peek;
# use AE;

my $var;
Devel::Leak::NoteSV($var);

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
		# warn "connected: @_";
		# $connected++;
		my $t; $t = EV::timer 1.0, 0, sub {
			# diag Dumper $c->spaces;
			EV::unloop;
			undef $t;
		};
		EV::loop;
		EV::unloop;
	},
	connfail => sub {
		# my $err = 0+$!;
		# is $err, Errno::ECONNREFUSED, 'connfail - refused' or diag "$!, $_[1]";
		# $nc->(@_) if $cfs == 0;
		# $cfs++;
		# and
		EV::unloop;
	},
	disconnected => sub {
		# warn "discon: @_ / $!";
		# $disconnected++;
		EV::unloop;
	},
});

# Dump($tnt);
# Dump($c);

# undef $c;

$c->connect;
EV::loop;




undef $c;
undef $tnt;

Devel::Leak::CheckSV($var);


# for (1..10) {
# $c->ping(sub {
# 	my $a = @_[0];
# 	say Dumper \@_ if !$a;
# 	# is $a->{code}, 0;
# 	EV::unloop;
# });
# EV::loop;
# }

# my $p = [[], {hash => 0}];

# for (1..10) {
# $c->select('tester', $p->[0], $p->[1], sub {
# 			my $a = @_[0];
# 			EV::unloop;
# 		});
# EV::loop;
# }
# undef $p;

# my $p = [["t1", "t2", 101, '-100', { a => 11, b => 12, c => 13 }], { replace => 0, hash => 0 }];

# for (1..10) {
# $c->insert('tester', $p->[0], $p->[1], sub {
# 	my $a = @_[0];

# 	EV::unloop;
# });
# EV::loop;
# }
# undef $p;

# my $p = [{_t1=>'t1', _t2=>'t2'}, {hash => 1, iterator => 'LE'}];
# for (1..10) {
# $c->select('tester', $p->[0], $p->[1], sub {
# 			my $a = @_[0];
# 			EV::unloop;
# 		});
# EV::loop;
# }
# undef $p;

# my $p = [['tt1', 'tt2', 456], {}];

# for (1..10) {
# $c->delete('tester', $p->[0], $p->[1], sub {
# 	my $a = @_[0];
# 	EV::unloop;
# });
# EV::loop;
# }
# undef $p;

# my $p = [{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [4 => ':', 0, 3, 'romy'] ],  { hash => 1 }];

# for (1..10) {
# $c->update('tester', $p->[0], $p->[1], $p->[2], sub {
# 	my $a = @_[0];
# 	EV::unloop;
# });
# EV::loop;
# }
# undef $p;



# Devel::Leak::CheckSV($var);
