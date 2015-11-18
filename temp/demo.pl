use 5.010;
use strict;
use Test::More;
use Test::Deep;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV;
use EV::Tarantool16;
use Time::HiRes 'sleep','time';
use Data::Dumper;
use Errno;
use Scalar::Util 'weaken';
use Renewer;

my $cfs = 0;
my $connected;
my $disconnected;

my $tnt = {
	port => 3301,
	host => '127.0.0.1',
	username => 'test_user',
	password => 'test_pass',
};

my $cnt = 0;
my $max_cnt = 30000;

my $c; $c = EV::Tarantool16->new({
	host => $tnt->{host},
	port => $tnt->{port},
	username => $tnt->{username},
	password => $tnt->{password},

	# spaces => $realspaces,
	reconnect => 0.2,
	log_level => 4,
	connected => sub {
		warn "connected: @_";
		$connected++;
		EV::unloop;
	},
	connfail => sub {
		warn "connfail: @_";
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

$c->select('_space', [], {}, sub {
	say Dumper \@_;
	EV::unloop;
});
EV::loop;

# $c->insert('memier', [7, {a => 1, b => 2}], { in => 's*' }, sub {
# 	say Dumper \@_;
# 	EV::unloop;
# });
# EV::loop;


# $c->call('get_test_tuple', [], {space => 'tester'}, sub {
# 	say Dumper \@_;
# 	EV::unloop;
# });
# EV::loop;

# $c->update('test', [
# 	'a',
# 	'b'
# ], [
# 	[4 => '=', 'T']
# ], {index => 'ident'}, sub {
# 	say Dumper \@_;
# 	EV::unloop;
# });
# EV::loop;

# $c->ping("", sub {
# 	say Dumper \@_;
# 	EV::unloop;
# });
# EV::loop;
