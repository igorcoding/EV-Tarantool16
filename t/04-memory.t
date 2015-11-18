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
use Test::More;
use Test::Deep;
use Data::Dumper;
use Renewer;
use Carp;
use Test::Tarantool16;
use Proc::ProcessTable;

sub find_self_proc {
	my $t = Proc::ProcessTable->new();
	my $proc;
	for my $p ( @{$t->table} ){
		if ($p->pid == $$) {
			$proc = $p;
			last;
		}
	}
	if (!$proc) {
		die "Couldn't find self process in ProcessTable";
	}
	return $proc;
};


my $w = AnyEvent->signal (signal => "INT", cb => sub { exit 0 });

my $tnt = {
	name => 'tarantool_tester',
	port => 3301,
	host => '127.0.0.1',
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
};

$tnt = Test::Tarantool16->new(
	# cleanup => 0,
	title   => $tnt->{name},
	host    => $tnt->{host},
	port    => $tnt->{port},
	# logger  => sub { diag (map { (my $line =$_) =~ s{^}{$self->{name}: }mg } @_) if $ENV{TEST_VERBOSE}},
	# logger  => sub { },
	logger  => sub { diag ( $tnt->{title},' ', @_ )},
	initlua => $tnt->{initlua},
	on_die  => sub { fail "tarantool $tnt->{name} is dead!: $!"; exit 1; },
	# tarantool_cmd => "/opt/mailru/tarantool16/root/usr/bin/tarantool %{args}"
);
# warn Dumper $tnt;
# __END__

$tnt->start(timeout => 10, sub {
	my ($status, $desc) = @_;
	if ($status == 1) {
		EV::unloop;
	}
});
EV::loop;

$tnt->{cnntrace} = 0;
my $SPACE_NAME = 'tester';

my $c;

sub meminfo () {
	my $proc = find_self_proc();
	my %s = (
		vsize => $proc->size,
		rss => $proc->rss,
	);
	return (@s{qw(rss vsize)});
}

sub memcheck ($$$$) {
	my ($n,$obj,$method,$args) = @_;
	my ($rss1,$vsz1) = meminfo();
	my $cnt = 0;
	my $start = time;
	my $do;$do = sub {
		#warn "[$cnt/$n] call $method(@$args): @_";
		# diag Dumper \@_;
		return EV::unloop if ++$cnt >= $n;
		$obj->$method(@$args,$do);
	};$do->();
	EV::loop;
	my $run = time - $start;
	my ($rss2,$vsz2) = meminfo();
	warn sprintf "$method: %0.6fs/%d; %0.2f rps (%+0.2fk/%+0.2fk)",$run,$cnt, $cnt/$run, ($rss2-$rss1)/1024, ($vsz2 - $vsz1)/1024;
	if ($rss2 > $rss1 or $vsz2 > $vsz1) {
		warn sprintf "%0.2fM/%0.2fM -> %0.2fM/%0.2fM", $rss1/1024/1024,$vsz1/1024/1024, $rss2/1024/1024,$vsz2/1024/1024;
	}
	is 1, 1;
}


diag '==== Memory tests ====';

subtest 'connect/disconnect test', sub {
	# plan( skip_all => 'skip');
	
	for (0..5) {
		my $cnt = 0;
		my $start = time;
		my $max_cnt = 10000;
		undef $c;

		my ($rss1,$vsz1) = meminfo();

		$c = EV::Tarantool16->new({
			host => $tnt->{host},
			port => $tnt->{port},
			username => $tnt->{username},
			password => $tnt->{password},
			reconnect => 0.2,
			log_level => 1,
			cnntrace => $tnt->{cnntrace},
			connected => sub {
				my $c = shift;
				diag Dumper \@_ unless $_[0];
				$c->disconnect;
				return EV::unloop if ++$cnt >= $max_cnt;
				$c->connect;
			},
			connfail => sub {
				my $c = shift;
				warn "@_ / $!";
				EV::unloop;
			},
			disconnected => sub {
				# warn "discon: @_ / $!";
				# EV::unloop;
			},
		});

		$c->connect;
		EV::loop;
		undef $c;

		my $run = time - $start;
		my ($rss2,$vsz2) = meminfo();
		warn sprintf "connect/disconnect: %0.6fs/%d; %0.2f rps (%+0.2fk/%+0.2fk)",$run,$cnt, $cnt/$run, ($rss2-$rss1)/1024, ($vsz2 - $vsz1)/1024;
		if ($rss2 > $rss1 or $vsz2 > $vsz1) {
			warn sprintf "%0.2fM/%0.2fM -> %0.2fM/%0.2fM", $rss1/1024/1024,$vsz1/1024/1024, $rss2/1024/1024,$vsz2/1024/1024;
		}
	}
	is 1, 1;
};

subtest 'basic memory test', sub {
	$c = EV::Tarantool16->new({
		host => $tnt->{host},
		port => $tnt->{port},
		username => $tnt->{username},
		password => $tnt->{password},
		reconnect => 0.2,
		log_level => 1,
		cnntrace => $tnt->{cnntrace},
		connected => sub {
			EV::unloop;
		},
		connfail => sub {
			my $c = shift;
			warn "@_ / $!";
			EV::unloop;
		},
		disconnected => sub {
			warn "@_ / $!";
			EV::unloop;
		},
	});

	$c->connect;
	EV::loop;


	memcheck 50000, $c, "ping",[];
	memcheck 50000, $c, "eval",["return {'hey'}", []];
	memcheck 50000, $c, "call",["string_function",[]];
	memcheck 50000, $c, "select",[$SPACE_NAME,{ _t1 => 't1' }];
	memcheck 50000, $c, "replace",[$SPACE_NAME,['t1', 't2', 12, 100 ], { hash => 1}];
	memcheck 50000, $c, "update",[$SPACE_NAME,{_t1 => 't1',_t2 => 't2',_t3 => 17}, [ [3 => '+', 1] ], { hash => 1 }];
	memcheck 50000, $c, "eval",["return {box.info}", [], { timeout => 0.00001 }];
};

done_testing;
