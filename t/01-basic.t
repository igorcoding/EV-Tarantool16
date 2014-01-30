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

=for rem
	{
	EV::Tarantool::test1();
	warn "XXX";
	}
	warn "YYY";
	exit;
	__END__
=cut

sub meminfo () {
	my $stat = do { open my $f,'<:raw',"/proc/$$/stat"; local $/; <$f> };
	$stat =~ m{ ^ \d+ \s+ \((.+?)\) \s+ ([RSDZTW]) \s+}gcx;
	my %s;
	@s{qw(ppid pgrp session tty_nr tpgid flags minflt cminflt majflt cmajflt utime stime cutime cstime priority nice threads itrealvalue starttime vsize rss rsslim )} = split /\s+/,substr($stat,pos($stat));
	$s{rss} *= 4096;
	return (@s{qw(rss vsize)});
}

$EV::DIED = sub {
	warn "@_";
	EV::unloop;
	exit;
};

sub memcheck ($$$$) {
	my ($n,$obj,$method,$args) = @_;
	my ($rss1,$vsz1) = meminfo();
	my $cnt = 0;
	my $start = time;
	my $do;$do = sub {
		#warn "[$cnt/$n] call $method(@$args): @_";
		return EV::unloop if ++$cnt >= $n;
		$obj->$method(@$args,$do);
	};$do->();
	EV::loop;
	my ($rss2,$vsz2) = meminfo();
	my $run = time - $start;
	warn sprintf "$method: %0.6fs/%d; %0.2f rps (%+0.2fk/%+0.2fk)",$run,$cnt, $cnt/$run, ($rss2-$rss1)/1024, ($vsz2 - $vsz1)/1024;
	if ($rss2 > $rss1 or $vsz2 > $vsz1) {
		warn sprintf "%0.2fM/%0.2fM -> %0.2fM/%0.2fM", $rss1/1024/1024,$vsz1/1024/1024, $rss2/1024/1024,$vsz2/1024/1024;
	}
}


my $spaces = {
		0 => {
			name => 'test',
			fields => [qw(id a b c)],
			types  => [qw(INT STR STR NUM NUM64 INT64 UTF)],
			indexes => {
				0 => {
					name => 'primary',
					fields => ['id','a'],
				},
				1 => {
					name => 'sec',
					fields => [qw(a b c)],
				},
				2 => {
					name => 'sec1',
					fields => [qw(a b c)],
				},
			}
		},
		1 => {
			name => 'test2',
			fields => [qw(id a b c)],
			types  => [qw(INT STR STR NUM NUM64 INT64 UTF)],
			indexes => {
				0 => {
					name => 'primary',
					fields => ['id'],
				},
				1 => {
					name => 'sec',
					fields => [qw(a b c)],
				},
			}
		}
	};
	
my $realspaces = {
	1 => {
		name => 'test1',
		fields => [qw( id a b c d e f )],
		types  => [qw(STR STR NUM64 )],
		indexes => {
			0 => { name => 'id', fields => ['id'] },
			1 => { name => 'ax', fields => ['a'] },
			2 => { name => 'bx', fields => ['b'] },
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

my $tnt = tnt_run();

	my $nc = sub {
		for (
			[ ping => [] ],
			[ lua => ['box.dostring',["return box.info.version"]] ],
			[ select => [ 0,[['key']] ] ],
			[ insert => [ 0,['key'] ] ],
			[ delete => [ 0,['key'] ] ],
		) {
			my ($method,$args) = @$_;
			$_[0]->$method(@$args, sub {
				is $_[0], undef, "$method - notconn retval";
				is $_[1],'Not connected', "$method - notconn error";
			});
		}
	};
	my $cfs = 0;
	my $connected;
	my $disconnected;
	my $c = EV::Tarantool->new({
		host => $tnt->{host},
		port => $tnt->{port},
		reconnect => 0.2,
		connected => sub {
			warn "connected: @_";
			$connected++;
			EV::unloop;
		},
		connfail => sub {
			my $err = 0+$!;
			is $err, Errno::ECONNREFUSED, 'connfail - refused' or diag "$!, $_[1]";
			$nc->(@_) if $cfs == 0;
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
	
	
	
	$nc->($c);
	$c->connect;
	EV::loop;
	is $cfs, 1, "Got one connfail";
	EV::loop;
	is $connected, 1, "Connected" or BAIL_OUT("Fail");
	
	#kill USR1 => $pid;
	#EV::loop;
	#exit;
	
	$c->lua('box.dostring',["return box.info.version"], sub {
		ok(ref $_[0], "Got result for lua") or BAIL_OUT();
		is $_[0]{status},'ok',   "lua - status ok";
		is $_[0]{count},1,       "lua - count ok";
		is 0+@{$_[0]{tuples}},1, "lua - tuples ok";
		EV::unloop;
	});
	EV::loop;
	
	$c->select(1,[['test1']], sub {
		ok(ref $_[0], "Got result for select") or BAIL_OUT();
		is $_[0]{status},'ok',   "select - status ok";
		is $_[0]{count},1,       "select - count ok";
		is 0+@{$_[0]{tuples}},1, "select - tuples ok";
		is_deeply $_[0]{tuples}[0], ['test1','testx',pack('Q',123)], 'select - result';
		EV::unloop;
	});
	EV::loop;
	
	$c->insert(2,['test1','test2','testtest','xxx','yyy','string'], { ret =>1 }, sub {
		ok(ref $_[0], "Got result for insert") or BAIL_OUT();
		is $_[0]{status},'ok',   "insert - status ok";
		is $_[0]{count},1,       "insert - count ok";
		is 0+@{$_[0]{tuples}},1, "insert - tuples ok";
		is_deeply $_[0]{tuples}[0], ['test1','test2','testtest','xxx','yyy','string'], 'insert - result';
		EV::unloop;
	});
	EV::loop;
	
	#TODO: update

	$c->delete(2,['test1','test2'], { ret =>1 }, sub {
		ok(ref $_[0], "Got result for delete") or BAIL_OUT();
		is $_[0]{status},'ok',   "delete - status ok";
		is $_[0]{count},1,       "delete - count ok";
		is 0+@{$_[0]{tuples}},1, "delete - tuples ok";
		is_deeply $_[0]{tuples}[0], ['test1','test2','testtest','xxx','yyy','string'], 'delete - result';
		EV::unloop;
	});
	EV::loop;
	
	$tnt->restart();
	
	while () {
		my $cur = $disconnected;
		EV::loop;
		#warn "unloop";
		last if ( $disconnected > $cur );
	}
	warn "got discon";
	
	while () {
		my $curcfs = $cfs;
		EV::loop;
		#warn "unloop";
		last if ( $cfs == $curcfs );
	}
	warn "connected again";
	
	undef $c;
	
	$connected = 0;
	my $c = EV::Tarantool->new({
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
			$nc->(@_) if $cfs == 0;
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
	$connected or die;
	
	#my $next;
	#$SIG{INT} = sub { $next = 1; };
	#sleep 0.01 while !$next;
	
	$c->update('test1',{id => 'test1'}, [[ 'a' => '=', 'new' ]], { ret => 1 }, sub {
		ok(ref $_[0], "Got result for update") or BAIL_OUT();
		#warn Dumper @_;
		is $_[0]{status},'ok',   "update - status ok";
		is $_[0]{count},1,       "update - count ok";
		is 0+@{$_[0]{tuples}},1, "update - tuples ok";
		is_deeply $_[0]{tuples}[0], { a => 'new', b => 123, id => 'test1' }, 'update - result';
		EV::unloop;
	});
	EV::loop;
	$c->select('test1',[{ id => 'test1' }], { hash => 1 }, sub {
		ok(ref $_[0], "Got result for select") or BAIL_OUT();
		#warn Dumper @_;
		is $_[0]{status},'ok',   "select - status ok";
		is $_[0]{count},1,       "select - count ok";
		is 0+@{$_[0]{tuples}},1, "select - tuples ok";
		is_deeply $_[0]{tuples}[0], { a => 'new', b => 123, id => 'test1' }, 'select - result';
		EV::unloop;
	});
	EV::loop;
	$c->select('test1',[{ a => 'new' }], { hash => 1, index => 'ax' }, sub {
		ok(ref $_[0], "Got result for select") or BAIL_OUT();
		#warn Dumper @_;
		is $_[0]{status},'ok',   "select - status ok";
		is $_[0]{count},1,       "select - count ok";
		is 0+@{$_[0]{tuples}},1, "select - tuples ok";
		is_deeply $_[0]{tuples}[0], { a => 'new', b => 123, id => 'test1' }, 'select - result';
		EV::unloop;
	});
	EV::loop;

	
	my ($cnt,$start);
	
	$ENV{MEMCHECK} or done_testing(),exit;
	
	memcheck 50000, $c,"ping",[];
	memcheck 50000, $c,"update",['test1',{ id => 'test1' }, [['a' => '=','new']], { ret => 0, hash => 1, }];
	undef $c;
	
	$cnt = 0;
	$c = EV::Tarantool->new({
		host => $tnt->{host},
		port => $tnt->{port},
		connected => sub {
			my $c = shift;
			$c->disconnect;
			return EV::unloop if ++$cnt >= 20000;
			$c->connect;
		},
		connfail => sub {
			my $c = shift;
			warn "@_ / $!";
		},
		disconnected => sub {
			my $c = shift;
			warn "disconnected: @_";
			EV::unloop;
			#$c->connect;
		},
	});
	my ($rss1,$vsz1) = meminfo();
	warn sprintf "%0.2fM/%0.2fM", $rss1/1024/1024,$vsz1/1024/1024;
	
	$c->connect;
	EV::loop;
	undef $c;
	
	my ($rss2,$vsz2) = meminfo();
	my $run = time - $start;
	warn sprintf "connect/disconnect: %0.6fs/%d; %0.2f rps (%+0.2fk/%+0.2fk)",$run,$cnt, $cnt/$run, ($rss2-$rss1)/1024, ($vsz2 - $vsz1)/1024;
	warn sprintf "%0.2fM/%0.2fM", $rss2/1024/1024,$vsz2/1024/1024;
	
	my ($rss1,$vsz1) = meminfo();
	for (1..2000) {
		if ($_ == 2) {
			($rss1,$vsz1) = meminfo();
		}
		my $s = EV::Tarantool->new({
			host => '0',
			port => 33013,
			spaces => $spaces,
			connected => sub {
				my $c = shift;
				warn "connected";
			},
		});
		$s->lua('x',[],{},sub {
			$s;
			EV::unloop;
		});
		EV::loop; # needed here to process postpone
	}
	my ($rss2,$vsz2) = meminfo();
	my $run = time - $start;
	warn sprintf "new/destroy: %0.6fs/%d; %0.2f rps (%+0.2fk/%+0.2fk)",$run,$cnt, $cnt/$run, ($rss2-$rss1)/1024, ($vsz2 - $vsz1)/1024;
	warn sprintf "%0.2fM/%0.2fM", $rss2/1024/1024,$vsz2/1024/1024;
	
	$connected = 0;
	$c = EV::Tarantool->new({
		host => $tnt->{host},
		port => $tnt->{port},
		spaces => $spaces,
		connected => sub {
			$connected++;
			EV::unloop;
		},
		connfail => sub {
			my $c = shift;
			warn "connfail: @_ / $!";
			EV::unloop;
		},
		disconnected => sub {
			my $c = shift;
			warn "disconnected: @_ / $!";
			EV::unloop;
		},
	});
	$c->connect;
	EV::loop;
	$connected or die;
	
	memcheck 50000, $c,"ping",[];
	memcheck 50000, $c,"select",[1,[['test1']]];
	memcheck 50000, $c,"select",['test',[{ id => 'test1' }]];
	memcheck 50000, $c,"insert",['test',{ id => 4, a => "xxx", c => "123" }, { ret => 1, hash => 1, }];
	#memcheck 50000, $c,"update",['test',{ id => 4, a => "xxx", c => "123" }, { ret => 1, hash => 1, }];
	
	done_testing();
