package TestTarantool;

use strict;
use FindBin;
use Time::HiRes 'sleep';
use Exporter 'import';
use POSIX ':sys_wait_h';
use Fcntl 'O_NONBLOCK', 'F_SETFL';

our @EXPORT = our @EXPORT_OK = qw(tnt_run);

my %cf = (
	host => '127.1.1.1',
	port => '33013',
);

$cf{root} = "$FindBin::Bin/../";
$cf{tntroot} = "$cf{root}.tarantool";
our %PIDS;
END {
	if (%PIDS) {
		for my $pid (keys %PIDS) {
			terminate($pid);
		}
		exit;
	}
}

our %TERMINATING;

sub terminate($) {
	my $pid = shift;
	kill 0 => $pid or return;
	return if $TERMINATING{$pid};
	local $TERMINATING{$pid} = 1;
	my $reaped;
	my $old = $SIG{CHLD};
	local $SIG{CHLD} = sub {
		local ($!,$?);
		my $epid = waitpid -1, WNOHANG;
		unless($epid == $pid) {
			goto &$old if ref $old;
			return;
		}
		my $ecode = $? >> 8;
		#warn "pid $pid reaped with $ecode";
		delete $PIDS{$pid};
		$reaped = 1;
	};
	#warn "$$: TERM'ing $pid";
	kill TERM => $pid;
	for (1..100) {
		return warn("$$: Process gone after TERM") if $reaped;
		sleep 0.01;
	}
	kill KILL => $pid;
	for (1..100) {
		return warn("$$: Process gone after KILL") if $reaped;
		sleep 0.01;
	}
	warn "Process not gone";
}


sub tnt_run (;$) {
	
	mkdir($cf{tntroot});
	if (-e "$cf{tntroot}/tarantool.pid") {
		my $tpid = do { open my $f, '<',"$cf{tntroot}/tarantool.pid"; local $/; <$f> };
		kill KILL => $tpid or warn "$!";
	}
	my $config = do { my $pos = tell DATA; local $/; my $c = <DATA>; seek DATA,$pos,0; $c };
	$config =~ s/\n__END__\r?\n.*$/\n/s;
	$config =~ s/ %\{([^}]+)\} /$cf{$1}/xsg;
	my @files = split /^@@\s*(.+?)\s*\r?\n/m, $config;
	shift @files;
	
	my $f;
	open $f, '>:raw', "$cf{tntroot}/00000000000000000001.snap" or die "$!";
	print {$f} pack 'H*','534e41500a302e31310a0a1eabad10';
	close $f;
	while (@files) {
		my ($name, $data) = splice @files, 0, 2;
		open $f, '>:raw', "$cf{tntroot}/$name" or die "$!";
		print {$f} $data;
		close $f;
	}
	
	pipe(my $rd, my $wr) or die "pipe: $!";
	defined(my $cpid = fork) or die "Can't fork: $!";
	if ($cpid) {
		close $rd;
		$SIG{INT} = $SIG{TERM} = sub {exit};
		$PIDS{$cpid}++;
		my $obj = bless {
			%cf,
			pid => $cpid,
			wr  => $wr,
		},'TestTarantool';
		return $obj;
	} else {
		close $wr;
		fcntl $rd, F_SETFL, O_NONBLOCK;
		my $ppid = getppid();
		
		%PIDS = (); # child process should not kill others
		
		sleep 0.1;
		my $next_action = 'run';
		
		$SIG{INT} = 'IGNORE';
		
		while () {
			warn "TNTMASTER[$$]: Watching $ppid";
			
			kill 0 => $ppid or do {
				warn "TNTMASTER[$$]: lost parent. exit\n";
				exit;
			};
			warn "TNTMASTER[$$]: taking action $next_action";
			if ($next_action eq 'run' or $next_action eq 'wait') {
				$SIG{TERM} = 'DEFAULT';
				$SIG{USR1} = 'DEFAULT';
				$SIG{CHLD} = 'DEFAULT';
				
				sleep 0.3 if $next_action eq 'wait';
				defined(my $tpid = fork()) or die "fork 2 failed: $!";
				
				if ($tpid) {
					warn "TNTMASTER[$$]: forked TNT: $tpid";
					my $leave;
					$PIDS{$tpid}++;
					$SIG{CHLD} = sub {
						local ($!,$?);
						my $pid = waitpid -1, WNOHANG;
						return unless $pid == $tpid;
						delete $PIDS{$tpid};
						my $ecode = $? >> 8;
						warn "pid $tpid exited with $ecode";
						$leave = 1;
					};
					$SIG{TERM} = sub {
						warn "TNTMASTER[$$]: received TERM";
						$next_action = 'exit';
						terminate($tpid);
						$leave = 1;
					};
					
					while (!$leave) {
						my $r = sysread($rd,my $buf, 4096);
						if ($r) {
							chomp($buf);
							warn "TNTMASTER[$$]: Command: $buf";
							if ($buf eq 'stop') {
								kill STOP => $tpid;
							}
							elsif ($buf eq 'cont') {
								kill CONT => $tpid;
							}
							else {
								$next_action  = $buf;
								terminate($tpid);
								last;
							}
						}
						elsif (defined $r) {}
						else {
							next if $!{EINTR} or $!{EAGAIN};
							warn "$!";
						}
						kill 0 => $ppid or do {
							warn "lost parent $ppid: $!\n";
							terminate($tpid);
							exit;
						};
						sleep 0.01;
					}
					
				} else {
					exec("tarantool_box -c '$cf{tntroot}/test.conf'");
					die;
				}
			}
			else {
				exit;
			}
		}
		die;
	}
}

sub restart {
	syswrite($_[0]{wr},"wait\n");
}

sub stop {
	syswrite($_[0]{wr},"stop\n");
}

sub cont {
	syswrite($_[0]{wr},"cont\n");
}

sub exit : method {
	syswrite($_[0]{wr},"exit\n");
}

1;

__DATA__

@@ test.conf

custom_proc_title="db-test-ev-tnt"
slab_alloc_arena = 0.01
bind_ipaddr      = 127.1.1.1
primary_port     = 33013
secondary_port   = 33014
admin_port       = 33015
script_dir       = %{tntroot}
work_dir         = %{tntroot}
wal_mode         = none
# log_level        = 1

space[1] = {
	enabled = 1,
	index = [
		{ unique = 1, type = TREE, key_field = [ { fieldno = 0, type = STR } ] },
		{ unique = 0, type = TREE, key_field = [ { fieldno = 1, type = STR } ] },
		{ unique = 0, type = TREE, key_field = [ { fieldno = 2, type = NUM64 } ] },
	]
}


space[2] = {
	enabled = 1,
	index = [
		{ unique = 1, type = TREE, key_field = [ { fieldno = 0, type = STR }, { fieldno = 1, type = STR } ] },
		{ unique = 0, type = TREE, key_field = [ { fieldno = 5, type = STR } ] },
		{ unique = 0, type = TREE, key_field = [ { fieldno = 2, type = NUM64 } ] },
	]
}

@@ init.lua

box.insert(1,"test1","testx",123ULL);
box.insert(1,"test2","testx",456ULL);
