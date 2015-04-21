use strict;
use 5.010;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV;
use AE;
use EV::Tarantool;
use Time::HiRes 'sleep','time';
use Scalar::Util 'weaken';
use Errno;
use Test::More;
use Test::Deep;
use Data::Dumper;
use Getopt::Long;
use List::BinarySearch qw( binsearch binsearch_pos );

my $MS_IN_SEC = 1000;

sub rand_num {
	my ($min_size, $max_size) = @_;
	return $min_size + int(rand($max_size - $min_size));
}

sub generate_blobs {
	my ($max_count, $rps, $min_size, $max_size, $inserter, $keys, $done_cb) = @_;

	my $urandom = "/dev/urandom";
	open(my $fh, "<", $urandom) or die "cannot open < $urandom: $!";
	my $insert_period = 1.0 / $rps;

	my $i = 1;
	my $t; $t = AE::timer $insert_period, $insert_period, sub {
		if (defined($max_count) && $i > $max_count) {
			undef $t;
			$done_cb->();
			return;
		}
		my $key = sprintf "%020d", $i;
		printf "Generating %s\n", $key;
		++$i;
		my $blob_size = rand_num $min_size, $max_size;
		my $blob;
		read $fh, $blob, $blob_size;

		$inserter->($key, $blob, sub {
			my ($resp) = @_;
			if (defined($resp)) {
				push @$keys, $key;
			}
		});
	};
	return $t;
}

sub tnt_inserter {
	my ($c, $space_name, $key, $blob, $cb) = @_;

	$c->insert($space_name, [$key, $blob], {}, sub {
		my $a = @_[0];
		diag Dumper \@_ unless $a;
		$cb->($a) if $cb;
	});
}

sub create_tnt_inserter {
	my ($c, $space_name) = @_;

	return sub {
		my ($key, $blob, $cb) = @_;
		return tnt_inserter $c, $space_name, $key, $blob, $cb;
	};
}

sub inserter_mode {
	my ($c, $opts, $generated_keys, $cb) = @_;

	my $inserter = create_tnt_inserter $c, $opts->{space};
	generate_blobs $opts->{max_count}, $opts->{rps}, $opts->{min_blob}, $opts->{max_blob}, $inserter, $generated_keys, sub {
		$c->disconnect;
		undef $c;
		say 'Done.';
	};
}





sub running_stats {
	my ($data, $exec_time) = @_;
	my $old_mean = $data->{mean};
	my $old_count = $data->{count};

	$data->{count} += 1;

	$data->{mean} *= $old_count;
	$data->{mean} += $exec_time;
	$data->{mean} /= $data->{count};

	$data->{var} += $old_mean * $old_mean;
	$data->{var} *= $old_count;
	$data->{var} += $exec_time * $exec_time;
	$data->{var} /= $data->{count};
	$data->{var} -= $data->{mean} * $data->{mean};

	$data->{std} = sqrt($data->{var});

	if (!defined($data->{min}) or $exec_time < $data->{min}) {
		$data->{min} = $exec_time;
	}

	if (!defined($data->{max}) or $exec_time > $data->{max}) {
		$data->{max} = $exec_time;
	}
}

sub evaluate_percentiles {
	my ($datapoints, $stats_data, $percentiles) = @_;
	my @sorted_data = sort {$a <=> $b} @$datapoints;
	my @percent_ranks = ();
	my $N = @sorted_data;
	for my $d (1..$N) {
		my $rank = 100 / $N * ($d - 0.5);
		push @percent_ranks, $rank;
	}

	$stats_data->{percentiles} = {};

	for my $P (@$percentiles) {
		if ($P < $percent_ranks[0]) {
			$stats_data->{percentiles}->{$P} = $sorted_data[0];
		} elsif ($P > $percent_ranks[$#percent_ranks]) {
			$stats_data->{percentiles}->{$P} = $sorted_data[$#percent_ranks];
		} else {
			my $index = binsearch {$a <=> $b} $P, @percent_ranks;
			if (defined($index)) {
				$stats_data->{percentiles}->{$P} = $sorted_data[$index];
			} else {
				$index = binsearch_pos { $a <=> $b } $P, @percent_ranks;
				my $k = $index - 1;
				my $k_1 = $index;
				my $interp = ($P - $percent_ranks[$k]) / ($percent_ranks[$k_1] - $percent_ranks[$k]);
				$stats_data->{percentiles}->{$P} = $sorted_data[$k] + $interp * ($sorted_data[$k_1] - $sorted_data[$k]);
			}
		}
	}
}



sub select_blobs {
	my ($count, $rps, $max_id, $selector, $datapoints, $stats_data, $done_cb) = @_;

	my $period = 1.0 / $rps;

	my $i = 1;
	my $t; $t = AE::timer $period, $period, sub {
		if (defined($count) && $i > $count) {
			undef $t;
			$done_cb->();
			return;
		}
		my $id = rand_num 1, $max_id;
		my $key = sprintf "%020d", $id;
		# printf "Selecting %s\n", $key;
		++$i;

		my $begin_time = time;

		$selector->($key, sub {
			my ($resp) = @_;
			my $exec_time = time - $begin_time;
			if (defined($resp)) {
				push @{$datapoints->{success}}, $exec_time;
				running_stats $stats_data->{success}, $exec_time;
			} else {
				push @{$datapoints->{error}}, $exec_time;
				running_stats $stats_data->{error}, $exec_time;
			}
		});
	};
	return $t;
}

sub tnt_selector {
	my ($c, $space_name, $key, $cb) = @_;

	$c->select($space_name, [$key], {}, sub {
		my $a = @_[0];
		diag Dumper \@_ unless $a;
		$cb->($a) if $cb;
	});
}

sub create_tnt_selector {
	my ($c, $space_name) = @_;

	return sub {
		my ($key, $cb) = @_;
		return tnt_selector $c, $space_name, $key, $cb;
	};
}

sub selector_mode {
	my ($c, $opts, $datapoints, $stats_data, $cb) = @_;

	my $selector = create_tnt_selector $c, $opts->{space};
	select_blobs $opts->{count}, $opts->{rps}, $opts->{max_id}, $selector, $datapoints, $stats_data, sub {
		$c->disconnect;
		undef $c;
		say 'Done.';
	};
}

sub main() {
	my $tnt = {
		port => 3301,
		host => '127.0.0.1'
	};

	my $common_opts = {
		space => 'sophier',
		rps => 1,
	};

	my $inserter_opts = {
		max_count => undef,
		min_blob => 10000,
		max_blob => 30000
	};

	my $selector_opts = {
		count => undef,
		max_id => 50,
	};

	my $mode;
	my @modes = ('inserter', 'selector');

	GetOptions ("mode=s" => \$mode,

				"space=s" => \$common_opts->{space},
              	"rps=f"   => \$common_opts->{rps},

              	"max_count=i"  => \$inserter_opts->{max_count},
              	"min_blob=f"  => \$inserter_opts->{min_blob},
              	"max_blob=f"  => \$inserter_opts->{max_blob},

				"count=i"  => \$selector_opts->{count},
				"max_id=i"  => \$selector_opts->{max_id},
              	)
  				or die("Error in command line arguments\n");

	die("Mode not specified. Possible values: ", Dumper(\@modes)) unless $mode;
	if (not($mode ~~ @modes)) {
		die 'Unknown mode: ', $mode;
	}

	my $c;
	my $blob_gen_w;
	my $connected = 0;

	my @generated_keys = ();
	my $stats_data;
	my $datapoints = {
		success => [],
		error => []
	};

	my $sig_w; $sig_w = AE::signal "INT", sub {
		if (defined($c)) {
			if ($connected == 1) {
				$c->disconnect();
			}
			undef $c;
		}

		if (defined($blob_gen_w)) {
			$blob_gen_w = undef;
		}
	};

	$c = EV::Tarantool->new({
		host => $tnt->{host},
		port => $tnt->{port},
		# spaces => $realspaces,
		reconnect => 0.2,
		connected => sub {
			warn "connected: @_";
			$connected = 1;


			my $t; $t = AE::timer 1.0, 0, sub {
				undef $t;
				if ($mode eq 'inserter') {
					my %opts = (%$common_opts, %$inserter_opts);
					inserter_mode($c, \%opts, \@generated_keys, sub {
						EV::unloop;
					});
				} elsif ($mode eq 'selector') {
					my %opts = (%$common_opts, %$selector_opts);

					$stats_data = {
						success => {
							mean => 0.0,
							var => 0.0,
							std => 0.0,
							min => undef,
							max => undef,
							count => 0
						},
						error => {
							mean => 0.0,
							var => 0.0,
							std => 0.0,
							min => undef,
							max => undef,
							count => 0
						}
					};
					selector_mode($c, \%opts, $datapoints, $stats_data, sub {
						EV::unloop;
					});
				} else {
					EV::unloop;
					die('not implemented!');
				}
			};

		},
		connfail => sub {
			my $err = 0+$!;
			is $err, Errno::ECONNREFUSED, 'connfail - refused' or diag "$!, $_[1]";
			EV::unloop;
		},
		disconnected => sub {
			warn "discon: @_ / $!";
			EV::unloop;
		},
	});

	$c->connect;
	EV::loop;

	say Dumper \@generated_keys;

	evaluate_percentiles $datapoints->{success}, $stats_data->{success}, [50, 75, 90, 99];
	evaluate_percentiles $datapoints->{error}, $stats_data->{error}, [50, 75, 90, 99];
	say Dumper $stats_data;

}


main();
