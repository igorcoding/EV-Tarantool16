package Selector;

use strict;
use 5.010;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use Time::HiRes 'sleep','time';
use EV;
use AE;
use Data::Dumper;
use List::BinarySearch qw( binsearch binsearch_pos );
use Scalar::Util qw( weaken );

use Util;

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
	my $measured_rps = 0;
	my $rps_measures = 0;
	my $last_id = $i;

	my $tt; $tt = AE::timer 1, 1, sub {
		$measured_rps += $i - $last_id;
		$last_id = $i;
		++$rps_measures;
	};

	my $done_called = 0;
	my $t; $t = sub {
		# my $t = $t or return;
		if (defined($count) && $i > $count) {
			undef $t;
			# if (not $done_called) {
				# $done_called = 1;
				undef $tt;
				$measured_rps /= $rps_measures if $rps_measures != 0;
				say Dumper $measured_rps;
				$done_cb->();
			# }
			return;
		}
		my $id = Util::rand_num 1, $max_id;
		my $key = sprintf "%020d", $id;
		# printf "Selecting %s\n", $key;
		++$i;

		my $begin_time = time;
		printf "%.4f. started %d\n", EV::now, $i;

		$selector->($key, sub {
			if (defined $t) {
				my ($resp) = @_;
				my $exec_time = time - $begin_time;
				if (defined($resp)) {
					push @{$datapoints->{success}}, $exec_time;
					running_stats $stats_data->{success}, $exec_time;
				} else {
					push @{$datapoints->{error}}, $exec_time;
					running_stats $stats_data->{error}, $exec_time;
				}
				printf "%.4f. finished %d\n", EV::now, $i;
				$t->();
			}
		});
	};

	$t->() for 1..$rps;
	weaken($t);

	return $t;
}

sub tnt_selector {
	my ($c, $space_name, $key, $cb) = @_;

	$c->select($space_name, [$key], {}, sub {
		my $a = @_[0];
		say Dumper \@_ unless $a;
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

sub executor {
	my ($c, $opts, $cb) = @_;

	my $datapoints = {
		success => [],
		error => []
	};

	my $stats_data = {
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

	my $selector = create_tnt_selector $c, $opts->{space};
	select_blobs $opts->{count}, $opts->{rps}, $opts->{max_id}, $selector, $datapoints, $stats_data, sub {
		evaluate_percentiles $datapoints->{success}, $stats_data->{success}, $opts->{percentiles};
		evaluate_percentiles $datapoints->{error}, $stats_data->{error}, $opts->{percentiles};
		$cb->($stats_data);
	};
}

1;
