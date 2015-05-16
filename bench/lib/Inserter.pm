package Inserter;

use strict;
use 5.010;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV;
use AE;
use Data::Dumper;

use Util;

sub generate_key {
	my ($k, $total_count, $replace_mode) = @_;
	if (!$replace_mode) {
		++$$k;
	} else {
		$$k = Util::rand_num 1, $total_count + 1;
	}
	return sprintf "%020d", $$k;
}

sub generate_blobs {
	my ($size_selector, $max_count, $rps, $min_size, $max_size, $updater_mode, $inserter, $keys, $done_cb) = @_;

	my $urandom = "/dev/urandom";
	open(my $fh, "<", $urandom) or die "cannot open < $urandom: $!";
	my $insert_period = 1.0 / $rps;

	my $replace;
	my $log_str;
	if (defined($updater_mode) and $updater_mode) {
		$replace = 1;
		$log_str = 'Updating';
	} else {
		$replace = 0;
		$log_str = 'Generating';
	}

	my $t;

	$size_selector->(sub {
		my $total_count = $_[0];

		my $i = 1;
		my $k = 0;
		$t = AE::timer $insert_period, $insert_period, sub {
			if (defined($max_count) && $i > $max_count) {
				say "Finishing up. i = $i";
				undef $t;
				$done_cb->();
				return;
			}
			++$i;
			my $key = generate_key \$k, $total_count, $replace;
			# printf "%s %s\n", $log_str, $key;
			my $blob_size = Util::rand_num $min_size, $max_size;
			my $blob;
			read $fh, $blob, $blob_size;

			$inserter->($key, $blob, $replace, sub {
				my ($resp) = @_;
				if (defined($resp)) {
					push @$keys, $key;
				}
			});
		};
	});

	return $t;
}

sub tnt_inserter {
	my ($c, $space_name, $key, $blob, $replace, $cb) = @_;

	$c->insert($space_name, [$key, $blob], { replace => $replace }, sub {
		my $a = @_[0];
		# say Dumper \@_ unless $a;
		$cb->($a) if $cb;
	});
}

sub create_tnt_inserter {
	my ($c, $space_name) = @_;

	return sub {
		my ($key, $blob, $replace, $cb) = @_;
		return tnt_inserter $c, $space_name, $key, $blob, $replace, $cb;
	};
}



sub tnt_size_selector {
	my ($c, $space_name, $cb) = @_;
	$c->eval('return box.space.sophier:select({})', [], {}, sub {
		my $a = @_[0];
		my $size = @{$a->{tuples}->[0]};
		$cb->($size) if $cb;
	});
}

sub create_tnt_size_selector {
	my ($c, $space_name) = @_;

	return sub {
		my ($cb) = @_;
		return tnt_size_selector $c, $space_name, $cb;
	};
}

sub executor {
	my ($c, $opts, $cb) = @_;

	my $generated_keys = [];

	my $selector = create_tnt_size_selector $c, $opts->{space};
	my $inserter = create_tnt_inserter $c, $opts->{space};
	generate_blobs $selector, $opts->{count}, $opts->{rps}, $opts->{min_blob}, $opts->{max_blob}, $opts->{updater}, $inserter, $generated_keys, sub {
		$cb->($generated_keys);
	};
}

1;
