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

my $MS_IN_SEC = 1000;

sub rand_num {
	my ($min_size, $max_size) = @_;
	return $min_size + int(rand($max_size - $min_size));
}

sub generate_blobs {
	my ($max_count, $rps, $min_size, $max_size, $inserter, $done_cb) = @_;

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
		printf "Generating %d\n", $i;

		my $key = sprintf "%020d", $i;
		++$i;
		my $blob_size = rand_num $min_size, $max_size;
		my $blob;
		read $fh, $blob, $blob_size;

		$inserter->($key, $blob);
	};
	return $t;
}

sub tnt_inserter {
	my ($c, $space_name, $key, $blob, $cb) = @_;

	$c->insert($space_name, [$key, $blob], {}, sub {
		my $a = @_[0];
		if (!$a) {
			diag Dumper \@_ ;
		}
		$cb->() if $cb;
	});
}

sub create_tnt_inserter {
	my ($c, $space_name) = @_;

	return sub {
		my ($key, $blob, $cb) = @_;
		return tnt_inserter $c, $space_name, $key, $blob, $cb;
	};
}

sub main() {
	my $tnt = {
		port => 3301,
		host => '127.0.0.1'
	};

	my $space_name = 'sophier';
	my $rps = 10;
	my $max_count = 15;
	my $min_size = 10000;
	my $max_size = 30000;

	my $c;
	my $blob_gen_w;
	my $connected = 0;

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

				my $inserter = create_tnt_inserter $c, $space_name;
				generate_blobs $max_count, $rps, $min_size, $max_size, $inserter, sub {
					$c->disconnect;
					undef $c;
					say 'Done.';
					EV::unloop;
				};
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


}


main();
