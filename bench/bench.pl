use strict;
use 5.010;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV;
use AE;
use EV::Tarantool16;
use Time::HiRes 'sleep','time';
use Scalar::Util 'weaken';
use Errno;
use Test::More;
use Test::Deep;
use Data::Dumper;
use Getopt::Long;

use Inserter;
use Selector;

sub main() {
	my $tnt = {
		port => 3301,
		host => '127.0.0.1'
	};

	my $common_opts = {
		space => 'sophier',
		rps => 1,
		count => undef
	};

	my $inserter_opts = {
		min_blob => 10,
		max_blob => 30,
		updater => 0
	};

	my $selector_opts = {
		max_id => 50,
		percentiles => [50, 75, 90, 99, 100]
	};

	my $mode;
	my @modes = ('inserter', 'selector', 'updater');

	GetOptions ("mode=s" => \$mode,

				"space=s" => \$common_opts->{space},
              	"rps=f"   => \$common_opts->{rps},
              	"count=i"  => \$common_opts->{count},

              	"min_blob=f"  => \$inserter_opts->{min_blob},
              	"max_blob=f"  => \$inserter_opts->{max_blob},

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

	my $starttime = time;


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

	$c = EV::Tarantool16->new({
		host => $tnt->{host},
		port => $tnt->{port},
		reconnect => 0.2,
		connected => sub {
			warn "connected: @_";
			$connected = 1;


			my $t; $t = AE::timer 1.0, 0, sub {
				undef $t;
				EV::unloop;
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

	if ($mode eq 'inserter') {
		my %opts = (%$common_opts, %$inserter_opts);
		Inserter::executor($c, \%opts, sub {
			my ($generated_keys) = @_;
			# say Dumper $generated_keys;
			EV::unloop;
		});
	} elsif ($mode eq 'selector') {
		my %opts = (%$common_opts, %$selector_opts);

		Selector::executor($c, \%opts, sub {
			my ($stats) = @_;
			say Dumper $stats;
			EV::unloop;
		});
	} elsif ($mode eq 'updater') {
		$inserter_opts->{updater} = 1;
		my %opts = (%$common_opts, %$inserter_opts);

		Inserter::executor($c, \%opts, sub {
			my ($generated_keys) = @_;
			# say Dumper $generated_keys;
			EV::unloop;
		});
	}else {
		EV::unloop;
		die('not implemented!');
	}
	EV::loop;

	if (defined($c)) {
		$c->disconnect;
		undef $c;
		my $elapsed = time - $starttime;
		say 'Done. time: ', $elapsed;
	}
}


main();
