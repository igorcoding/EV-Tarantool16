use 5.010;
use strict;
use AE;
use EV;
use Scalar::Util 'weaken';
use Time::HiRes;
use Data::Dumper;

my $count = 2;
my $max = 20;

my $action;$action = sub {
	my $cb = shift;
	my $w;$w = AE::timer rand()/1, 0, sub {
		undef $w;
		$cb->();
	};
};

my $i = 0;
my $avg_time = 0;
my $total = 0;

my $t;$t = sub {
	# my $t = $t or return;

	if ($i >= $max) {
		say 'hey';
		undef $t;
		# EV::unloop;
		return;
	}
	++$i;
	printf "%.4f. started %d\n", EV::now, $i;
	my $begin = time;
	$action->(sub {
		if (defined $t) {
			printf "%.4f. finished %d\n", EV::now, $i;
			++$total;
			$avg_time += time - $begin;
			$t->();
		}
	});
};

$t->() for 1..$count;
weaken $t;

EV::loop;
say 'here';

$avg_time /= $total;
say Dumper $avg_time;
