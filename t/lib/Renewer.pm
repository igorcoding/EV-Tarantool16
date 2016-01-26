package Renewer;

use 5.010;
use strict;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV;
use Data::Dumper;
use Errno;
use Test::More;


sub renew_tnt {
	my ($c, $space, $cb) = @_;
	my $c = shift;
	my $space = shift;
	my $cb = pop;
	my $do_fill = shift // 1;

	$c->call("truncate_$space", [], sub {
		if ($do_fill) {
			$c->call("fill_$space", [], sub {
				# my $a = $_[0];
				# diag Dumper $a;
				$cb->();
			});
		} else {
			$cb->();
		}
	});
}

1;
