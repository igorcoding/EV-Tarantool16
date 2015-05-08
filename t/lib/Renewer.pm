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

	$c->call("truncate_$space", [], sub {
		# my $a = @_[0];
		# $cb->();
		$c->call("fill_$space", [], sub {
			my $a = @_[0];
			# diag Dumper $a;
			$cb->();
		});
	});
}

1;
