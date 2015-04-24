package Util;

use strict;
use 5.010;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";

sub rand_num {
	my ($min_size, $max_size) = @_;
	return $min_size + int(rand($max_size - $min_size));
}

1;
