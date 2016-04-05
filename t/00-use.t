use strict;
use warnings;

use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";

use Test::More tests => 2;
BEGIN {
	use_ok('EV::Tarantool16');
	use_ok('EV::Tarantool16::Multi');
};
