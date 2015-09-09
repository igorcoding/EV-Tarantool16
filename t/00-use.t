use strict;
use warnings;

use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";

use Test::More tests => 1;
BEGIN { use_ok('EV::Tarantool16') };
