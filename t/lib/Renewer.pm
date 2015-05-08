package Renewer;

use 5.010;
use strict;
use Test::More;
use Test::Deep;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV;
use EV::Tarantool;
use Time::HiRes 'sleep','time';
use Data::Dumper;
use Errno;
use Scalar::Util 'weaken';
# use AE;

sub insertion {
	my ($c, $space, $cb) = @_;

	$c->call("fill_$space", [], sub {
		my $a = @_[0];
		$cb->();
	});
}

sub deletion {
	my ($c, $space, $cb) = @_;

	$c->call("truncate_$space", [], sub {
		my $a = @_[0];
		# say Dumper \@_;
		$cb->();
	});
}

sub renew_tnt {
	my ($c, $space, $cb) = @_;

	deletion $c, $space, sub {
		insertion($c, $space, $cb);
	};
}

1;

# renew_tnt(sub {
# 	EV::unloop;
# });
# EV::loop;
