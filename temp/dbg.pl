use strict;
use 5.010;
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV::Tarantool16;
use Data::Dumper;

sub runner {
    my ($n,$obj,$method,$args) = @_;
    my $cnt = 0;
    my $do;$do = sub {
        # warn "[$cnt/$n] call $method(@$args): @_";
        # diag Dumper \@_;
        return EV::unloop if ++$cnt >= $n;
        $obj->$method(@$args,$do);
    };$do->();
    EV::loop;
}

my $c; $c = EV::Tarantool16->new({
    host => '127.0.0.1',
    port => 3301,
    username => 'test_user',
    password => 'test_pass',
    reconnect => 0.2,
    timeout => 0,
    connected => sub {
        warn "connected: @_";
        EV::unloop;
    },
    connfail => sub {
        warn "connfail: @_ / $!";
        EV::unloop;
    },
    disconnected => sub {
        warn "discon: @_ / $!";
        EV::unloop;
    },
});

$c->connect;
EV::loop;

# $c->ping(sub {
#     my $a = @_[0];
#     warn Dumper \@_ if !$a;
#     EV::unloop;
# });
# EV::loop;

runner 500000, $c, "ping",[];
