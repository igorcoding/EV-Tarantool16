package EV::Tarantool16::Multi;

use 5.010;
use strict;
use warnings;
no warnings 'uninitialized';
use Scalar::Util qw(weaken);
use EV::Tarantool16;
use Carp;
sub U(@) { $_[0] }

sub log_err {}
sub log_warn {
	my $self = shift;
	warn "@_\n" if $self->{log_level} >= 2;
}

sub new {
	my $pkg = shift;
	my $self = bless {
		timeout => 1,
		reconnect => 1/3,
		cnntrace => 1,
		ares_reuse => 0,
		wbuf_limit => 16000,
		servers => [],
		log_level => 3,
		one_connected => undef,
		connected => undef,
		all_connected => undef,
		one_disconnected => undef,
		disconnected => undef,
		all_disconnected => undef,
		@_,
		stores => [],
		connected_mode => 'any',
	},$pkg;
	
	my $servers = delete $self->{servers};
	$self->{servers} = [];
	
	my $i = 0;
	my $rws = 0;
	my $ros = 0;
	for (@$servers) {
		my $srv;
		my $id = $i++;
		if (ref) {
			$srv = { %$_, id => $id };
		} else {
			m{^(?:([^:]*?):([^:]*?)@)?([^:]+)(?::(\d+))};
			$srv = {
				rw       => 1,
				username => $1,
				password => $2,
				host     => $3,
				port     => $4 // 3301,
				id       => $id,
				gen      => 1,
			};
		}
		$srv->{node} = ($srv->{rw} ? 'rw' : 'ro' ) . ':' . $srv->{host} . ':' . $srv->{port};
		if ($srv->{rw}) { $rws++ } else { $ros++; }
		push @{$self->{servers}}, $srv;
		my $warned;
		$srv->{c} = EV::Tarantool16->new({
			username => $srv->{username},
			password => $srv->{password},
			host => $srv->{host},
			port => $srv->{port},
			timeout => $self->{timeout},
			reconnect => $self->{reconnect},
			read_buffer => 2*1024*1024,
			cnntrace => $self->{cnntrace},
			ares_reuse => $self->{ares_reuse},
			wbuf_limit => $self->{wbuf_limit},
			log_level => $self->{log_level},
			connected => sub {
				my $c = shift;
				@{ $srv->{peer} = {} }{qw(host port)} = @_;
				
				$self->_db_online( $srv );
			},
			connfail => sub {
				my ($c,$fail) = @_;
				$self->{connfail} ? $self->{connfail}( U($self,$c),$fail ) :
				!$warned++ && $self->log_warn("Connection to $srv->{node} failed: $fail");
			},
			disconnected => sub {
				my $c = shift;
				$srv->{gen}++;
				@_ and $srv->{peer} and $self->log_warn("Connection to $srv->{node}/$srv->{peer}{host}:$srv->{peer}{port} closed: @_");
				$self->_db_offline( $srv, @_ );
				
			},
		});
	}
	if (not $ros+$rws ) {
		die "Cluster could not ever be 'connected' since have no servers (@{$servers})\n";
	}
	
	return $self;
}

sub connect : method {
	my $self = shift;
	for my $srv (@{ $self->{servers} }) {
		$srv->{c}->connect;
	}
}

sub disconnect : method {
	my $self = shift;
	for my $srv (@{ $self->{servers} }) {
		$srv->{c}->disconnect;
	}
}

sub ok {
	my $self = shift;
	if (@_ and $_[0] ne 'any') {
		return @{ $self->{$_[0].'stores'} } > 0 ? 1 : 0;
	} else {
		return @{ $self->{stores} } > 0 ? 1 : 0;
	}
}

sub _db_online {
	my $self = shift;
	my $srv  = shift;
	
	my $first = (
		( @{ $self->{stores} } == 0 )
	) || 0;
	
	push @{ $self->{stores} }, $srv;
	
	my $event = "one_connected";
	my @args = ( U($self,$srv->{c}), @{ $srv->{peer} }{qw(host port)} );
	
	$self->{$event} && $self->{$event}( @args );
	$first and $self->{connected} and $self->{connected}( @args );
	
	if ( $self->{all_connected} and @{ $self->{servers} } == @{ $self->{stores} } ) {
		$self->{all_connected}( $self, $self->{stores} );
	}
}

sub _db_offline {
	my $self = shift;
	my $srv  = shift;
	my $c = $srv->{c};
	
	$self->{stores}   = [ grep $_ != $srv, @{ $self->{stores} } ];
	
	my $last = (
		( @{ $self->{stores} } == 0 )
	) || 0;
	
	my $event = "one_disconnected";
	
	my @args = ( U($self,$srv->{c}), @_ );
	$self->{$event} && $self->{$event}( @args );
	
	$last and $self->{disconnected} and $self->{disconnected}( @args );
	
	if( $self->{all_disconnected} and @{ $self->{stores} } == 0 ) {
		$self->{all_disconnected}( $self );
	}
}

=for rem
	RW     - send request only to RW node
	RO     - send request only to RO node
	ANY    - send request to any node
	ARO    - send request to any node, but prefer ro
	ARW    - send request to any node, but prefer rw
=cut

sub _srv_by_mode {
	my $self = shift;
	
	my $mode = $self->{connected_mode};
	my $srv;
	
	@{ $self->{stores} } or do { $_[-1]( undef, "Have no connected nodes for mode $mode" ), return };
	$srv = $self->{stores}[ rand @{ $self->{stores} } ];
	my $cb = pop;
	return $srv->{c}, $cb;
}

sub _srv_rw {
	my $self = shift;
	my $mode = $self->{connected_mode};
	
	@{ $self->{stores} } or do { $_[-1]( undef, "Have no connected nodes for mode rw" ), return };
	my $srv = $self->{stores}[ rand @{ $self->{stores} } ];

	my $cb = pop;
	return $srv->{c}, $cb;
}

sub ping : method {
	my ($srv,$cb)  = &_srv_by_mode or return;
	$srv->ping(@_,$cb);
}

sub eval : method {
	my ($srv,$cb)  = &_srv_by_mode or return;
	$srv->eval(@_,$cb);
}

sub call : method {
	my ($srv,$cb)  = &_srv_by_mode or return;
	$srv->call(@_,$cb);
}

sub lua : method {
	my ($srv,$cb)  = &_srv_by_mode or return;
	$srv->lua(@_,$cb);
}

sub select : method {
	my ($srv,$cb)  = &_srv_by_mode or return;
	$srv->select(@_,$cb);
}

sub insert : method {
	my ($srv,$cb)  = &_srv_rw or return;
	$srv->insert(@_,$cb);
}

sub delete : method {
	my ($srv,$cb)  = &_srv_rw or return;
	$srv->delete(@_,$cb);
}

sub update : method {
	my ($srv,$cb)  = &_srv_rw or return;
	$srv->update(@_,$cb);
}

sub each : method {
	my $self = shift;
	my $cb = pop;
	my $flags = shift;
	for my $s (@{ $self->{stores} }) {
		$cb->($s);
	}
}



1;
