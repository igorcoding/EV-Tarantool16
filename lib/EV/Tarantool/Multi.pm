package EV::Tarantool::Multi;

use 5.010;
use strict;
use warnings;
no warnings 'uninitialized';
use EV::Tarantool;

sub U(@) { $_[0] }

sub log_err {}
sub log_warn {
	shift;
	warn "@_\n"
}

sub new {
	my $pkg = shift;
	my $self = bless {
		timeout => 1,
		recovery_lag  => 1,
		reconnect => 1/3,
		connected_mode => 'any', # rw|ro|any - when to call 'connected'
		@_,
		stores => [],
		rwstores => [],
		rostores => [],
	},$pkg;
	
	my $servers = delete $self->{servers};
	my $spaces = delete $self->{spaces};
	$self->{servers} = [];
	
	my $i = 0;
	for (@$servers) {
		my $srv;
		my $id = $i++;
		if (ref) {
			$srv = { %$_, id => $id };
		}
		else {
			m{^(?:(rw|ro):|)([^:]+)(?::(\d+)|)};
			$srv = {
				rw   => $1 eq 'rw' ? 1 : 0,
				host => $2,
				port => $3 // 33013,
				id   => $id,
			};
		}
		$srv->{node} = ($srv->{rw} ? 'rw' : 'ro' ) . ':' . $srv->{host} . ':' . $srv->{port};
		push @{$self->{servers}}, $srv;
		my $warned;
		$srv->{c} = EV::Tarantool->new({
			host => $srv->{host},
			port => $srv->{port},
			timeout => $self->{timeout},
			reconnect => $self->{reconnect},
			spaces => $spaces,
			connected => sub {
				my $c = shift;
				@{ $srv->{peer} = {} }{qw(host port)} = @_;
				$warned = 0;
				$self->_db_online( $srv );
			},
			connfail => sub {
				my ($c,$fail) = @_;
				$self->{connfail} ? $self->{connfail}( U($self,$c),$fail ) :
				!$warned++ && $self->log_warn( "Connection to $srv->{node} failed: $fail" );
			},
			disconnected => sub {
				my $c = shift;
				@_ and $srv->{peer} and $self->log_warn( "Connection to $srv->{node}/$srv->{peer}{host}:$srv->{peer}{port} closed: @_" );
				$self->_db_offline( $srv, @_ );
				
			},
		});
		#$srv->{c}->connect;
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

sub _db_online {
	my $self = shift;
	my $srv  = shift;
	
	my $first = ( $self->{connected_mode} eq 'rw' ? ( $srv->{rw} && (@{ $self->{rwstores} } == 0) ) : ( @{ $self->{stores} } == 0 ) ) || 0;
	
	#warn "online $srv->{node} for $self->{connected_mode}; first = $first";
	
	push @{ $self->{stores} }, $srv;
	push @{ $self->{rwstores} }, $srv if $srv->{rw};
	push @{ $self->{rostores} }, $srv if !$srv->{rw};
	
	my $key = $srv->{rw} ? 'rw' : 'ro';
	my $event = "${key}_connected";
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
	$self->{stores}   = [ grep $_ != $c, @{ $self->{stores} } ];
	$self->{rwstores} = [ grep $_ != $c, @{ $self->{rwstores} } ] if $srv->{rw};
	$self->{rostores} = [ grep $_ != $c, @{ $self->{rostores} } ] if !$srv->{rw};
	
	my $last = ( $self->{connected_mode} eq 'rw' ? ( $srv->{rw} && (@{ $self->{rwstores} } == 0) ) : ( @{ $self->{stores} } == 0 ) ) || 0;
	
	my $key = $srv->{rw} ? 'rw' : 'ro';
	my $event = "${key}_disconnected";
	
	my @args = ( U($self,$srv->{c}), @_ );
	$self->{$event} && $self->{$event}( @args );
	
	$last and $self->{disconnected} and $self->{disconnected}( @args );
	
	if( @{ $self->{stores} } == 0 and $self->{all_disconnected} ) {
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
	my $mode;
	if ( @_ > 1 and !ref $_[-2] and $_[-2] =~ /^(?:r[ow]|any|a(?:ny|)r[ow])$/i  ) {
		$mode = splice @_, -2,1;
	} else {
		$mode = $self->{connected_mode};
	}
	my $srv;
	if ($mode eq 'rw') {
		@{ $self->{rwstores} } or do { $_[-1]( undef, "Have no connected nodes for mode $mode" ), return };
		$srv = $self->{rwstores}[ rand @{ $self->{rwstores} } ];
	}
	elsif ($mode eq 'ro' ) { # fb to any
		@{ $self->{rostores} } or do { $_[-1]( undef, "Have no connected nodes for mode $mode" ), return };
		$srv = $self->{rostores}[ rand @{ $self->{rostores} } ];
	}
	elsif ($mode eq 'arw') {
		@{ $self->{stores} } or do { $_[-1]( undef, "Have no connected nodes for mode $mode" ), return };
		$srv = @{ $self->{rwstores} } ? $self->{rwstores}[ rand @{ $self->{rwstores} } ] : $self->{rostores}[ rand @{ $self->{rostores} } ];
	}
	elsif ($mode eq 'aro') {
		@{ $self->{stores} } or do { $_[-1]( undef, "Have no connected nodes for mode $mode" ), return };
		$srv = @{ $self->{rostores} } ? $self->{rostores}[ rand @{ $self->{rostores} } ] : $self->{rwstores}[ rand @{ $self->{rwstores} } ];
	}
	else {
		@{ $self->{stores} } or do { $_[-1]( undef, "Have no connected nodes for mode $mode" ), return };
		$srv = $self->{stores}[ rand @{ $self->{stores} } ];
		
	}
	return $srv->{c};
}

sub ping : method {
	my $srv  = &_srv_by_mode or return;
	$srv->ping(@_);
}

sub lua : method {
	my $srv  = &_srv_by_mode or return;
	$srv->lua(@_);
}

sub select : method {
	my $srv  = &_srv_by_mode or return;
	$srv->select(@_);
}

sub insert : method {
	my $srv  = &_srv_by_mode or return;
	$srv->insert(@_);
}

sub delete : method {
	my $srv  = &_srv_by_mode or return;
	$srv->delete(@_);
}

sub update : method {
	my $srv  = &_srv_by_mode or return;
	$srv->update(@_);
}

sub each : method {
	my $self = shift;
	my $cb = pop;
	my $flags = shift;
	for (@{ $self->{stores} }) {
		$cb->($_);
	}
}



1;