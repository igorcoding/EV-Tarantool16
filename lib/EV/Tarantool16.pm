package EV::Tarantool16;

use 5.010;
use strict;
use warnings;
use Types::Serialiser;

our $VERSION = '1.27';

use EV ();

require XSLoader;
XSLoader::load('EV::Tarantool16', $VERSION);

=begin HTML

=head4 Build Status

<table>
    <tr>
        <td>master</td>
        <td><img src="https://travis-ci.org/igorcoding/EV-Tarantool16.svg?branch=master" alt="Travis CI Build status (master)" /></td>
    </tr>
</table>

=end HTML

=head1 NAME

EV::Tarantool16 - EV client for Tarantool 1.6

=head1 VESRION

Version 1.27

=cut

=head1 SYNOPSIS

    use EV::Tarantool16;
    my $c; $c = EV::Tarantool16->new({
        host => '127.0.0.1',
        port => 3301,
        username => 'test_user',
        password => 'test_passwd',
        reconnect => 0.2,
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

    $c->ping(sub {
        my $a = @_[0];
        diag Dumper \@_ if !$a;
        EV::unloop;
    });
    EV::loop;


=head1 SUBROUTINES/METHODS

=head2 new {option => value,...}

Create new EV::Tarantool16 instance.

=over 4

=item host => $address

Address connect to.

=item port => $port

Port connect to.

=item username => $username

Username.

=item password => $password

Password.

=item reconnect => $reconnect

Reconnect timeout.

=item log_level => $log_level

Logging level. Values: (0: None), (1: Error), (2: Warning), (3: Info), (4: Debug)

=item cnntrace => $cnntrace

Enable (1) or disable(0) evcnn tracing.

=item ares_reuse => $ares_reuse

Enable (1) or disable(0) c-ares connection reuse (default = 0).

=item wbuf_limit => $wbuf_limit

Write vector buffer length limit. Defaults to 16384. Set wbuf_limit = 0 to disable write buffer length check on every request.

=item connected => $sub

Called when connection to Tarantool 1.6 instance is established, authenticated successfully and retrieved spaces information from it.

=item connfail => $sub

Called when connection to Tarantool 1.6 instance failed.

=item disconnected => $sub

Called when Tarantool 1.6 instance disconnected.

=back


=cut

=head2 connect

Connect to Tarantool 1.6 instance. EV::Tarantool16->connected is called when connection is established.

=cut

=head2 disconnect

Disconnect from Tarantool 1.6 instance. EV::Tarantool16->disconnected is called afterwards.

=cut

=head2 ping $opts, $cb->($result)

Execute ping request

=over 4

=item $opts

HASHREF of additional options to the request

=over 4

=item timeout => $timeout

Request execution timeout

=back

=back

=cut

=head2 eval $lua_expression, $tuple_args, $opts, $cb->($result)

Execute eval request

=over 4

=item $lua_expression

Lua code that will be run in tarantool

=item tuple_args

Tuple (ARRAYREF) that will be passed as argument in lua code

=item $opts

HASHREF of additional options to the request

=over 4

=item timeout => $timeout

Request execution timeout

=item space => $space

This space definition will be used to decode response tuple

=item in => $in

Format for parsing input (string). One char is for one argument ('s' = string, 'n' = number, 'a' = array, '*' = anything (type is determined automatically))

=back

=back

=cut

=head2 call $function_name, $tuple_args, $opts, $cb->($result)

Execute eval request

=over 4

=item $function_name

Lua function that will be called in tarantool

=item tuple_args

Tuple (ARRAYREF) that will be passed as argument in lua code

=item $opts

HASHREF of additional options to the request

=over 4

=item timeout => $timeout

Request execution timeout

=item space => $space

This space definition will be used to decode response tuple

=item in => $in

Format for parsing input (string). One char is for one argument ('s' = string, 'n' = number, 'a' = array, '*' = anything (type is determined automatically))

=back

=back

=cut

=head2 select $space_name, $keys, $opts, $cb->($result)

Execute select request

=over 4

=item $space_name

Tarantool space name.

=item $keys

Select keys (ARRAYREF or HASHREF).

=item $opts

HASHREF of additional options to the request

=over 4

=item timeout => $timeout

Request execution timeout

=item hash => $hash

Use hash as result

=item index => $index

Index name or id to use

=item limit => $limit

Select limit

=item offset => $offset

Select offset

=item iterator => $iterator

Select iterator type. Possible values:
'EQ',
'REQ',
'ALL',
'LT',
'LE',
'GE',
'GT',
'BITS_ALL_SET',
'BITS_ANY_SET',
'BITS_ALL_NOT_SET',
'OVERLAPS',
'NEIGHBOR'

=item in => $in

Format for parsing input (string). One char is for one argument ('s' = string, 'n' = number, 'a' = array, '*' = anything (type is determined automatically))

=back

=back

=cut

=head2 insert $space_name, $tuple, $opts, $cb->($result)

Execute insert request

=over 4

=item $space_name

Tarantool space name.

=item $tuple

Tuple to be inserted (ARRAYREF or HASHREF).

=item $opts

HASHREF of additional options to the request

=over 4

=item timeout => $timeout

Request execution timeout

=item hash => $hash

Use hash as result

=item replace => $replace

Insert(0) or replace(1) a tuple

=item in => $in

Format for parsing input (string). One char is for one argument ('s' = string, 'n' = number, 'a' = array, '*' = anything (type is determined automatically))

=back

=back

=cut

=head2 replace $space_name, $tuple, $opts, $cb->($result)

Execute replace request (same as insert, but replaces tuple if already exists). ($opts->{replace} = 1)

=cut

=head2 update $space_name, $key, $operations, $opts, $cb->($result)

Execute update request

=over 4

=item $space_name

Tarantool space name.

=item $key

Select key where to perform update (ARRAYREF or HASHREF).

=item $operations

Update operations (ARRAYREF) in this format:
[$field_no => $operation, $operation_args]
Please refer to Tarantool 1.6 documentaion for more details.

=item $opts

HASHREF of additional options to the request

=over 4

=item timeout => $timeout

Request execution timeout

=item hash => $hash

Use hash as result

=item index => $index

Index name or id to use

=item in => $in

Format for parsing input (string). One char is for one argument ('s' = string, 'n' = number, 'a' = array, '*' = anything (type is determined automatically))

=back

=back

=cut

=head2 upsert $space_name, $tuple, $operations, $opts, $cb->($result)

Execute upsert request

=over 4

=item $space_name

Tarantool space name.

=item $tuple

A tuple that will be inserted to Tarantool if there is no tuple like it already (ARRAYREF or HASHREF).

=item $operations

Update operations (ARRAYREF) in this format:
[$field_no => $operation, $operation_args]
Please refer to Tarantool 1.6 documentaion for more details.

=item $opts

HASHREF of additional options to the request

=over 4

=item timeout => $timeout

Request execution timeout

=item hash => $hash

Use hash as result

=item in => $in

Format for parsing input (string). One char is for one argument ('s' = string, 'n' = number, 'a' = array, '*' = anything (type is determined automatically))

=back

=back

=cut

=head2 delete $space_name, $key, $opts, $cb->($result)

Execute delete request

=over 4

=item $space_name

Tarantool space name.

=item $key

Select key (ARRAYREF or HASHREF).

=item $opts

HASHREF of additional options to the request

=over 4

=item timeout => $timeout

Request execution timeout

=item hash => $hash

Use hash as result

=item index => $index

Index name or id to use

=item in => $in

Format for parsing input (string). One char is for one argument ('s' = string, 'n' = number, 'a' = array, '*' = anything (type is determined automatically))

=back

=back

=cut

=head2 lua $function_name, $args, $opts, $cb->($result)

Execute call request (added for backward compatibility with EV::Tarantool). See 'call' method.

=cut

*lua = \&call;

=head2 stats $cb->($result)

Get Tarantool stats

=head3 Result

Returns a HASHREF, consisting of the following data:

=over 4

=item arena

=over 4

=item size

Arena allocated size

=item used

Arena used size

=item slabs

Slabs memory use

=back

=item info

=over 4

=item lsn

Tarantool log sequence number

=item lut

Last update time (current_time - box_info.replication.idle)

=item lag

Replication lag

=item pid

Process pid

=item uptime

Server's uptime

=back

=item op

Total operations count for each operation (select, insert, ...)

=item space

Tuples count in each space

=back

=cut
sub stats {
	my $self = shift;
	my $cb   = pop;
	my $opts = shift;
	my $expression = qq{
		local fiber = require('fiber')
		local slab_info = box.slab.info()
		local stat = {}
		stat['arena'] = {}
		stat['arena']['size'] = slab_info.arena_size
		stat['arena']['used'] = slab_info.arena_used
		stat['arena']['slabs'] = 0
		for i,s in pairs(slab_info.slabs) do
			stat['arena']['slabs'] = stat['arena']['slabs'] + s.slab_count * s.slab_size
		end

		stat['arena']['free'] = slab_info.arena_size - slab_info.arena_used

		local box_info = box.info
		stat['info'] = {}
		stat['info']['lsn'] = box_info.server.lsn
		if (box_info.replication.status ~= 'off') then
			stat['info']['lut'] = fiber.time() - box_info.replication.idle
			stat['info']['lag'] = box_info.replication.lag
		end
		stat['info']['pid'] = box_info.pid
		stat['info']['uptime'] = box_info.uptime

		local ops = box.stat()
		stat['op'] = {}
		for op,op_info in pairs(ops) do
			stat['op'][string.lower(op)] = op_info.total
		end

		stat['space'] = {}
		for space_name,v in pairs(box.space) do
			if (not string.match(space_name, "[0-9]+") and v['engine'] ~= 'sysview') then
				stat['space'][space_name] = box.space[space_name]:len()
			end
		end

		return {stat}
	};

	my $eval_cb = sub {
		if (!$_[0]) {
			my $error_msg = $_[1];
			$cb->(undef, "Couldn\'t get stats. Error: $error_msg");
			return;
		}
		my $stat = $_[0]->{tuples}->[0]->[0];
		$cb->($stat);
	};

	if ($opts) {
		$self->eval($expression, [], $opts, $eval_cb);
	} else {
		$self->eval($expression, [], $eval_cb);
	}
}



=head1 RESULT

=head2 Success result

    count => 1,
    tuples => [
                {
                  _t1 => 'tt1',
                  _t2 => 'tt2',
                  _t3 => 456,
                  _t4 => 5
                }
              ],
    status => 'ok',
    code => 0,
    sync => 5

=over 4

=item count

Tuples count

=item tuples

Tuples themeselves

=item status

Status string ('ok')

=item code

Return code (0 if ok, else error code (L<https://github.com/tarantool/tarantool/blob/master/src/box/errcode.h>))

=item sync

Request id

=back

=cut


=head2 Error result

    [undef, $error_msg, {
        errstr => $error_msg,
        status => 'error',
        code => $error_code,
        sync => 3
    }]

=over 4

=item errstr

Error string

=item status

Status string ('error')

=item code

Return code (0 if ok, else error code (L<https://github.com/tarantool/tarantool/blob/master/src/box/errcode.h>))

=item sync

Request id

=back

=cut


=head1 AUTHOR

igorcoding, E<lt>igorcoding@gmail.comE<gt>,
Mons Anderson, E<lt>mons@cpan.orgE<gt>

=head1 BUGS

Please report any bugs or feature requests in L<https://github.com/igorcoding/EV-Tarantool16/issues>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2015 by igorcoding

This program is released under the following license: GPL

=cut

1;
