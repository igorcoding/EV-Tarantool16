package EV::Tarantool16;

use 5.010;
use strict;
use warnings;
use Types::Serialiser;

our $VERSION = '1.1.2';

use EV ();

require XSLoader;
XSLoader::load('EV::Tarantool16', $VERSION);

# Preloaded methods go here.

1;

=head1 NAME

EV::Tarantool16 - EV client for Tarantool 1.6

=head1 VESRION

Version 1.1.2

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

Tuple (ARRAYREF or HASHREF) that will be passed as argument in lua code

=item $opts

HASHREF of additional options to the request

=over 4

=item timeout => $timeout

Request execution timeout

=item hash => $hash

Use hash as result

=back

=back

=cut

=head2 call $function_name, $tuple_args, $opts, $cb->($result)

Execute eval request

=over 4

=item $function_name

Lua function that will be called in tarantool

=item tuple_args

Tuple (ARRAYREF or HASHREF) that will be passed as argument in lua code

=item $opts

HASHREF of additional options to the request

=over 4

=item timeout => $timeout

Request execution timeout

=item hash => $hash

Use hash as result

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

=item index => $index_id

Index id to use

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

=back

=back

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

=item index => $index_id

Index id to use

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

=item index => $index_id

Index id to use

=back

=back

=cut


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
