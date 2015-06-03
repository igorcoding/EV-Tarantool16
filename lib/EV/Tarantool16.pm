package EV::Tarantool16;

use 5.010;
use strict;
use warnings;
use Types::Serialiser;

our $VERSION = '0.01';

use EV ();

require XSLoader;
XSLoader::load('EV::Tarantool16', $VERSION);

# Preloaded methods go here.

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

EV::Tarantool16 - Perl extension for Tarantool 1.6

=head1 SYNOPSIS

  use EV::Tarantool16;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for EV::Tarantool16, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Mons Anderson, E<lt>mons@cpan.orgE<gt>,
igorcoding, E<lt>igorcoding@gmail.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by igorcoding

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.16.1 or,
at your option, any later version of Perl 5 you may have available.


=cut
