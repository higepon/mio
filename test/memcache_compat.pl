use strict;
use warnings;
use Cache::Memcached::Fast;
use Cache::Memcached;

#my $m = Cache::Memcached::Fast->new( { servers => ['localhost:11211']} );
my $m = Cache::Memcached->new( { servers => ['localhost:11211']} );
$m->set( "hello" => "world" );
$m->set( "hi" => "japan" );

warn $m->get("helllo");
warn $m->get("hi");

## Cache::Memcached::Fast
my $href = $m->get_multi("mio:range-search", "he", "hi", "50");
warn $href->{"hello"};
warn $href->{"hi"};
