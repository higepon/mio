# Example: Access to Mio.
use strict;
use warnings;
use Cache::Memcached;
use Data::Dumper;

my $m = Cache::Memcached->new( { servers => ['127.0.0.1:11211']} );
$m->set( "hello" => "world" );
$m->set( "intel" => "cpu" );
$m->set( "japan" => "Tokyo" );

warn $m->get("hello");
warn $m->get("intel");
warn $m->get("japan");

my $href = $m->get_multi("mio:range-search", "he", "j", "10", "asc");

# Should print
# $VAR1 = {
#           'hello' => 'world',
#           'intel' => 'cpu'
#         };
warn Dumper $href;

