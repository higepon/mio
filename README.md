## What is mio
In short, mio is memcached + "range search".

mio suports range search queries like "get 10 datum which key are between key1 and key2".

N.B.
At present, it is in alpha quality.

## Building and Installing 
  1. Install a recent version of Erlang.
  2. git clone git://github.com/higepon/mio.git
  3. make
  4. make install with Environment variables for configuration.
     TARGET_DIR: Installation target directory.
     SBIN_DIR: sbin direcotry.

     e.g.
       sudo TARGET_DIR=/user/local/mio SBIN_DIR=/usr/sbin/ make install 

## Running mio

    # Run first node named mio1 with verbose mode on host exmpale.com (default port 11211)
    % mio -v -n mio1@FQDN_of_your_host


    # Run second node named mio2 on host example.com.
    # With -i option, indidate the introducer node.
    % mio -v -n mio2@example.com -i mio1@FQDN_of_your_host

    # Run third node named mio3 on host example.com (port 11411).
    % mio -v -n mio3@example.com -i mio1@FQDN_of_your_host -p 11411

## Access to mio
Use memcached client to access mio.
   
### Perl
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


### PHP
    
    $mem = new Memcached;
    $mem->addServer('127.0.0.1', 11211);
    $mem->set("hello", "world");
    $mem->set("intel", "cpu");
    $mem->set("japan", "Tokyo");
    printf("%s\n", $mem->get("hello"));
    printf("%s\n", $mem->get("intel"));
    printf("%s\n", $mem->get("japan"));
    var_dump($mem->getMulti(array("mio:range-search", "he", "j", "10", "asc")));
    ?>

### Java
    import com.danga.MemCached.*;
    import java.util.*;

    public class JavaMemcachedClient
    {
        public static void main(String[] args)
        {
            String[] serverlist = { "127.0.0.1:11211" };
            SockIOPool pool = SockIOPool.getInstance();
            pool.setServers(serverlist);
            pool.initialize();

            MemCachedClient mc = new MemCachedClient();
            mc.set("hello", "world");
            mc.set("intel", "cpu");
            mc.set("japan", "Tokyo");
            System.out.printf("hello => %s intel => %s japan => %s\n", mc.get("hello"), mc.get("intel"), mc.get("japan"));

            String[] keys = { "mio:range-search", "he", "j", "10", "asc" };
            Map<String, Object> ret = mc.getMulti(keys);

            for (Map.Entry<String, Object> e : ret.entrySet()) {
                System.out.println(e.getKey() + " : " + e.getValue());
            }
        }
    }



## Algorithm
Mio using "Skip Graph" algorithm.
See "Load Balancing and Locality in Range-Queries Data Structures" by James Aspnes.


## Author
Copyright (C) Cybozu Labs, Inc.

Written by Taro Minowa(Higepon) <higepon@labs.cybozu.co.jp>

## License
New BSD License
