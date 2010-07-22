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
   
    # Perl example
    use strict;
    use warnings;
    use Cache::Memcached::Fast;
    use Cache::Memcached;

    my $m = Cache::Memcached->new( { servers => ['example.com:11211']} );
    $m->set( "hello" => "world" );
    $m->set( "hi" => "japan" );

    warn $m->get("helllo");
    warn $m->get("hi");

    my $href = $m->get_multi("mio:range-search", "he", "hi", "50");
    warn $href->{"hello"};
    warn $href->{"hi"};

## Algorithm
Mio using "Skip Graph" algorithm.
See "Load Balancing and Locality in Range-Queries Data Structures" by James Aspnes.


## Author
Copyright (C) Cybozu Labs, Inc.

Written by Taro Minowa(Higepon) <higepon@labs.cybozu.co.jp>

## License
New BSD License
