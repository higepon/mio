#!/bin/bash
# Bench mark results
# http://spreadsheets.google.com/ccc?key=0AmmP2yjXUnM5dDZHU0trSjBpNkhYc2xVZHEyUWJwb1E&hl=ja

function bench_memcached {
    echo "*** Memcached ***"
    bench "memcached -p 11211" "pkill -f memcached" $1 $2
}

function bench_mio {
    echo "*** Mio ***"
    bench "bin/start.sh" "bin/stop.sh" $1 $2
}

function bench {
    echo "\n****    thread=$3 N=$4\n"
    $1 -m 8 &
    sleep 1
    mcb -c set -a 127.0.0.1 -p 11211 -t $3 -n $4 -l 100
    $2
}



# bench_memcached 1 1000
bench_mio 1 10000
# bench_memcached 1 100

#bench_memcached 1 10000

# bench_memcached 1 10000

# bench_mio 1 10000

#bench_memcached 10 1000
#bench_mio 10 1000

#bench_memcached 10 1000

#bench_memcached 10 1000

#bench_mio 10 1000

#bench_memcached 10 10000
# bench_mio 10 10000

