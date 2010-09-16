#!/bin/bash
rm -f mio.log.*
scripts/mio -d -n mio3@127.0.0.1
scripts/mio -d -n mio4@127.0.0.1 -imio1@127.0.0.1 -p 11811
sleep 1
scripts/mioctl -n mio3@127.0.0.1 stop 1>/dev/null
scripts/mioctl -n mio4@127.0.0.1 stop 1>/dev/null
grep 'Error' mio.log.1 1>/dev/null
case $? in
    0) echo "two-nodes failed"; exit 1;;
    *) echo "two-nodes passed"; exit 0;;
esac


