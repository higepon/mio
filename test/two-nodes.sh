#!/bin/bash
rm -f mio.log.*
scripts/mio -d
scripts/mio -d -n mio2 -imio1@`hostname -s`.local -p 11811
sleep 1
scripts/mioctl -n mio1 stop 1>/dev/null
scripts/mioctl -n mio2 stop 1>/dev/null
grep 'Error' mio.log.1 1>/dev/null
case $? in
    0) echo "two-nodes failed"; exit 1;;
    *) echo "two-nodes passed"; exit 0;;
esac


