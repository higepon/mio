#!/bin/bash

scripts/mio -d
scripts/mio -d -n mio2 -imio1@`hostname -s`.local -p 11811
sleep 1
(scripts/mioctl -n mio1 stop 1>/dev/null) && (scripts/mioctl -n mio2 stop 1>/dev/null) && echo "two-node test passed"
