#!/bin/sh
opt_name=
opt_port=
opt_introducer=

while getopts 'n:p:i:' OPTION
do
  case $OPTION in
  n)    opt_name="$OPTARG"
        ;;
  p)    opt_port="$OPTARG"
        ;;
  i)    opt_introducer="$OPTARG"
        ;;
  ?)    printf "Usage: %s: [-n node_name] [-p port] [-i introducer_name] \n" $(basename $0) >&2
        exit 2
        ;;
  esac
done
shift $(($OPTIND - 1))

mio_name=${opt_name:-"mio1@"`hostname -s`}
mio_port=${opt_port:-"11121"}
mio_introducer=$opt_introducer
echo "Starting mio as name=$mio_name, port=$mio_port, introducer=$mio_introducer"
