#!/bin/sh
opt_name=
opt_port=
opt_introducer=
opt_verbose=false
while getopts 'n:p:i:v' OPTION
do
  case $OPTION in
  n)    opt_name="$OPTARG"
        ;;
  p)    opt_port="$OPTARG"
        ;;
  v)    opt_verbose=true
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
mio_port=${opt_port:-"11211"}
mio_introducer=${opt_introducer:-""}
mio_verbose=$opt_verbose
echo "Starting mio as name=$mio_name, port=$mio_port, introducer=$mio_introducer verbose=$mio_verbose\n"

if [ -n "$mio_introducer" ]; then
    erl -sname $mio_name \
        -mio \
        -noshell \
        -noinput \
        -pa ebin mio \
        -s mio_app start \
        -mio debug $mio_verbose port $mio_port boot_node $mio_introducer
else
    erl -sname $mio_name \
        -mio \
        -noshell \
        -noinput \
        -pa ebin mio \
        -s mio_app start \
        -mio debug $mio_verbose port $mio_port
fi
