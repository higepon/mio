#!/bin/sh
opt_cookie=
opt_name=
opt_port=
opt_introducer=
opt_verbose=false
opt_maxlevel=
while getopts 'n:p:i:vc:m:' OPTION
do
  case $OPTION in
  m)    opt_maxlevel="$OPTARG"
        ;;
  c)    opt_cookie="$OPTARG"
        ;;
  n)    opt_name="$OPTARG"
        ;;
  p)    opt_port="$OPTARG"
        ;;
  v)    opt_verbose=true
        ;;
  i)    opt_introducer="$OPTARG"
        ;;
  ?)    printf "Usage: %s: [-n node_name] [-p port] [-i introducer_name] [-c cookie] [-m maxlevel]\n" $(basename $0) >&2
        exit 2
        ;;
  esac
done
shift $(($OPTIND - 1))
mio_cookie=${opt_cookie:-"mio"}
mio_name=${opt_name:-"mio1"}
mio_name="${mio_name}@"`hostname`
mio_port=${opt_port:-"11211"}
mio_introducer=${opt_introducer:-""}
mio_maxlevel=${opt_maxlevel:-"3"}
mio_verbose=$opt_verbose
echo "Starting mio as name=$mio_name, port=$mio_port, introducer=$mio_introducer maxlevel=$mio_maxlevel verbose=$mio_verbose\n"
echo "To start other node, use \"bin/start.sh -i $mio_name -n <other_node_name> -m $mio_maxlevel -c $mio_cookie\n"

if [ -n "$mio_introducer" ]; then
    erl +K true -name $mio_name \
        -setcookie $mio_cookie \
        -mio \
        -noshell \
        -noinput \
        -pa ebin mio \
        -s mio_app start \
        -mio debug $mio_verbose port $mio_port boot_node $mio_introducer maxlevel $mio_maxlevel
else
    erl +K true -name $mio_name \
        -setcookie $mio_cookie \
        -mio \
        -noshell \
        -noinput \
        -pa ebin mio \
        -s mio_app start \
        -mio debug $mio_verbose port $mio_port maxlevel $mio_maxlevel
fi
