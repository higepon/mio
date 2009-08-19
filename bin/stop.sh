#!/bin/sh
opt_name=
opt_cookie=
while getopts 'n:c:' OPTION
do
  case $OPTION in
  c)    opt_cookie="$OPTARG"
        ;;
  n)    opt_name="$OPTARG"
        ;;
  ?)    printf "Usage: %s: [-n node_name_to_stop]\n" $(basename $0) >&2
        exit 2
        ;;
  esac
done
shift $(($OPTIND - 1))
mio_cookie=${opt_cookie:-"mio"}
mio_name=${opt_name:-"mio1"}
mio_name="${mio_name}@"`hostname`

echo "Try to stop $mio_name ...\n"

erl -name dummy \
    -mio \
    -noshell \
    -noinput \
    -pa ebin mio \
    -target_node $mio_name \
    -setcookie $mio_cookie \
    -s mio_app stop \
    -s init stop
