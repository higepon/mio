#!/bin/bash
erl -sname dummy -mio -noshell -noinput -pa ebin mio -s mio_app stop -s init stop 
pkill -f mioserver ## application is stopped above, but remains erl process.
