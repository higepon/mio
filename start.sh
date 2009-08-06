#!/bin/bash
erl -sname mioserver@localhost -mio -noshell -noinput -pa ebin mio -s mio_app start -mio debug $1 port 11211 &
