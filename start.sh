#!/bin/bash
erl -mio -noshell -noinput -pa ebin mio -s mio start -mio debug false &
