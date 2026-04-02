#!/bin/bash

# stop the Gunicorn server
kill -s TERM $(cat /usr/local/taos/taosanode/taosanode.pid)