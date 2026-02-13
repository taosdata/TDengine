#!/bin/bash

# active virtual environment
source /usr/local/taos/taosanode/venv/bin/activate

# start the flask service by using gunicorn
cd /usr/local/taos/taosanode/lib/taosanalytics/

/usr/local/taos/taosanode/venv/bin/gunicorn -c /usr/local/taos/taosanode/cfg/taosanode.config.py app:app
