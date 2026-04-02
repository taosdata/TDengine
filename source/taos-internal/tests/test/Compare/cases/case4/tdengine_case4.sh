#!/bin/bash

# DB information
export DATABASE='TDENGINE'

export CONFIG_DIR='/etc/taos/'
export HOST=
export USERNAME='root'
export PASSWORD='taosdata'

# ==================Insert configurable parameters==================
export INSERT_DB_NAME='db_1table_10b'
# export INSERT_DB_NAME='db'
export TB_PREFIX='t'
export DB_PROPERTY="keep 36500"
export RECORDS_PER_REQUEST=200
export INSERT_MODE=0
export IS_REAL_SITUATION=0

# ==================Query configurable parameters==================
export QUERY_DB_NAME='db'
export QUERY_FILE=${TDENGINE_HOME}/testQuery/query_cmd.txt
