#!/bin/bash

# DB information
export DATABASE='INFLUXDB'


export HOST="http://localhost:8086"
export USERNAME=
export PASSWORD=

# ==================Insert configurable parameters==================
export INSERT_DB_NAME='db'
export TAG_PREFIX='card'
export POINTS_PER_BATCH=10000

# ==================Query configurable parameters==================
export QUERY_DB_NAME='db'
export QUERY_FILE=${INFLUXDB_HOME}/testQuery/query_cmd.txt
