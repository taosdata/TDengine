#!/bin/bash

# DB information
export HOST="http://10.1.0.11:4242"

export DATABASE='OPENTSDB'

# ==================Insert configurable parameters==================
export POINTS_PER_REQUEST=40
export TAG_PREFIX='monitor'

# ==================Query configurable parameters==================
export QUERY_FILE=${OPENTSDB_HOME}/testQuery/query_cmd.txt
