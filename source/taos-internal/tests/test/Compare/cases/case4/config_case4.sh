#!/bin/bash

# ==================Insert configurable parameters==================
export SCHEMA_FILE=${DATA_HOME}/schema.txt
export SAMPLE_FILE=${DATA_HOME}/sample.txt
export NDETECTORS=1
export INSERT_THREAD=1
export RECORDS_PER_DETECTOR=10000000000
export START_TIME='01/01/2007 00:00:00'
# in milliseconds
export TIME_INTERVAL=50

# ==================Query configurable parameters==================
export QUERY_TIME=5
export QUERY_THREAD=1
