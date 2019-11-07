#!/bin/bash

# ==================Insert configurable parameters==================
export SCHEMA_FILE=${DATA_HOME}/schema.txt
export SAMPLE_FILE=${DATA_HOME}/sample.txt
export NDETECTORS=100
export INSERT_THREAD=5
export RECORDS_PER_DETECTOR=1000000
export START_TIME='01/01/2017 00:00:00'
# in milliseconds
export TIME_INTERVAL=1000

# ==================Query configurable parameters==================
export QUERY_TIME=5
export QUERY_THREAD=1
