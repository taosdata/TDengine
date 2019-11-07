#!/bin/bash

# color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

# PROJECT HOME DIRECTORY
export HOME_DIR="$(dirname $(readlink -f $0))"

# TDENGINE PROJECT SETTING
export TDENGINE_HOME=${HOME_DIR}/TDengine

# TDENGINE PROJECT SETTING
export INFLUXDB_HOME=${HOME_DIR}/influxdb

# OPENTSDB PROJECT SETTING
export OPENTSDB_HOME=${HOME_DIR}/openTSDB

# MYSQL PROJECT SETTING
export MYSQL_HOME=${HOME_DIR}/mysql

# DATA SETTING
export DATA_HOME=${HOME_DIR}/data


CONFIG_FILE=
# 0 for insert, 1 for query
ACTION=0
TCONFIG_FILE="${HOME_DIR}/config/config.sh"

function show_help() {
    echo "  Usage: ./test.sh -f CONFIG_FILE [-a ACTION]"
    echo "      -F string"
    echo "         General configuration file"
    echo "      -f string"
    echo "         Database engine specific configuration file"
    echo "      -a int"
    echo "         Action to take. 0 for insert, 1 for query"
}

# ============= Main program starts from here ==============
TEST_DIR=
ACTION_DIR=

while getopts "h?F:f:a:" opt; do
    case "$opt" in
        h|\?)
            show_help
            exit 0
            ;;
        f) 
            CONFIG_FILE="$OPTARG"
            ;;
        F) 
            TCONFIG_FILE="$OPTARG"
            ;;
        a) 
            ACTION="$OPTARG"
            ;;
        *)
            ;;
    esac
done

. ${TCONFIG_FILE}

shift $((OPTIND -1))

if [ -z ${CONFIG_FILE} ] || [ ! -f ${CONFIG_FILE} ]; then
    echo "  Invalid configuration file"
    echo
    show_help
    exit 1
fi

. ${CONFIG_FILE}

case "$DATABASE" in
    "TDENGINE")
        echo "You are testing TDengine..."
        TEST_DIR=${TDENGINE_HOME}
        ;;
    "INFLUXDB")
        echo "You are testing Influxdb..."
        TEST_DIR=${INFLUXDB_HOME}
        ;;
    "OPENTSDB")
        echo "You are testing OpenTSDB..."
        TEST_DIR=${OPENTSDB_HOME}
        ;;
    *)
        echo "Invalid database to test"
        exit 1
        ;;
esac

case "$ACTION" in
    "0")
        ACTION_DIR=${TEST_DIR}/testInsert
        ;;
    "1")
        ACTION_DIR=${TEST_DIR}/testQuery
        ;;
    *)
        echo "Invalid action"
        exit 1
        ;;
esac

${ACTION_DIR}/test.sh
