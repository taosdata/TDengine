#!/bin/bash

echo "TEST INFORMATION"
echo "======================================"
echo "Databse              : ${DATABASE}"
echo "Action               : Insert"
echo "Schema file          : ${SCHEMA_FILE}"
echo "Sample file          : ${SAMPLE_FILE}"
echo "Insert thread        : ${INSERT_THREAD}"
echo "Detectors            : ${NDETECTORS}"
echo "Records per detector : ${RECORDS_PER_DETECTOR}"
echo "Start time           : ${START_TIME}"
echo "Time interval        : ${TIME_INTERVAL}"
echo
echo "Host                 : ${HOST}"
echo "User name            : ${USERNAME}"
echo "Password             : ${PASSWORD}"
echo "DB name              : ${INSERT_DB_NAME}"
echo "Points per batch     : ${POINTS_PER_BATCH}"
echo "======================================"

ROOT_DIR="$(dirname $(readlink -f $0))"
LANGUAGE=GO

make -C ${ROOT_DIR}/${LANGUAGE} &> /dev/null

args="-batch ${POINTS_PER_BATCH} -connections ${INSERT_THREAD} -db ${INSERT_DB_NAME} -detectors ${NDETECTORS} -host ${HOST} -interval ${TIME_INTERVAL} -points ${RECORDS_PER_DETECTOR} -sample ${SAMPLE_FILE} -schema ${SCHEMA_FILE} -start_time $(date -d "${START_TIME}" +%s)000 -tag_prefix ${TAG_PREFIX}"

if [ ! -z ${USERNAME} ]; then
    args="$args -user ${USERNAME}"
fi

if [ ! -z ${PASSWORD} ]; then
    args="$args -pass ${PASSWORD}"
fi

${ROOT_DIR}/${LANGUAGE}/insertInfluxdb ${args}
