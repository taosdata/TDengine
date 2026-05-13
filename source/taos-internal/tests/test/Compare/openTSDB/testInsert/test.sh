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
echo "Tag prefix           : ${TAG_PREFIX}"
echo "Points per request   : ${POINTS_PER_REQUEST}"
echo "======================================"

ROOT_DIR="$(dirname $(readlink -f $0))"
LANGUAGE=C

make -C ${ROOT_DIR}/${LANGUAGE} &> /dev/null

${ROOT_DIR}/${LANGUAGE}/insertOpentsdb -h ${HOST} -m ${SAMPLE_FILE} -s ${SCHEMA_FILE} -C ${INSERT_THREAD} -e ${POINTS_PER_REQUEST} -i ${RECORDS_PER_DETECTOR} -n ${NDETECTORS} -t ${TAG_PREFIX} -S $(date -d "${START_TIME}" +%s)000 -v ${TIME_INTERVAL}
