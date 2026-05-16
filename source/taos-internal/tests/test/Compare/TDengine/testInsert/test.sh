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
echo "Config dir           : ${CONFIG_DIR}"
echo "Host                 : ${HOST}"
echo "User name            : ${USERNAME}"
echo "Password             : ${PASSWORD}"
echo "DB name              : ${INSERT_DB_NAME}"
echo "Table prefix         : ${TB_PREFIX}"
echo "STable               : ${STABLE}"
echo "DB property          : ${DB_PROPERTY}"
echo "Records per request  : ${RECORDS_PER_REQUEST}"
echo "Insert mode          : ${INSERT_MODE}"
echo "Real situation       : ${IS_REAL_SITUATION}"
echo "======================================"

ROOT_DIR="$(dirname $(readlink -f $0))"
LANGUAGE=C

make -C ${ROOT_DIR}/${LANGUAGE} &> /dev/null

args=" -c ${CONFIG_DIR} -p ${PASSWORD} -u ${USERNAME} -b ${STABLE} -B ${INSERT_DB_NAME} -m ${SAMPLE_FILE} -s ${SCHEMA_FILE} -t ${TB_PREFIX} -C ${INSERT_THREAD} -e ${RECORDS_PER_REQUEST} -i ${RECORDS_PER_DETECTOR} -n ${NDETECTORS} -M ${INSERT_MODE}  -S $(date -d "${START_TIME}" +%s)000 -v ${TIME_INTERVAL}"

# if [ ! -z "${DB_PROPERTY}" ]; then
#     args="$args -T '${DB_PROPERTY}'"
# fi

if [ ! -z ${HOST} ]; then
    args="$args -h ${HOST}"
fi

if [ ${IS_REAL_SITUATION} = "1" ]; then
    args="$args -r"
fi

echo "Starting to test..."
if [ ! -z "${DB_PROPERTY}" ]; then
    ${ROOT_DIR}/${LANGUAGE}/insertTDengine ${args} -T "${DB_PROPERTY}"
else
    ${ROOT_DIR}/${LANGUAGE}/insertTDengine ${args}
fi
echo "Test done!"
