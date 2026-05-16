#!/bin/bash

echo "TEST INFORMATION"
echo "======================================"
echo "Databse              : ${DATABASE}"
echo "Action               : Query"
echo "Query time           : ${QUERY_TIME}"
echo "Query thread         : ${QUERY_THREAD}"
echo
echo "Host                 : ${HOST}"
echo "User name            : ${USERNAME}"
echo "Password             : ${PASSWORD}"
echo "DB name              : ${QUERY_DB_NAME}"
echo "Query file           : ${QUERY_FILE}"
echo "======================================"

ROOT_DIR="$(dirname $(readlink -f $0))"
LANGUAGE=GO

make -C ${ROOT_DIR}/${LANGUAGE} &> /dev/null

args="-connections ${QUERY_THREAD} -db ${QUERY_DB_NAME} -host ${HOST} -query_time ${QUERY_TIME} -command_file ${QUERY_FILE}"

if [ ! -z ${USERNAME} ]; then
    args="$args -user ${USERNAME}"
fi

if [ ! -z ${PASSWORD} ]; then
    args="$args -pass ${PASSWORD}"
fi

${ROOT_DIR}/${LANGUAGE}/queryInfluxdb ${args}
