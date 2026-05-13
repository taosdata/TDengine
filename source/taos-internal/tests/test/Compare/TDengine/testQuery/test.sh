#!/bin/bash

echo "TEST INFORMATION"
echo "======================================"
echo "Databse              : ${DATABASE}"
echo "Action               : Query"
echo "Query time           : ${QUERY_TIME}"
echo "Query thread         : ${QUERY_THREAD}"
echo
echo "Config dir           : ${CONFIG_DIR}"
echo "Host                 : ${HOST}"
echo "User name            : ${USERNAME}"
echo "Password             : ${PASSWORD}"
echo "DB name              : ${QUERY_DB_NAME}"
echo "Query file           : ${QUERY_FILE}"
echo "======================================"

ROOT_DIR="$(dirname $(readlink -f $0))"
LANGUAGE=C

make -C ${ROOT_DIR}/${LANGUAGE} &> /dev/null

args="-c ${CONFIG_DIR} -p ${PASSWORD} -u ${USERNAME} -B ${QUERY_DB_NAME} -C ${QUERY_THREAD} -F ${QUERY_FILE} -Q ${QUERY_TIME}"

if [ ! -z ${HOST} ]; then
    args="$args -h ${HOST}"
fi

${ROOT_DIR}/${LANGUAGE}/queryTDengine ${args}
# -h ${HOST} -u ${USERNAME} -p ${PASSWORD} -B ${QUERY_DB_NAME} -Q ${QUERY_TIME} -C ${QUERY_THREAD} -F ${QUERY_FILE}
