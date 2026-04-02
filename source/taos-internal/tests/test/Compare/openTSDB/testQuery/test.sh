#!/bin/bash

echo "TEST INFORMATION"
echo "======================================"
echo "Databse              : ${DATABASE}"
echo "Action               : Query"
echo "Query time           : ${QUERY_TIME}"
echo "Query thread         : ${QUERY_THREAD}"
echo
echo "Host                 : ${HOST}"
echo "Query file           : ${QUERY_FILE}"
echo "======================================"

ROOT_DIR="$(dirname $(readlink -f $0))"
LANGUAGE=C

make -C ${ROOT_DIR}/${LANGUAGE} &> /dev/null

${ROOT_DIR}/${LANGUAGE}/queryOpentsdb -h ${HOST} -Q ${QUERY_TIME} -C ${QUERY_THREAD} -F ${QUERY_FILE}
