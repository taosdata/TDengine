#!/bin/bash

##################################################
# 
# Do JDBC test 
#
##################################################

set -e


# Get responsible directories
if [ "${tools_dir}" == "" ]; then
   tools_dir=$(pwd);
fi

code_dir="$(readlink -m ${tools_dir}/..)"
test_dir="${code_dir}/test"
jdbc_test_dir="${test_dir}/JDBCTest"
BUILD_DIR=${tools_dir}/../../build

echo "------------------------------------------------------------------------"
echo "Start JDBC tesing ..."
echo "------------------------------------------------------------------------"
HOST_IP=$(hostname --all-ip-addresses | awk '{print $1}')
#cd ${jdbc_test_dir}
#${jdbc_test_dir}/testjdbc ${HOST_IP} jdbcdb root taosdata /etc/taos
export LD_LIBRARY_PATH=${BUILD_DIR}/lib:$LD_LIBRARY_PATH
echo -e "execute ${jdbc_test_dir}/testjdbc $1 jdbcdb root taosdata $2"
${jdbc_test_dir}/testjdbc $1 jdbcdb root taosdata $2
exitValue=""
while read line
do
    exitValue=$line
done < ${code_dir}/script/jdbc/error.count
CASE_FAILED_COUNT=${exitValue}
cd ${tools_dir}
echo "------------------------------------------------------------------------"
echo "JDBC Test Done, Results As Below:"
echo ""
echo "********************************************************"
if [ "${CASE_FAILED_COUNT}" == "0" ]; then
   echo -e "JDBC test results: \033[44;32;1m ${CASE_FAILED_COUNT} \033[0m test case(s) failed! \033[44;32;1m SUCCESS!!! \033[0m"
else
   echo -e "JDBC test results: \033[44;31;5m ${CASE_FAILED_COUNT} \033[0m test case(s)\033[44;31;1m FAILED!!! \033[0m"
   exit ${CASE_FAILED_COUNT}
fi
echo "********************************************************"
echo ""
echo "------------------------------------------------------------------------"

