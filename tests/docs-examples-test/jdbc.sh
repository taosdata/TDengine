#!/bin/bash

pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &
cd ../../docs/examples/java

mvn clean test > jdbc-out.log 2>&1
tail -n 20 jdbc-out.log

cases=`grep 'Tests run' jdbc-out.log | awk 'END{print $3}'`
totalJDBCCases=`echo ${cases/%,}`
failed=`grep 'Tests run' jdbc-out.log | awk 'END{print $5}'`
JDBCFailed=`echo ${failed/%,}`
error=`grep 'Tests run' jdbc-out.log | awk 'END{print $7}'`
JDBCError=`echo ${error/%,}`

totalJDBCFailed=`expr $JDBCFailed + $JDBCError`
totalJDBCSuccess=`expr $totalJDBCCases - $totalJDBCFailed`

if [ "$totalJDBCSuccess" -gt "0" ]; then
  echo -e "\n${GREEN} ### Total $totalJDBCSuccess JDBC case(s) succeed! ### ${NC}"
fi

if [ "$totalJDBCFailed" -ne "0" ]; then
  echo -e "\n${RED} ### Total $totalJDBCFailed JDBC case(s) failed! ### ${NC}"
  exit 8
fi