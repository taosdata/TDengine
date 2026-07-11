#!/bin/bash
set -e

_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_TEST_CI="$(cd "${_SCRIPT_DIR}/../.." && pwd)/ci"
if [ -f "${_TEST_CI}/setup_internal_mirrors.sh" ]; then
  # shellcheck source=../../ci/setup_internal_mirrors.sh
  source "${_TEST_CI}/setup_internal_mirrors.sh"
else
  _REPO_ROOT="$(cd "${_SCRIPT_DIR}/../../../../../" && pwd)"
  # shellcheck source=../../../../../tools/cicd/tsdb-test-pipeline/ci/setup_internal_mirrors.sh
  source "${_REPO_ROOT}/tools/cicd/tsdb-test-pipeline/ci/setup_internal_mirrors.sh"
fi
setup_internal_mirrors

pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &
cd ../../docs/examples/JDBC/JDBCDemo

mvn_status=0
mvn clean test > jdbc-out.log 2>&1 || mvn_status=$?
tail -n 20 jdbc-out.log

if [ "$mvn_status" -ne 0 ]; then
  exit "$mvn_status"
fi

totalJDBCCases=`grep 'Tests run' jdbc-out.log | awk -F"[:,]" 'END{ print $2 }'`
failed=`grep 'Tests run' jdbc-out.log | awk -F"[:,]" 'END{ print $4 }'`
error=`grep 'Tests run' jdbc-out.log | awk -F"[:,]" 'END{ print $6 }'`
totalJDBCFailed=$((failed + error))
totalJDBCSuccess=$((totalJDBCCases - totalJDBCFailed))

if [ "$totalJDBCSuccess" -gt "0" ]; then
  echo -e "\n${GREEN} ### Total $totalJDBCSuccess JDBC case(s) succeed! ### ${NC}"
fi

if [ "$totalJDBCFailed" -ne "0" ]; then
  echo -e "\n${RED} ### Total $totalJDBCFailed JDBC case(s) failed! ### ${NC}"
  exit 8
fi
