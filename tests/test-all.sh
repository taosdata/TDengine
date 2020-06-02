#!/bin/bash

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

echo "### run TSIM script ###"
cd script
if [ "$1" == "cron" ]; then
  ./test.sh -f fullGeneralSuite.sim 2>&1 | grep 'success\|failed\|fault' | grep -v 'default' | tee out.log
else
  ./test.sh -f basicSuite.sim 2>&1 | grep 'success\|failed\|fault' | grep -v 'default' | tee out.log
fi

totalSuccess=`grep 'success' out.log | wc -l`
totalBasic=`grep success out.log | grep Suite | wc -l`

if [ "$totalSuccess" -gt "0" ]; then
  totalSuccess=`expr $totalSuccess - $totalBasic`
fi

echo -e "${GREEN} ### Total $totalSuccess TSIM case(s) succeed! ### ${NC}"

totalFailed=`grep 'failed\|fault' out.log | wc -l`
# echo -e "${RED} ### Total $totalFailed TSIM case(s) failed! ### ${NC}"

if [ "$totalFailed" -ne "0" ]; then
  echo -e "${RED} ### Total $totalFailed TSIM case(s) failed! ### ${NC}"

#  exit $totalFailed
fi

echo "### run Python script ###"
cd ../pytest

if [ "$1" == "cron" ]; then
  ./fulltest.sh 2>&1 | grep 'successfully executed\|failed\|fault' | grep -v 'default'| tee pytest-out.log
else
  ./smoketest.sh 2>&1 | grep 'successfully executed\|failed\|fault' | grep -v 'default'| tee pytest-out.log
fi
totalPySuccess=`grep 'successfully executed' pytest-out.log | wc -l`

if [ "$totalPySuccess" -gt "0" ]; then
  echo -e "${GREEN} ### Total $totalPySuccess python case(s) succeed! ### ${NC}"
fi

totalPyFailed=`grep 'failed\|fault' pytest-out.log | wc -l`
if [ "$totalPyFailed" -ne "0" ]; then
  echo -e "${RED} ### Total $totalPyFailed python case(s) failed! ### ${NC}"
#  exit $totalPyFailed
fi

exit $(($totalFailed + $totalPyFailed))
