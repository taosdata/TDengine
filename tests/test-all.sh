#!/bin/bash

function runSimCaseOneByOne {
  while read -r line; do
    if [[ $line =~ ^run.* ]]; then
      case=`echo $line | awk '{print $2}'`
      ./test.sh -f $case 2>&1 | grep 'success\|failed\|fault' | grep -v 'default' | tee -a out.log
    fi
  done < $1
}

function runPyCaseOneByOne {
  while read -r line; do
    if [[ $line =~ ^python.* ]]; then
      $line 2>&1 | grep 'successfully executed\|failed\|fault' | grep -v 'default'| tee -a pytest-out.log
    fi
  done < $1
}

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

echo "### run TSIM script ###"
cd script

[ -f out.log ] && rm -f out.log

if [ "$1" == "cron" ]; then
  runSimCaseOneByOne fullGeneralSuite.sim
else
  runSimCaseOneByOne basicSuite.sim
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

[ -f pytest-out.log ] && rm -f pytest-out.log

if [ "$1" == "cron" ]; then
  runPyCaseOneByOne fulltest.sh
else
  runPyCaseOneByOne smoketest.sh
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
