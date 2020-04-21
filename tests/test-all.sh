#!/bin/bash

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

cd script
./test.sh -f basicSuite.sim 2>&1 | grep 'success\|failed\|fault' | tee out.txt

totalSuccess=`grep success out.txt | wc -l`
totalBasic=`grep success out.txt | grep Suite | wc -l`

if [ "$totalSuccess" -gt "0" ]; then
  totalSuccess=`expr $totalSuccess - $totalBasic`
  echo -e "${GREEN} ### Total $totalSuccess TSIM case(s) succeed! ### ${NC}"
fi

totalFailed=`grep 'failed\|fault' out.txt | wc -l`
if [ "$totalFailed" -ne "0" ]; then
  echo -e "${RED} ### Total $totalFailed TSIM case(s) failed! ### ${NC}"
  exit $totalFailed
fi

cd ../pytest
./simpletest.sh 2>&1 | grep 'successfully executed\|failed' | tee pytest-out.txt
totalPySuccess=`grep 'successfully executed' pytest-out.txt | wc -l`

if [ "$totalPySuccess" -gt "0" ]; then
  echo -e "${GREEN} ### Total $totalPySuccess python case(s) succeed! ### ${NC}"
fi

totalPyFailed=`grep 'failed' pytest-out.txt | wc -l`
if [ "$totalPyFailed" -ne "0" ]; then
  echo -e "${RED} ### Total $totalPyFailed python case(s) failed! ### ${NC}"
  exit $totalPyFailed
fi

