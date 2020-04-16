#!/bin/bash

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

cd script
sudo ./test.sh 2>&1 | grep 'success\|failed' | tee out.txt

total_success=`grep success out.txt | wc -l`

if [ "$total_success" -gt "0" ]; then
  total_success=`expr $total_success - 1`
  echo -e "${GREEN} ### Total $total_success TSIM case(s) succeed! ### ${NC}"
fi

total_failed=`grep failed out.txt | wc -l`
if [ "$total_failed" -ne "0" ]; then
  echo -e "${RED} ### Total $total_failed TSIM case(s) failed! ### ${NC}"
  exit $total_failed
fi

cd ../pytest
sudo ./simpletest.sh 2>&1 | grep 'successfully executed\|failed' | tee pytest-out.txt
total_py_success=`grep 'successfully executed' pytest-out.txt | wc -l`

if [ "$total_py_success" -gt "0" ]; then
  echo -e "${GREEN} ### Total $total_py_success python case(s) succeed! ### ${NC}"
fi

total_py_failed=`grep 'failed' pytest-out.txt | wc -l`
if [ "$total_py_failed" -ne "0" ]; then
  echo -e "${RED} ### Total $total_py_failed python case(s) failed! ### ${NC}"
  exit $total_py_failed
fi

