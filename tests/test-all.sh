#!/bin/bash

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

function runSimCaseOneByOne {
  while read -r line; do
    if [[ $line =~ ^./test.sh* ]] || [[ $line =~ ^run* ]]; then
			case=`echo $line | grep sim$ |awk '{print $NF}'`

      start_time=`date +%s`
      ./test.sh -f $case > /dev/null 2>&1 && \
        echo -e "${GREEN}$case success${NC}" | tee -a out.log || \
        echo -e "${RED}$case failed${NC}" | tee -a out.log
      out_log=`tail -1 out.log  `
      # if [[ $out_log =~ 'failed' ]];then
      #   exit 8
      # fi
      end_time=`date +%s`
      echo execution time of $case was `expr $end_time - $start_time`s. | tee -a out.log
    fi
  done < $1
}

function runPyCaseOneByOne {
  while read -r line; do
    if [[ $line =~ ^python.* ]]; then
      if [[ $line != *sleep* ]]; then
        
        if [[ $line =~ '-r' ]];then
          case=`echo $line|awk '{print $4}'`
        else
          case=`echo $line|awk '{print $NF}'`
        fi
        start_time=`date +%s`
        $line > /dev/null 2>&1 && \
          echo -e "${GREEN}$case success${NC}" | tee -a pytest-out.log || \
          echo -e "${RED}$case failed${NC}" | tee -a pytest-out.log
        end_time=`date +%s`
        out_log=`tail -1 pytest-out.log  `
        # if [[ $out_log =~ 'failed' ]];then
        #   exit 8
        # fi
        echo execution time of $case was `expr $end_time - $start_time`s. | tee -a pytest-out.log
      else
        $line > /dev/null 2>&1
      fi
    fi
  done < $1
}

totalFailed=0
totalPyFailed=0

tests_dir=`pwd`

if [ "$2" != "python" ]; then
  echo "### run TSIM test case ###"
  cd $tests_dir/script

  [ -f out.log ] && rm -f out.log
  if [ "$1" == "cron" ]; then
    echo "### run TSIM regression test ###"
    runSimCaseOneByOne regressionSuite.sim
  elif [ "$1" == "full" ]; then
    echo "### run TSIM full test ###"
    runSimCaseOneByOne jenkins/basic.txt
  elif [ "$1" == "b1" ]; then
    echo "### run TSIM b1 test ###"
    runSimCaseOneByOne jenkins/basic_1.txt
  elif [ "$1" == "b2" ]; then
    echo "### run TSIM b2 test ###"
    runSimCaseOneByOne jenkins/basic_2.txt
  elif [ "$1" == "b3" ]; then
    echo "### run TSIM b3 test ###"
    runSimCaseOneByOne jenkins/basic_3.txt
  elif [ "$1" == "smoke" ] || [ -z "$1" ]; then
    echo "### run TSIM smoke test ###"
    runSimCaseOneByOne basicSuite.sim
  fi

  totalSuccess=`grep 'success' out.log | wc -l`
  totalBasic=`grep success out.log | grep Suite | wc -l`

  if [ "$totalSuccess" -gt "0" ]; then
    totalSuccess=`expr $totalSuccess - $totalBasic`
  fi

  echo -e "\n${GREEN} ### Total $totalSuccess TSIM case(s) succeed! ### ${NC}"

  totalFailed=`grep 'failed\|fault' out.log | wc -l`
# echo -e "${RED} ### Total $totalFailed TSIM case(s) failed! ### ${NC}"

  if [ "$totalFailed" -ne "0" ]; then
    echo -e "\n${RED} ### Total $totalFailed TSIM case(s) failed! ### ${NC}"

#  exit $totalFailed
  fi
fi

if [ "$2" != "sim" ]; then
  echo "### run Python test case ###"

  cd $tests_dir
  IN_TDINTERNAL="community"

  if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]]; then
    cd ../..
  else
    cd ../
  fi

  TOP_DIR=`pwd`
  TAOSLIB_DIR=`find . -name "libtaos.so"|grep -w lib|head -n1`
  if [[ "$TAOSLIB_DIR" == *"$IN_TDINTERNAL"* ]]; then
    LIB_DIR=`find . -name "libtaos.so"|grep -w lib|head -n1|cut -d '/' --fields=2,3,4,5`
  else
    LIB_DIR=`find . -name "libtaos.so"|grep -w lib|head -n1|cut -d '/' --fields=2,3,4`
  fi

  export LD_LIBRARY_PATH=$TOP_DIR/$LIB_DIR:$LD_LIBRARY_PATH

  cd $tests_dir/pytest

  [ -f pytest-out.log ] && rm -f pytest-out.log

  if [ "$1" == "cron" ]; then
    echo "### run Python regression test ###"
    runPyCaseOneByOne regressiontest.sh
  elif [ "$1" == "full" ]; then
    echo "### run Python full test ###"
    runPyCaseOneByOne fulltest.sh
  elif [ "$1" == "pytest" ]; then
    echo "### run Python full test ###"
    runPyCaseOneByOne fulltest.sh
  elif [ "$1" == "b2" ] || [ "$1" == "b3" ]; then
    exit $(($totalFailed + $totalPyFailed))
  elif [ "$1" == "smoke" ] || [ -z "$1" ]; then
    echo "### run Python smoke test ###"
    runPyCaseOneByOne smoketest.sh
  fi
  totalPySuccess=`grep 'success' pytest-out.log | wc -l`

  if [ "$totalPySuccess" -gt "0" ]; then
    echo -e "\n${GREEN} ### Total $totalPySuccess python case(s) succeed! ### ${NC}"
  fi

  totalPyFailed=`grep 'failed\|fault' pytest-out.log | wc -l`
  if [ "$totalPyFailed" -ne "0" ]; then
    echo -e "\n${RED} ### Total $totalPyFailed python case(s) failed! ### ${NC}"
#  exit $totalPyFailed
  fi
fi

exit $(($totalFailed + $totalPyFailed))
