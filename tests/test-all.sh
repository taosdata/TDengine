#!/bin/bash

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

function dohavecore(){
  corefile=`find $corepath -mmin 1`  
  if [ -n "$corefile" ];then
  echo 'taosd or taos has generated core'
  if [[ $1 == 1 ]];then
    exit 8
  fi
  fi
}
function runSimCaseOneByOne {
  while read -r line; do
    if [[ $line =~ ^./test.sh* ]] || [[ $line =~ ^run* ]]; then
			case=`echo $line | grep sim$ |awk '{print $NF}'`
      IN_TDINTERNAL="community"
      start_time=`date +%s`
      IN_TDINTERNAL="community"
      date +%F\ %T | tee -a out.log
      if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]]; then
        echo -n $case
        ./test.sh -f $case > /dev/null 2>&1 && \
        ( grep -q 'script.*'$case'.*failed.*, err.*lineNum' ../../../sim/tsim/log/taoslog0.0 && echo -e "${RED} failed${NC}" | tee -a out.log  ||  echo -e "${GREEN} success${NC}" | tee -a out.log )|| \
        ( grep -q 'script.*success.*m$' ../../../sim/tsim/log/taoslog0.0 && echo -e "${GREEN} success${NC}" | tee -a out.log )  || \
        echo -e "${RED} failed${NC}" | tee -a out.log
      else
        echo -n $case
        ./test.sh -f $case > /dev/null 2>&1 && \
        ( grep -q 'script.*'$case'.*failed.*, err.*lineNum' ../../sim/tsim/log/taoslog0.0 && echo -e "${RED} failed${NC}" | tee -a out.log  ||  echo -e "${GREEN} success${NC}" | tee -a out.log )|| \
        ( grep -q 'script.*success.*m$' ../../sim/tsim/log/taoslog0.0 && echo -e "${GREEN} success${NC}" | tee -a out.log )  || \
        echo -e "${RED} failed${NC}" | tee -a out.log
      fi
      out_log=`tail -1 out.log  `
      # if [[ $out_log =~ 'failed' ]];then
      #   exit 8
      # fi
      end_time=`date +%s`
      echo execution time of $case was `expr $end_time - $start_time`s. | tee -a out.log
      dohavecore 0
    fi
  done < $1
}
function runSimCaseOneByOnefq {
  while read -r line; do
    if [[ $line =~ ^./test.sh* ]] || [[ $line =~ ^run* ]]; then
			case=`echo $line | grep sim$ |awk '{print $NF}'`

      start_time=`date +%s`
      IN_TDINTERNAL="community"
      date +%F\ %T | tee -a out.log
      if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]]; then
        echo -n $case
        ./test.sh -f $case > /dev/null 2>&1 && \
        ( grep -q 'script.*'$case'.*failed.*, err.*lineNum' ../../../sim/tsim/log/taoslog0.0 && echo -e "${RED} failed${NC}" | tee -a out.log  ||  echo -e "${GREEN} success${NC}" | tee -a out.log )|| \
        ( grep -q 'script.*success.*m$' ../../../sim/tsim/log/taoslog0.0 && echo -e "${GREEN} success${NC}" | tee -a out.log )  || \
        echo -e "${RED} failed${NC}" | tee -a out.log
      else
        echo -n $case
        ./test.sh -f $case > /dev/null 2>&1 && \
        ( grep -q 'script.*'$case'.*failed.*, err.*lineNum' ../../sim/tsim/log/taoslog0.0 && echo -e "${RED} failed${NC}" | tee -a out.log  ||  echo -e "${GREEN} success${NC}" | tee -a out.log )|| \
        ( grep -q 'script.*success.*m$' ../../sim/tsim/log/taoslog0.0 && echo -e "${GREEN} success${NC}" | tee -a out.log )  || \
        echo -e "${RED} failed${NC}" | tee -a out.log
      fi
      
      out_log=`tail -1 out.log  `
      if [[ $out_log =~ 'failed' ]];then
        if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]]; then
          cp -r ../../../sim ~/sim_`date "+%Y_%m_%d_%H:%M:%S"`
        else 
          cp -r ../../sim ~/sim_`date "+%Y_%m_%d_%H:%M:%S" `
        fi
        exit 8
      fi
      end_time=`date +%s`
      echo execution time of $case was `expr $end_time - $start_time`s. | tee -a out.log
      dohavecore 1
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
        date +%F\ %T | tee -a pytest-out.log
        echo -n $case
        $line > /dev/null 2>&1 && \
          echo -e "${GREEN} success${NC}" | tee -a pytest-out.log || \
          echo -e "${RED} failed${NC}" | tee -a pytest-out.log
        end_time=`date +%s`
        out_log=`tail -1 pytest-out.log  `
        # if [[ $out_log =~ 'failed' ]];then
        #   exit 8
        # fi
        echo execution time of $case was `expr $end_time - $start_time`s. | tee -a pytest-out.log
      else
        $line > /dev/null 2>&1
      fi
      dohavecore 0
    fi
  done < $1
}
function runPyCaseOneByOnefq {
  while read -r line; do
    if [[ $line =~ ^python.* ]]; then
      if [[ $line != *sleep* ]]; then
        
        if [[ $line =~ '-r' ]];then
          case=`echo $line|awk '{print $4}'`
        else
          case=`echo $line|awk '{print $NF}'`
        fi
        start_time=`date +%s`
        date +%F\ %T | tee -a pytest-out.log
        echo -n $case
        $line > /dev/null 2>&1 && \
          echo -e "${GREEN} success${NC}" | tee -a pytest-out.log || \
          echo -e "${RED} failed${NC}" | tee -a pytest-out.log
        end_time=`date +%s`
        out_log=`tail -1 pytest-out.log  `
        if [[ $out_log =~ 'failed' ]];then
          cp -r ../../sim ~/sim_`date "+%Y_%m_%d_%H:%M:%S" `
          exit 8
        fi
        echo execution time of $case was `expr $end_time - $start_time`s. | tee -a pytest-out.log
      else
        $line > /dev/null 2>&1
      fi
      dohavecore 1
    fi
  done < $1
}
totalFailed=0
totalPyFailed=0

tests_dir=`pwd`
corepath=`grep -oP '.*(?=core_)' /proc/sys/kernel/core_pattern||grep -oP '.*(?=core-)' /proc/sys/kernel/core_pattern`
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
    runSimCaseOneByOne jenkins/basic_4.txt
    runSimCaseOneByOne jenkins/basic_5.txt
    runSimCaseOneByOne jenkins/basic_6.txt
    runSimCaseOneByOne jenkins/basic_7.txt
  elif [ "$1" == "b2" ]; then
    echo "### run TSIM b2 test ###"
    runSimCaseOneByOne jenkins/basic_2.txt
  elif [ "$1" == "b3" ]; then
    echo "### run TSIM b3 test ###"
    runSimCaseOneByOne jenkins/basic_3.txt
  elif [ "$1" == "b1fq" ]; then
    echo "### run TSIM b1 test ###"
    runSimCaseOneByOnefq jenkins/basic_1.txt
  elif [ "$1" == "b2fq" ]; then
    echo "### run TSIM b2 test ###"
    runSimCaseOneByOnefq jenkins/basic_2.txt
  elif [ "$1" == "b3fq" ]; then
    echo "### run TSIM b3 test ###"
    runSimCaseOneByOnefq jenkins/basic_3.txt
  elif [ "$1" == "b4fq" ]; then
    echo "### run TSIM b4 test ###"
    runSimCaseOneByOnefq jenkins/basic_4.txt
  elif [ "$1" == "b5fq" ]; then
    echo "### run TSIM b5 test ###"
    runSimCaseOneByOnefq jenkins/basic_5.txt
  elif [ "$1" == "b6fq" ]; then
    echo "### run TSIM b6 test ###"
    runSimCaseOneByOnefq jenkins/basic_6.txt
  elif [ "$1" == "b7fq" ]; then
    echo "### run TSIM b7 test ###"
    runSimCaseOneByOnefq jenkins/basic_7.txt
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
  elif [ "$1" == "pytestfq" ]; then
    echo "### run Python full test ###"
    runPyCaseOneByOnefq fulltest.sh
  elif [ "$1" == "p1" ]; then
    echo "### run Python_1 test ###"
    runPyCaseOneByOnefq pytest_1.sh
  elif [ "$1" == "p2" ]; then
    echo "### run Python_2 test ###"
    runPyCaseOneByOnefq pytest_2.sh
  elif [ "$1" == "p3" ]; then
    echo "### run Python_3 test ###"
    runPyCaseOneByOnefq pytest_3.sh
  elif [ "$1" == "p4" ]; then
    echo "### run Python_4 test ###"
    runPyCaseOneByOnefq pytest_4.sh
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
