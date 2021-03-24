#!/bin/bash

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

tests_dir=`pwd`
IN_TDINTERNAL="community"

function stopTaosd {
	echo "Stop taosd"
  sudo systemctl stop taosd
  PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
	while [ -n "$PID" ]
	do
    pkill -TERM -x taosd
    sleep 1
  	PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
	done
}

function dohavecore(){
  corefile=`find $corepath -mmin 1`  
  core_file=`echo $corefile|cut -d " " -f2`
  echo $core_file
  proc=`echo $corefile|cut -d "_" -f3`
  if [ -n "$corefile" ];then
    echo 'taosd or taos has generated core'
    tar -zcPf $corepath'taos_'`date "+%Y_%m_%d_%H_%M_%S"`.tar.gz /usr/local/taos/
    if [[ $1 == 1 ]];then
      echo '\n'|gdb /usr/local/taos/bin/$proc $core_file -ex "bt 10" -ex quit
      exit 8
    fi
  fi
}

function runSimCaseOneByOne {
  while read -r line; do
    if [[ $line =~ ^./test.sh* ]] || [[ $line =~ ^run* ]]; then
			case=`echo $line | grep sim$ |awk '{print $NF}'`    
      start_time=`date +%s`      
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

  start=`sed -n "/$1-start/=" jenkins/basic.txt`
  end=`sed -n "/$1-end/=" jenkins/basic.txt`  
  for ((i=$start;i<=$end;i++)) ; do
    line=`sed -n "$i"p jenkins/basic.txt`
    if [[ $line =~ ^./test.sh* ]] || [[ $line =~ ^run* ]]; then
			case=`echo $line | grep sim$ |awk '{print $NF}'`

      start_time=`date +%s`    
      date +%F\ %T | tee -a out.log
      if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]]; then
        echo -n $case
        ./test.sh -f $case > ../../../sim/case.log 2>&1 && \
        ( grep -q 'script.*'$case'.*failed.*, err.*lineNum' ../../../sim/tsim/log/taoslog0.0 && echo -e "${RED} failed${NC}" | tee -a out.log  ||  echo -e "${GREEN} success${NC}" | tee -a out.log )|| \
        ( grep -q 'script.*success.*m$' ../../../sim/tsim/log/taoslog0.0 && echo -e "${GREEN} success${NC}" | tee -a out.log )  || \
        ( echo -e "${RED} failed${NC}" | tee -a out.log && echo '=====================log=====================' && cat ../../../sim/case.log )
      else
        echo -n $case
        ./test.sh -f $case > ../../sim/case.log 2>&1 && \
        ( grep -q 'script.*'$case'.*failed.*, err.*lineNum' ../../sim/tsim/log/taoslog0.0 && echo -e "${RED} failed${NC}" | tee -a out.log  ||  echo -e "${GREEN} success${NC}" | tee -a out.log )|| \
        ( grep -q 'script.*success.*m$' ../../sim/tsim/log/taoslog0.0 && echo -e "${GREEN} success${NC}" | tee -a out.log )  || \
        ( echo -e "${RED} failed${NC}" | tee -a out.log && echo '=====================log=====================' &&  cat ../../sim/case.log )
      fi
      
      out_log=`tail -1 out.log  `
      if [[ $out_log =~ 'failed' ]];then
        if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]]; then
          cp -r ../../../sim ~/sim_`date "+%Y_%m_%d_%H:%M:%S"`
          rm -rf ../../../sim/case.log
        else 
          cp -r ../../sim ~/sim_`date "+%Y_%m_%d_%H:%M:%S" `
          rm -rf ../../sim/case.log
        fi
        dohavecore $2
        if [[ $2 == 1 ]];then
          exit 8
        fi
      fi
      end_time=`date +%s`
      echo execution time of $case was `expr $end_time - $start_time`s. | tee -a out.log
      dohavecore $2
    fi
  done 
  rm -rf ../../../sim/case.log
  rm -rf ../../sim/case.log
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
function runPyCaseOneByOnefq() {
  cd $tests_dir/pytest
  if [[ $1 =~ full ]] ; then
    start=1
    end=`sed -n '$=' fulltest.sh`
  else
    start=`sed -n "/$1-start/=" fulltest.sh`
    end=`sed -n "/$1-end/=" fulltest.sh`
  fi
  for ((i=$start;i<=$end;i++)) ; do
    line=`sed -n "$i"p fulltest.sh`
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
        $line > ../../sim/case.log 2>&1 && \
          echo -e "${GREEN} success${NC}" | tee -a pytest-out.log || \
          echo -e "${RED} failed${NC}" | tee -a pytest-out.log 
        end_time=`date +%s`
        out_log=`tail -1 pytest-out.log  `
        if [[ $out_log =~ 'failed' ]];then
          cp -r ../../sim ~/sim_`date "+%Y_%m_%d_%H:%M:%S" `
          echo '=====================log===================== '
          cat ../../sim/case.log
          rm -rf ../../sim/case.log
          dohavecore $2
          if [[ $2 == 1 ]];then
            exit 8
          fi
        fi
        echo execution time of $case was `expr $end_time - $start_time`s. | tee -a pytest-out.log
      else
        $line > /dev/null 2>&1
      fi
      dohavecore $2
    fi
  done 
  rm -rf ../../sim/case.log
}

totalFailed=0
totalPyFailed=0
totalJDBCFailed=0
totalUnitFailed=0

corepath=`grep -oP '.*(?=core_)' /proc/sys/kernel/core_pattern||grep -oP '.*(?=core-)' /proc/sys/kernel/core_pattern`
if [ "$2" != "jdbc" ] && [ "$2" != "python" ] && [ "$2" != "unit" ]; then
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
    runSimCaseOneByOnefq b1 0
    runSimCaseOneByOnefq b4 0
    runSimCaseOneByOnefq b5 0
    runSimCaseOneByOnefq b6 0
    runSimCaseOneByOnefq b7 0
  elif [ "$1" == "b2" ]; then
    echo "### run TSIM b2 test ###"
    runSimCaseOneByOnefq b2 0
  elif [ "$1" == "b3" ]; then
    echo "### run TSIM b3 test ###"
    runSimCaseOneByOnefq b3 0
  elif [ "$1" == "b1fq" ]; then
    echo "### run TSIM b1 test ###"
    runSimCaseOneByOnefq b1 1
  elif [ "$1" == "b2fq" ]; then
    echo "### run TSIM b2 test ###"
    runSimCaseOneByOnefq b2 1
  elif [ "$1" == "b3fq" ]; then
    echo "### run TSIM b3 test ###"
    runSimCaseOneByOnefq b3 1
  elif [ "$1" == "b4fq" ]; then
    echo "### run TSIM b4 test ###"
    runSimCaseOneByOnefq b4 1
  elif [ "$1" == "b5fq" ]; then
    echo "### run TSIM b5 test ###"
    runSimCaseOneByOnefq b5 1
  elif [ "$1" == "b6fq" ]; then
    echo "### run TSIM b6 test ###"
    runSimCaseOneByOnefq b6 1
  elif [ "$1" == "b7fq" ]; then
    echo "### run TSIM b7 test ###"
    runSimCaseOneByOnefq b7 1
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

if [ "$2" != "sim" ] && [ "$2" != "jdbc" ] && [ "$2" != "unit" ]; then
  echo "### run Python test case ###"

  cd $tests_dir

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
    runPyCaseOneByOnefq full 0
  elif [ "$1" == "p1" ]; then
    echo "### run Python_1 test ###"
    runPyCaseOneByOnefq p1 1
  elif [ "$1" == "p2" ]; then
    echo "### run Python_2 test ###"
    runPyCaseOneByOnefq p2 1
  elif [ "$1" == "p3" ]; then
    echo "### run Python_3 test ###"
    runPyCaseOneByOnefq p3 1 
  elif [ "$1" == "p4" ]; then
    echo "### run Python_4 test ###"
    runPyCaseOneByOnefq p4 1
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


if [ "$2" != "sim" ] && [ "$2" != "python" ] && [ "$2" != "unit" ] && [ "$1" == "full" ]; then
  echo "### run JDBC test cases ###"

  cd $tests_dir

  if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]]; then
    cd ../../
  else
    cd ../
  fi

  pwd
  cd debug/
  
  stopTaosd
  rm -rf /var/lib/taos/*
  nohup build/bin/taosd -c /etc/taos/ > /dev/null 2>&1 &
  sleep 30
  
  cd $tests_dir/../src/connector/jdbc  
  
  mvn test > jdbc-out.log 2>&1
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
  fi
  dohavecore 1
fi

if [ "$2" != "sim" ] && [ "$2" != "python" ] && [ "$2" != "jdbc" ] && [ "$1" == "full" ]; then
  echo "### run Unit tests ###"  

  stopTaosd
  cd $tests_dir

  if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]]; then
    cd ../../
  else
    cd ../
  fi

  pwd
  cd debug/build/bin
  rm -rf /var/lib/taos/*
  nohup ./taosd -c /etc/taos/ > /dev/null 2>&1 &
  sleep 30
  
  pwd
  ./queryTest > unittest-out.log 2>&1
  tail -n 20 unittest-out.log

  totalUnitTests=`grep "Running" unittest-out.log | awk '{print $3}'`  
  totalUnitSuccess=`grep 'PASSED' unittest-out.log | awk '{print $4}'`
  totalUnitFailed=`expr $totalUnitTests - $totalUnitSuccess`

  if [ "$totalUnitSuccess" -gt "0" ]; then
    echo -e "\n${GREEN} ### Total $totalUnitSuccess Unit test succeed! ### ${NC}"
  fi
  
  if [ "$totalUnitFailed" -ne "0" ]; then
    echo -e "\n${RED} ### Total $totalUnitFailed Unit test failed! ### ${NC}"
  fi
  dohavecore 1
fi


exit $(($totalFailed + $totalPyFailed + $totalJDBCFailed + $totalUnitFailed))
