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
  sudo systemctl stop taosd || echo 'no sudo or systemctl or stop fail'
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
  if [ -n "$corefile" ];then
    core_file=`echo $corefile|cut -d " " -f2`
    proc=`file $core_file|awk -F "execfn:"  '/execfn:/{print $2}'|tr -d \' |awk '{print $1}'|tr -d \,`
    echo 'taosd or taos has generated core'
    rm case.log
    if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]] && [[ $1 == 1 ]]; then
      cd ../../../
      tar -zcPf $corepath'taos_'`date "+%Y_%m_%d_%H_%M_%S"`.tar.gz debug/build/bin/taosd debug/build/bin/tsim debug/build/lib/libtaos*so*
      if [[ $2 == 1 ]];then
        cp -r sim ~/sim_`date "+%Y_%m_%d_%H:%M:%S"`
      else
        cd community
        cp -r sim ~/sim_`date "+%Y_%m_%d_%H:%M:%S" `
      fi
    else 
      cd ../../
      if [[ $1 == 1 ]];then 
        tar -zcPf $corepath'taos_'`date "+%Y_%m_%d_%H_%M_%S"`.tar.gz debug/build/bin/taosd debug/build/bin/tsim debug/build/lib/libtaos*so*
        cp -r sim ~/sim_`date "+%Y_%m_%d_%H:%M:%S" `
      fi
    fi
    if [[ $1 == 1 ]];then
      echo '\n'|gdb $proc $core_file -ex "bt 10" -ex quit
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
        ./test.sh -f $case > case.log 2>&1 && \
        ( grep -q 'script.*'$case'.*failed.*, err.*lineNum' ../../../sim/tsim/log/taoslog0.0 && echo -e "${RED} failed${NC}" | tee -a out.log  ||  echo -e "${GREEN} success${NC}" | tee -a out.log )|| \
        ( grep -q 'script.*success.*m$' ../../../sim/tsim/log/taoslog0.0 && echo -e "${GREEN} success${NC}" | tee -a out.log )  || \
        ( echo -e "${RED} failed${NC}" | tee -a out.log && echo '=====================log=====================' && cat case.log )
      else
        echo -n $case
        ./test.sh -f $case > ../../sim/case.log 2>&1 && \
        ( grep -q 'script.*'$case'.*failed.*, err.*lineNum' ../../sim/tsim/log/taoslog0.0 && echo -e "${RED} failed${NC}" | tee -a out.log  ||  echo -e "${GREEN} success${NC}" | tee -a out.log )|| \
        ( grep -q 'script.*success.*m$' ../../sim/tsim/log/taoslog0.0 && echo -e "${GREEN} success${NC}" | tee -a out.log )  || \
        ( echo -e "${RED} failed${NC}" | tee -a out.log && echo '=====================log=====================' &&  cat case.log )
      fi
      
      out_log=`tail -1 out.log  `
      if [[ $out_log =~ 'failed' ]];then
        rm case.log
        if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]]; then
          cp -r ../../../sim ~/sim_`date "+%Y_%m_%d_%H:%M:%S"`
        else 
          cp -r ../../sim ~/sim_`date "+%Y_%m_%d_%H:%M:%S" `
        fi
        dohavecore $2 1
        if [[ $2 == 1 ]];then
          exit 8
        fi
      fi
      end_time=`date +%s`
      echo execution time of $case was `expr $end_time - $start_time`s. | tee -a out.log
      dohavecore $2 1
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
        date +%F\ %T | tee -a $tests_dir/pytest-out.log
        echo -n $case
        $line > /dev/null 2>&1 && \
          echo -e "${GREEN} success${NC}" | tee -a $tests_dir/pytest-out.log || \
          echo -e "${RED} failed${NC}" | tee -a $tests_dir/pytest-out.log
        end_time=`date +%s`
        out_log=`tail -1 pytest-out.log  `
        # if [[ $out_log =~ 'failed' ]];then
        #   exit 8
        # fi
        echo execution time of $case was `expr $end_time - $start_time`s. | tee -a $tests_dir/pytest-out.log
      else
        $line > /dev/null 2>&1
      fi
    fi
  done < $1
}

function runPyCaseOneByOnefq() {
  if [[ $3 =~ system ]] ; then
    cd $tests_dir/system-test
  elif [[ $3 =~ develop ]] ; then
    cd $tests_dir/develop-test
  else
    cd $tests_dir/pytest
  fi
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
        if [[ $1 =~ full ]] ; then
          line=$line" -s"
        fi
        $line > case.log 2>&1 && \
          echo -e "${GREEN} success${NC}" | tee -a pytest-out.log || \
          echo -e "${RED} failed${NC}" | tee -a pytest-out.log 
        end_time=`date +%s`
        out_log=`tail -1 pytest-out.log  `
        if [[ $out_log =~ 'failed' ]];then
          cp -r ../../sim ~/sim_`date "+%Y_%m_%d_%H:%M:%S" `
          echo '=====================log===================== '
          cat case.log
          rm -rf case.log
          dohavecore $2 2
          if [[ $2 == 1 ]];then
            exit 8
          fi
        fi
        echo execution time of $case was `expr $end_time - $start_time`s. | tee -a pytest-out.log
      else
        $line > /dev/null 2>&1
      fi
      dohavecore $2 2
    fi
  done 
  rm -rf ../../sim/case.log
}

######################
# main entry
######################

unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     OS=Linux;;
    Darwin*)    OS=Darwin;;
    CYGWIN*)    OS=Windows;;
    *)          OS=Unknown;;
esac

case "${OS}" in
    Linux*)     TAOSLIB=libtaos.so;;
    Darwin*)    TAOSLIB=libtaos.dylib;;
    Windows*)    TAOSLIB=taos.dll;;
    Unknown)          TAOSLIB="UNKNOWN:${unameOut}";;
esac

echo TAOSLIB is ${TAOSLIB}

totalFailed=0
totalPyFailed=0
totalJDBCFailed=0
totalUnitFailed=0
totalExampleFailed=0
totalApiFailed=0

if [ "${OS}" == "Linux" ]; then
    corepath=`grep -oP '.*(?=core_)' /proc/sys/kernel/core_pattern||grep -oP '.*(?=core-)' /proc/sys/kernel/core_pattern`
    if [ -z "$corepath" ];then
      echo "/coredump/core_%e_%p_%t" > /proc/sys/kernel/core_pattern || echo "Permission denied"
      corepath="/coredump/"
    fi
fi

if [ "$2" != "jdbc" ] && [ "$2" != "python" ] && [ "$2" != "unit" ]  && [ "$2" != "example" ]; then
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
    runSimCaseOneByOnefq b7 0
  elif [ "$1" == "b2" ]; then
    echo "### run TSIM b2 test ###"
    runSimCaseOneByOnefq b2 0
    runSimCaseOneByOnefq b5 0
  elif [ "$1" == "b3" ]; then
    echo "### run TSIM b3 test ###"
    runSimCaseOneByOnefq b3 0
    runSimCaseOneByOnefq b6 0
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

if [ "$2" != "sim" ] && [ "$2" != "jdbc" ] && [ "$2" != "unit" ]  && [ "$2" != "example" ]; then
  echo "### run Python test case ###"

  cd $tests_dir

  if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]]; then
    cd ../..
  else
    cd ../
  fi

  TOP_DIR=`pwd`
  TAOSLIB_DIR=`find . -name "${TAOSLIB}"|grep -w lib|head -n1`
  if [[ "$TAOSLIB_DIR" == *"$IN_TDINTERNAL"* ]]; then
    LIB_DIR=`find . -name "${TAOSLIB}"|grep -w lib|head -n1|cut -d '/' -f 2,3,4,5`
  else
    LIB_DIR=`find . -name "${TAOSLIB}"|grep -w lib|head -n1|cut -d '/' -f 2,3,4`
  fi

  export LD_LIBRARY_PATH=$TOP_DIR/$LIB_DIR:$LD_LIBRARY_PATH

  [ -f $tests_dir/pytest-out.log ] && rm -f $tests_dir/pytest-out.log
  cd $tests_dir/pytest

  if [ "$1" == "cron" ]; then
    echo "### run Python regression test ###"
    runPyCaseOneByOne regressiontest.sh
  elif [ "$1" == "full" ]; then
    echo "### run Python full test ###"
    cd $tests_dir/develop-test
    for name in *.sh
    do
      runPyCaseOneByOne $name
    done
    cd $tests_dir/system-test
    for name in *.sh
    do
      runPyCaseOneByOne $name
    done
    cd $tests_dir/pytest
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
  elif [ "$1" == "system-test" ]; then
    echo "### run system-test test ###"
    runPyCaseOneByOnefq full 1 system
  elif [ "$1" == "develop-test" ]; then
    echo "### run develop-test test ###"
    runPyCaseOneByOnefq full 1 develop
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


if [ "$2" != "sim" ] && [ "$2" != "python" ] && [ "$2" != "unit" ]  && [ "$2" != "example" ] && [ "$1" == "full" ]; then
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

if [ "$2" != "sim" ] && [ "$2" != "python" ] && [ "$2" != "jdbc" ]  && [ "$2" != "example" ]  && [ "$1" == "full" ]; then
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

if [ "$2" != "sim" ] && [ "$2" != "python" ] && [ "$2" != "jdbc" ] && [ "$2" != "unit" ] && [ "$1" == "full" ]; then
  echo "### run Example tests ###"  

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
  echo "sleeping for 30 seconds"
  #sleep 30
  
  cd $tests_dir
  echo "current dir: "
  pwd
  cd examples/c
  echo "building applications"
  make > /dev/null
  totalExamplePass=0

  echo "Running tests"
  ./apitest > /dev/null 2>&1
  if [ $? != "0" ]; then
    echo "apitest failed"
    totalExampleFailed=`expr $totalExampleFailed + 1`    
  else
    echo "apitest pass"
    totalExamplePass=`expr $totalExamplePass + 1`
  fi 

  ./prepare > /dev/null 2>&1
  if [ $? != "0" ]; then
    echo "prepare failed"
    totalExampleFailed=`expr $totalExampleFailed + 1`    
  else
    echo "prepare pass"
    totalExamplePass=`expr $totalExamplePass + 1`
  fi

  ./subscribe -test > /dev/null 2>&1
  if [ $? != "0" ]; then
    echo "subscribe failed"
    totalExampleFailed=`expr $totalExampleFailed + 1`    
  else
    echo "subscribe pass"
    totalExamplePass=`expr $totalExamplePass + 1`
  fi

  yes |./asyncdemo 127.0.0.1 test 1000 10 > /dev/null 2>&1
  if [ $? != "0" ]; then
    echo "asyncdemo failed"
    totalExampleFailed=`expr $totalExampleFailed + 1`    
  else
    echo "asyncdemo pass"
    totalExamplePass=`expr $totalExamplePass + 1`
  fi

  ./demo 127.0.0.1 > /dev/null 2>&1
  if [ $? != "0" ]; then
    echo "demo failed"
    totalExampleFailed=`expr $totalExampleFailed + 1`    
  else
    echo "demo pass"
    totalExamplePass=`expr $totalExamplePass + 1`
  fi
  echo "### run setconfig tests ###"

  stopTaosd

  cd $tests_dir
  echo "current dir: "
  
  pwd
  
  cd script/api
  echo "building setcfgtest"
  make > /dev/null
  ./clientcfgtest
  if [ $? != "0" ]; then
    echo "clientcfgtest failed"
    totalExampleFailed=`expr $totalExampleFailed + 1`    
  else
    echo "clientcfgtest pass"
    totalExamplePass=`expr $totalExamplePass + 1`
  fi


  if [ "$totalExamplePass" -gt "0" ]; then
    echo -e "\n${GREEN} ### Total $totalExamplePass examples succeed! ### ${NC}"
  fi
  
  if [ "$totalExampleFailed" -ne "0" ]; then
    echo -e "\n${RED} ### Total $totalExampleFailed examples failed! ### ${NC}"
  fi

  if [ "${OS}" == "Linux" ]; then
    dohavecore 1
  fi

  



fi



exit $(($totalFailed + $totalPyFailed + $totalJDBCFailed + $totalUnitFailed + $totalExampleFailed))
