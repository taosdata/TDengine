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
function runSimCaseOneByOnefq {
  end=`sed -n '$=' jenkins/basic.txt` 
  for ((i=1;i<=$end;i++)) ; do
    if [[ $(($i%$1)) -eq $3 ]];then
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
    fi
  done < $1
}

function runPyCaseOneByOnefq() {
  end=`sed -n '$=' $1`
  for ((i=1;i<=$end;i++)) ; do
   if [[ $(($i%$2)) -eq $4 ]];then
    line=`sed -n "$i"p $1`
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
          dohavecore $3 2
          if [[ $3 == 1 ]];then
            exit 8
          fi
        fi
        echo execution time of $case was `expr $end_time - $start_time`s. | tee -a pytest-out.log
      else
        $line > /dev/null 2>&1
      fi
      dohavecore $3 2
    else
    echo $line
      if [[ $line =~ ^bash.* ]]; then
        $line > case.log 2>&1  
        cat case.log
        if [ $? -ne 0 ];then
          exit 8
        fi
      fi
    fi
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

cd $tests_dir/pytest

[ -f pytest-out.log ] && rm -f pytest-out.log

if [ "$1" == "full" ]; then
  echo "### run Python full test ###"
  runPyCaseOneByOne fulltest-tools.sh
  runPyCaseOneByOne fulltest-query.sh
  runPyCaseOneByOne fulltest-other.sh
  runPyCaseOneByOne fulltest-insert.sh
  runPyCaseOneByOne fulltest-connector.sh
elif [ "$1" == "sim" ]; then
  echo "### run sim $2 test ###"
  cd $tests_dir/script
  runSimCaseOneByOnefq  $2 1 $3
else
  echo "### run $1 $2 test ###"

  if [ "$1" != "query" ] && [ "$1" != "taosAdapter" ] && [ "$1" != "other" ] && [ "$1" != "tools" ] && [ "$1" != "insert" ] && [ "$1" != "connector" ] ;then
    echo " wrong option:$1 must one of [query,other,tools,insert,connector,taosAdapter]"
    exit 8
  fi
  cd $tests_dir/pytest
  runPyCaseOneByOnefq fulltest-$1.sh $2 1 $3 
  cd $tests_dir/develop-test
  runPyCaseOneByOnefq fulltest-$1.sh $2 1 $3 
  cd $tests_dir/system-test
  runPyCaseOneByOnefq fulltest-$1.sh $2 1 $3 
fi

