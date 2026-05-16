#!/bin/bash
# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'
IN_TDINTERNAL="community"
TDIR=`pwd`
if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]]; then
  cd ../..
else
  cd ../../..
fi
TOP_DIR=`pwd`
TAOSD_DIR=`find . -name "taosd"|grep -v community|grep bin|head -n1`
VALGRIND_OUT=taosd_valgrind.out 
VALGRIND_ERR=taosd_valgrind.err
rm -rf /var/lib/taos/*
# nohup valgrind  --tool=memcheck --leak-check=yes $TAOSD_DIR > $TDIR/$VALGRIND_OUT 2> $TDIR/$VALGRIND_ERR  &
nohup valgrind  --leak-check=yes $TAOSD_DIR > $TDIR/$VALGRIND_OUT 2> $TDIR/$VALGRIND_ERR  &
sleep 20
cd -
./crash_gen.sh  -p -t 10 -s 1000 
ps -ef |grep valgrind|grep -v grep|awk '{print $2}'|xargs kill -term
while true 
do
	monitoring=` ps -ef|grep valgrind |grep -v grep| wc -l`
	if [ $monitoring -eq 0 ] 
	then
		echo "Manipulator is not running "
    break
	else
		sleep 1
	fi
done

grep 'start to execute\|ERROR SUMMARY' $VALGRIND_ERR | grep -v 'grep' | uniq | tee taosd_mem_err.log

for memError in `grep 'ERROR SUMMARY' taosd_mem_err.log | awk '{print $4}'`
do
memError=(${memError//,/})
if [ -n "$memError" ]; then
    if [ "$memError" -gt 12 ]; then
    echo -e "${RED} ## Memory errors number valgrind reports is $memError.\
                More than our threshold! ## ${NC}"
    fi
fi
done

grep 'start to execute\|definitely lost:' $VALGRIND_ERR|grep -v 'grep'|uniq|tee taosd-definitely-lost-out.log
for defiMemError in `grep 'definitely lost:' taosd-definitely-lost-out.log | awk '{print $7}'`
do
defiMemError=(${defiMemError//,/})
if [ -n "$defiMemError" ]; then
    if [ "$defiMemError" -gt 0 ]; then
      cat $VALGRIND_ERR
      echo -e "${RED} ## Memory errors number valgrind reports \
                  Definitely lost is $defiMemError. More than our threshold! ## ${NC}"
      exit 8
    fi
fi
done
