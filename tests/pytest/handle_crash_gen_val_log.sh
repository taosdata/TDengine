#!/bin/bash
# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'
nohup /root/TDinternal/debug/build/bin/taosd -c /root/TDinternal/community/sim/dnode1/cfg >/dev/null &
./crash_gen.sh --valgrind -p -t 10 -s 100 -b 4
pidof taosd|xargs kill  
grep 'start to execute\|ERROR SUMMARY' valgrind.err|grep -v 'grep'|uniq|tee crash_gen_mem_err.log

for memError in `grep 'ERROR SUMMARY' crash_gen_mem_err.log | awk '{print $4}'`
do
if [ -n "$memError" ]; then
    if [ "$memError" -gt 12 ]; then
    echo -e "${RED} ## Memory errors number valgrind reports is $memError.\
                More than our threshold! ## ${NC}"
    fi
fi
done

grep 'start to execute\|definitely lost:' valgrind.err|grep -v 'grep'|uniq|tee crash_gen-definitely-lost-out.log
for defiMemError in `grep 'definitely lost:' crash_gen-definitely-lost-out.log | awk '{print $7}'`
do

if [ -n "$defiMemError" ]; then
    if [ "$defiMemError" -gt 3 ]; then
    echo -e "${RED} ## Memory errors number valgrind reports \
                Definitely lost is $defiMemError. More than our threshold! ## ${NC}"
    exit 8
    fi
fi
done