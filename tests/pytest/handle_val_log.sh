# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

grep 'start to execute\|ERROR SUMMARY' mem-error-out.log|grep -v 'grep'|uniq|tee uniq-mem-error-out.log

for memError in `grep 'ERROR SUMMARY' uniq-mem-error-out.log | awk '{print $4}'`
do
if [ -n "$memError" ]; then
    if [ "$memError" -gt 12 ]; then
    echo -e "${RED} ## Memory errors number valgrind reports is $memError.\
                More than our threshold! ## ${NC}"
    fi
fi
done

grep 'start to execute\|definitely lost:' mem-error-out.log|grep -v 'grep'|uniq|tee uniq-definitely-lost-out.log
for defiMemError in `grep 'definitely lost:' uniq-definitely-lost-out.log | awk '{print $7}'`
do
if [ -n "$defiMemError" ]; then
    if [ "$defiMemError" -gt 13 ]; then
    echo -e "${RED} ## Memory errors number valgrind reports \
                Definitely lost is $defiMemError. More than our threshold! ## ${NC}"
    fi
fi
done