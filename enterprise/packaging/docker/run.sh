#!/bin/bash
TAOS_RUN_TAOSBENCHMARK_TEST_ONCE=0
while ((1))
do
    # echo "outer loop: $a"
    sleep 10
    output=`taos -k`
    status=${output:0:1}
    # echo $output
    # echo $status
    if [ "$status"x = "0"x ]
    then
        taosd &
    fi
    # echo "$status"x "$TAOS_RUN_TAOSBENCHMARK_TEST"x "$TAOS_RUN_TAOSBENCHMARK_TEST_ONCE"x
    if [ "$status"x = "2"x ] && [ "$TAOS_RUN_TAOSBENCHMARK_TEST"x = "1"x ] && [ "$TAOS_RUN_TAOSBENCHMARK_TEST_ONCE"x = "0"x ]
    then
        TAOS_RUN_TAOSBENCHMARK_TEST_ONCE=1
        # result=`taos -s "show databases;" | grep " test "`
        # if [ "${result:0:5}"x != " test"x ]
        # then
        #     taosBenchmark -y -t 1000 -n 1000 -S 900000
        # fi
        taos -s "select stable_name from information_schema.ins_stables where db_name = 'test';"|grep -q -w meters
        if [ $? -ne 0 ]; then
            taosBenchmark -y -t 1000 -n 1000 -S 900000
	    taos -s "create user admin_user pass 'NDS65R6t' sysinfo 0;"
	    taos -s "GRANT ALL on test.* to admin_user;"
        fi
    fi
    # check taosadapter
    nc -z localhost 6041
    if [ $? -ne 0 ]; then
	taosadapter &
    fi
done
