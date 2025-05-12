#!/bin/bash

mkdir -p /data/wz/crash_gen_logs/
logdir='/data/wz/crash_gen_logs/'
date_tag=`date +%Y%m%d-%H%M%S`
hostname='vm_valgrind_'

for i in {1..50}
do 
        echo $i
        # create docker and start crash_gen
        log_dir=${logdir}${hostname}${date_tag}_${i}
        docker run -d  --hostname=${hostname}${date_tag}_${i} --name ${hostname}${date_tag}_${i} --privileged  -v ${log_dir}:/corefile/ -- crash_gen:v1.0 sleep 99999999999999 
                echo create docker ${hostname}${date_tag}_${i}
                docker exec -d ${hostname}${date_tag}_${i} sh -c 'rm -rf /home/taos-connector-python'
                docker cp /data/wz/TDengine ${hostname}${date_tag}_${i}:/home/TDengine
                docker cp /data/wz/taos-connector-python ${hostname}${date_tag}_${i}:/home/taos-connector-python
                echo copy TDengine in container done!
        docker exec  ${hostname}${date_tag}_${i}  sh -c 'sh /home/TDengine/tests/pytest/auto_run_valgrind.sh '
        if [ $? -eq 0 ] 
        then
                echo crash_gen exit as expect , run success

                # # clear docker which success
                docker stop ${hostname}${date_tag}_${i}
                docker rm -f ${hostname}${date_tag}_${i}
        else
                docker stop ${hostname}${date_tag}_${i}
                echo crash_gen exit error , run failed
        fi
done