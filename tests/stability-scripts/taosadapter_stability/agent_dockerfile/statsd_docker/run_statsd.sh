#!/bin/bash
case "$1" in
    -h|--help)
    echo "Usage:"
    echo "1st arg: agent_count"
    echo "2nd arg: container_hostname prefix"
    echo "3rd arg: TaosadapterIp"
    echo "4th arg: TaosadapterPort"
    echo "eg: ./run_statsd.sh 1 statsd_agent1 172.26.10.86 6044"
    echo "eg: ./run_statsd.sh 2 statsd_agent* 172.26.10.86 6044"
    exit 0
;;
esac

if [ $1 == "rm" ]; then
        docker ps | grep $2 | awk '{print $1}' | xargs docker stop | xargs docker rm
        exit
fi

if [ ! -n "$1" ]; then
    echo "please input 1st arg"
    exit
fi
if [ ! -n "$2" ]; then
    echo "please input 2nd arg"
    exit
fi
if [ ! -n "$3" ]; then
    echo "please input 3rd arg"
    exit
fi
if [ ! -n "$4" ]; then
    echo "please input 4th arg"
    exit
fi
if [ $1 -eq 1 ];then
	docker ps | grep $2
        if [ $? -eq 0 ];then
	        docker stop $2 && docker rm $2
	fi
       	docker run -itd --name $2 -h $2 -e TaosadapterIp=$3 -e TaosadapterPort=$4 taosadapter_statsd:v1 /bin/bash
else
        perfix=`echo $2 | cut -d '*' -f 1`
        for i in `seq 1 $1`;
        do
		docker ps | grep $perfix$i
                if [ $? -eq 0 ];then
	                docker stop $perfix$i && docker rm $perfix$i
		fi
                docker run -itd --name $perfix$i -h $perfix$i -e TaosadapterIp=$3 -e TaosadapterPort=$4 taosadapter_statsd:v1 /bin/bash
        done
fi
#docker run -itd --name statsd_agent1 -h statsd_agent1 -e TaosadapterIp=172.26.10.86 -e TaosadapterPort=6044 taosadapter_statsd:v1 /bin/bash
