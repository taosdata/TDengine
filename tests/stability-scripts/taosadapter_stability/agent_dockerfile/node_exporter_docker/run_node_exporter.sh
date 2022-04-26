#!/bin/bash
case "$1" in
    -h|--help)
    echo "Usage:"
    echo "1st arg: port range"
    echo "2nd arg: container_hostname prefix"
    echo "eg: ./run_node_exporter.sh 10000 node_exporter_agent1"
    echo "eg: ./run_node_exporter.sh 10000:10010 node_exporter_agent*"
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

if [ ! `echo $1 | grep :` ];then
        docker ps | grep $2
        if [ $? -eq 0 ];then
                docker stop $2 && docker rm $2
        fi
        docker run -itd --name $2 -h $2 -p $1:9100 taosadapter_node_exporter:v1 /bin/bash
else
        perfix=`echo $2 | cut -d '*' -f 1`
	start_port=`echo $1 | cut -d ':' -f 1`
	end_port=`echo $1 | cut -d ':' -f 2`
        for i in `seq $start_port $end_port`;
        do
                docker ps | grep $perfix$i
                if [ $? -eq 0 ];then
                        docker stop $perfix$i && docker rm $perfix$i
                fi
		docker run -itd --name $perfix$i -h $perfix$i -p $i:9100 taosadapter_node_exporter:v1 /bin/bash
        done
fi
#docker run -itd --name node_exporter_agent1 -h node_exporter_agent1 -p 10000:9100 taosadapter_node_exporter:v1 /bin/bash
