#!/bin/bash
BUILD_ID=dontKillMe

#******This script setup 3 nodes env for remote client installer test. Only for Linux *********

pwd=`pwd`
hostname=`hostname`
if [ -z $JENKINS_HOME ]; then
    workdir="${pwd}/cluster"
    echo $workdir
else
    workdir="${JENKINS_HOME}/workspace/cluster"
    echo $workdir
fi

name="taos"
if command -v prodb ;then
  name="prodb"
fi

# Stop all taosd processes
for(( i=0; i<3; i++))
do
    pid=$(ps -ef | grep ${name}d | grep -v grep | awk '{print $2}')
    if [ -n "$pid" ]; then
        ${csudo}kill -9 $pid || :
    fi
done

# Init 3 dnodes workdir and config file
rm -rf ${workdir}
mkdir ${workdir}
mkdir ${workdir}/output
mkdir ${workdir}/dnode1
mkdir ${workdir}/dnode1/data
mkdir ${workdir}/dnode1/log
mkdir ${workdir}/dnode1/cfg
touch ${workdir}/dnode1/cfg/${name}.cfg
echo -e "firstEp ${hostname}:6031\nsecondEp ${hostname}:6032\nfqdn ${hostname}\nserverPort 6031\nlogDir ${workdir}/dnode1/log\ndataDir ${workdir}/dnode1/data\n" >> ${workdir}/dnode1/cfg/${name}.cfg

# Start first node
nohup ${name}d -c ${workdir}/dnode1/cfg/${name}.cfg & > /dev/null
sleep 5

${name} -P 6031 -s "CREATE DNODE \`${hostname}:6032\`;CREATE DNODE \`${hostname}:6033\`"

mkdir ${workdir}/dnode2
mkdir ${workdir}/dnode2/data
mkdir ${workdir}/dnode2/log
mkdir ${workdir}/dnode2/cfg
touch ${workdir}/dnode2/cfg/${name}.cfg
echo -e "firstEp ${hostname}:6031\nsecondEp ${hostname}:6032\nfqdn ${hostname}\nserverPort 6032\nlogDir ${workdir}/dnode2/log\ndataDir ${workdir}/dnode2/data\n" >> ${workdir}/dnode2/cfg/${name}.cfg

nohup ${name}d -c ${workdir}/dnode2/cfg/${name}.cfg & > /dev/null
sleep 5

mkdir ${workdir}/dnode3
mkdir ${workdir}/dnode3/data
mkdir ${workdir}/dnode3/log
mkdir ${workdir}/dnode3/cfg
touch ${workdir}/dnode3/cfg/${name}.cfg
echo -e "firstEp ${hostname}:6031\nsecondEp ${hostname}:6032\nfqdn ${hostname}\nserverPort 6033\nlogDir ${workdir}/dnode3/log\ndataDir ${workdir}/dnode3/data\n" >> ${workdir}/dnode3/cfg/${name}.cfg

nohup ${name}d -c ${workdir}/dnode3/cfg/${name}.cfg & > /dev/null
sleep 5

${name} -P 6031 -s "CREATE MNODE ON DNODE 2;CREATE MNODE ON DNODE 3;"