#!/bin/sh

if [ "$1" = "" -o "$2" = "" ]; then
  echo "Usage: bin/deploy.sh [hostname] [deployDir]"
  exit 1
fi

host=$1
deployDir=$2

mvn clean package -Dmaven.test.skip=true
#ssh root@$host "rm -rf /root/tsync/; mkdir /root/tsync; mkdir /root/tsync/config; mkdir /root/tsync/logs"

### jar
scp target/consume-to-net-jar-with-dependencies.jar root@$host:$deployDir/consume-to-net.jar
scp target/consume-to-tdengine-jar-with-dependencies.jar root@$host:$deployDir/consume-to-tdengine.jar
scp target/net-to-tq-jar-with-dependencies.jar root@$host:$deployDir/net-to-tq.jar
scp target/produce-to-tq-jar-with-dependencies.jar root@$host:$deployDir/produce-to-tq.jar

### config
#scp config/consume-to-net.json root@$host:$deployDir/config/consume-to-net.json
#scp config/consume-to-tdengine.json root@$host:$deployDir/config/consume-to-tdengine.json
#scp config/net-to-tq.json root@$host:$deployDir/config/net-to-tq.json
#scp config/produce-to-tq.json root@$host:$deployDir/config/produce-to-tq.json

### scripts
scp bin/produce-to-tq.sh root@$host:$deployDir
scp bin/consume-to-tdengine-start.sh root@$host:$deployDir
scp bin/consume-to-tdengine-stop.sh root@$host:$deployDir
scp bin/consume-to-net-start.sh root@$host:$deployDir
scp bin/consume-to-net-stop.sh root@$host:$deployDir
scp bin/net-to-tq-start.sh root@$host:$deployDir
scp bin/net-to-tq-stop.sh root@$host:$deployDir

ssh root@$host "chmod a+x $deployDir/*.sh"
