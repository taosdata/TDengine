#!/bin/sh

host="192.168.1.139"
mvn clean package -Dmaven.test.skip=true
ssh root@$host "rm -rf /root/tsync/; mkdir /root/tsync; mkdir /root/tsync/config; mkdir /root/tsync/logs"

scp target/consume-to-net-jar-with-dependencies.jar root@$host:/root/tsync/consume-to-net.jar
scp config/consume-to-net.json root@$host:/root/tsync/config/consume-to-net.json

scp target/consume-to-tdengine-jar-with-dependencies.jar root@$host:/root/tsync/consume-to-tdengine.jar
scp config/consume-to-tdengine.json root@$host:/root/tsync/config/consume-to-tdengine.json

scp target/net-to-tq-jar-with-dependencies.jar root@$host:/root/tsync/net-to-tq.jar
scp config/net-to-tq.json root@$host:/root/tsync/config/net-to-tq.json

scp target/produce-to-tq-jar-with-dependencies.jar root@$host:/root/tsync/produce-to-tq.jar
scp config/produce-to-tq.json root@$host:/root/tsync/config/produce-to-tq.json

scp bin/produce-to-tq.sh root@$host:/root/tsync

scp bin/consume-to-tdengine-start.sh root@$host:/root/tsync
scp bin/consume-to-tdengine-stop.sh root@$host:/root/tsync

scp bin/consume-to-net-start.sh root@$host:/root/tsync
scp bin/consume-to-net-stop.sh root@$host:/root/tsync

scp bin/net-to-tq-start.sh root@$host:/root/tsync
scp bin/net-to-tq-stop.sh root@$host:/root/tsync

ssh root@$host "chmod a+x /root/tsync/*.sh"
