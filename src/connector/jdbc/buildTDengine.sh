#!/bin/bash
ulimit -c unlimited

function buildTDengine {    
	cd /root/TDengine

	git remote update
	REMOTE_COMMIT=`git rev-parse --short remotes/origin/develop`
	LOCAL_COMMIT=`git rev-parse --short @`

	echo " LOCAL: $LOCAL_COMMIT"
	echo "REMOTE: $REMOTE_COMMIT"
	if [ "$LOCAL_COMMIT" == "$REMOTE_COMMIT" ]; then
		echo "repo up-to-date"
	else
		echo "repo need to pull"        
		git pull

		LOCAL_COMMIT=`git rev-parse --short @`        
		cd /root/TDengine/debug        
		rm -rf /root/TDengine/debug/*        
		cmake ..
		make > /dev/null
		make install
	fi
}

function restartTaosd {
	systemctl stop taosd
	pkill -KILL -x taosd
	sleep 10
	
    	logDir=`grep 'logDir' /etc/taos/taos.cfg|awk 'END{print $2}'`
    	dataDir=`grep 'dataDir' /etc/taos/taos.cfg|awk '{print $2}'`    
    
	rm -rf $logDir/*
	rm -rf $dataDir/*
	
	taosd 2>&1 > /dev/null &
	sleep 10
}

buildTDengine
restartTaosd
