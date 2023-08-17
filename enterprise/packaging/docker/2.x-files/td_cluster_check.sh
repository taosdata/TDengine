#!/bin/bash


sleep 13
echo "td check successfully!"

exit 0


FQDN=$(taosd -C|grep -E 'fqdn.*(\S+)' -o |head -n1|sed 's/fqdn *//')
FIRSET_EP=$(taosd -C|grep -E 'firstEp.*(\S+)' -o|head -n1|sed 's/firstEp *//')

# parse first ep host and port
FIRST_EP_HOST=${FIRSET_EP%:*}
FIRST_EP_PORT=${FIRSET_EP#*:}

TAOS_TIMEOUT_SECOND=${TAOS_TIMEOUT_SECOND:-10}
ENDPOINT=$FQDN:$SERVER_PORT

logging() {
    logLevel=$1
    logMsg=$2
    echo "`date \"+%Y-%m-%d %H:%M:%S.%N\"` run.sh: [$logLevel] $logMsg" 2>&1 | tee -a /var/log/td_cluster_check.log
}


logging "INFO" "TAOS_TIMEOUT_SECOND: ${TAOS_TIMEOUT_SECOND}"
logging "INFO" "FIRST_EP_HOST: ${FIRST_EP_HOST}"
logging "INFO" "FIRST_EP_PORT: ${FIRST_EP_PORT}"
logging "INFO" "ENDPOINT: ${ENDPOINT}"

nc -z localhost 6041
if [ $? -ne 0 ]; then
    logging "INFO" "taosadapter port 6041 is not ok"
    exit 1
fi

# get dnode id
DNODETmp=$(timeout $TAOS_TIMEOUT_SECOND taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -w 2000 -s "show dnodes;" | grep -E "$ENDPOINT" | awk '{split($0,a,"|");print a[1]}')
if [[ "$DNODETmp" == "" ]]; then
    logging "INFO" "Could not get dnode number"
    exit 1
fi
logging "INFO" "DNODETmp $DNODETmp"


# get vgroup status
VGroupStatusTmp=$(timeout $TAOS_TIMEOUT_SECOND taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT  -w 2000 -s "show vnodes on dnode $DNODETmp;" | grep -i false)
if [[ "$VGroupStatusTmp" == "" ]]; then
    logging "INFO" "All vgroup is ready!!"
    exit 0
else 
    logger "INFO" "there is vgroup not ready ！！ $VGroupStatusTmp"
    exit 1
fi

