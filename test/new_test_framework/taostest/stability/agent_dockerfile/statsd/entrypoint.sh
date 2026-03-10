#!/bin/bash
sed -i 's/TaosadapterIp/'$TaosadapterIp'/g;s/TaosadapterPort/'$TaosadapterPort'/g;' /root/statsd/config.js
nohup node /root/statsd/stats.js /root/statsd/config.js &
sleep 10
for i in `seq 1 100`;
do
	echo "${HOSTNAME}.count${i}:55|c" | nc -w 1 -u 127.0.0.1 8125
done
tail -f /dev/null
