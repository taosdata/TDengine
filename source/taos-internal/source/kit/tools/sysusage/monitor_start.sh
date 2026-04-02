mv monitor.csv monitor.csv.bak
mv taosd.monitor taosd.monitor.bak
dstat -trmc --output monitor.csv &
echo "CPU MEMROY" >> taosd.monitor
while [ 1 ]
do
  currentTime=`date +%H%M%S`
  echo T$currentTime >> taosd.monitor
	ps aux |grep taosd|grep -v grep|awk '{print($3" "$4);}' >>taosd.monitor
	sleep 0.3
done

