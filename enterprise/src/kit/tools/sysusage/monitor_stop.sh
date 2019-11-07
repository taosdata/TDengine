pid=`ps aux |grep dstat|grep -v grep|awk '{print($2);}'`
sudo kill -9 $pid
