# shell script: network_check.sh
# function: check whether network works well and whether there is taosd running
# author: fang pan
# date: 2019-10-21

if [ $# -ne 4 ]
then
    echo "the parameter number is incorrect!"
    echo "Please add the IP and port!"
    exit
fi
IP=$1
port=$2
user=$3
device=$4
if [ -z $IP -o -z $port ]
then
    echo "the parameter should not be null,exit!" >>${log_file}
    exit
fi


echo "First use ping to check network>>>>>>>>>>>>>>>>>>>>>>"
# get number of lost packages
RATE=`ping -c 4 -w 3 $IP | grep 'packet loss' | grep -v grep | awk -F',' '{print $3}' | awk -F'%' '{print $1}'`
if [ $RATE -eq 100 ]
then
    echo " ping the $IP is not connected, FAILURE"
    echo " FAILURE"
    exit
else
    echo " ping the $IP is connected"
    result=`echo -e "\n" | telnet $IP $port 2>/dev/null | grep "Connected" | wc -l`

    if [ $result -eq 1 ]
    then
        echo "  Network $IP:$port is Open."
    else
        echo "  Network $IP:$port is Closed."
    fi
fi

echo "Check whether TDengine server is running on $IP>>>>>>>>>>>>>"
if [ $result -eq 0 ]
then
    echo "  TDengine server is running on $IP"
else
    echo "  TDengine server is running on $IP"
fi

echo "Check current network speed>>>>>>>>>>>>>>>>"
for((i=1;i<=3;i++));
do
down_speed_old=`cat /sys/class/net/$device/statistics/rx_bytes`
up_speed_old=`cat /sys/class/net/$device/statistics/tx_bytes`
sleep 10
down_speed_new=`cat /sys/class/net/$device/statistics/rx_bytes`
up_speed_new=`cat /sys/class/net/$device/statistics/tx_bytes`
down_speed=`echo "($down_speed_new-$down_speed_old)/10240"|bc`
up_speed=`echo "($up_speed_new-$up_speed_old)/10240"|bc`
echo -e "speed : \n\tDN : $[(($down_speed_new-$down_speed_old))] B/s \t $down_speed KB/s"
echo -e "speed : \n\tUP : $[(($up_speed_new-$up_speed_old))] B/s \t $up_speed KB/s"
done
