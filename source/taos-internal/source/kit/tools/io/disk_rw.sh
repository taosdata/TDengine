# shell script: disk_rw.sh
# function: check the disk read and write speed
# author: fang pan
# date: 2019-10-21

if [ $# -ne 2 ]
then
    echo "the parameter number is incorrect! "
    echo "Please add the disk you want to check and a folder on the disk!"
    exit
fi
disk=$1
toWrite=$2
sudo fio -filename=$disk -direct=1 -iodepth 1 -thread -rw=randread -ioengine=psync -bs=16k -size=200M -numjobs=1 -runtime=10 -group_reporting -name=mytest > diskrw_report.txt
echo "==========================================" >> diskrw_report.txt
cat diskrw_report.txt | grep "READ:" | grep -v grep
touch $toWrite/mytest.txt
sudo fio -filename=$toWrite/mytest.txt -direct=1 -iodepth 1 -thread -rw=randwrite -ioengine=psync -bs=16k -size=200M -numjobs=1 -runtime=10 -group_reporting -name=mytest  >>diskrw_report.txt
cat diskrw_report.txt | grep "WRITE:" | grep -v grep
echo "Detailed report about disk IO can be found in the file diskrw_report.txt"
rm -rf $toWrite/mytest.txt
