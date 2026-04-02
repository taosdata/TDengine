#!bin/bash
# this script works on machine 192.168.1.116
pwdDir=`pwd`
homeDir=/home/ubuntu/workspace
sourceDir=$homeDir/TDinternal
debugDir=$sourceDir/debug
simDir=$sourceDir/enterprise/tests/jenkins
currentDate=`date +%Y%m%d`
outputf=coverage$currentDate
recordf=record_coverage

echo "test finished, begin to count coverage >>>>>>>"
cd $sourceDir
gcovr -r . -o coverage.xml 
rm -rf $homeDir/report/coverage*
mv coverage.xml $homeDir/report/$outputf
cd $homeDir
mv $homeDir/report/$recordf ./
python2 $homeDir/py/coverAnalyze.py $homeDir/report/$outputf
mv $recordf $homeDir/report/
echo "Please to $homeDir/report and take a look at the report"

