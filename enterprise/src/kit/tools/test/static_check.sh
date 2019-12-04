currentDate=`date +%Y%m%d`
outputf=static_check_report$currentDate
homeDir=/home/ubuntu/fpan/workspace
sourceDir=$homeDir/TDinternal
pwdDir=`pwd`
cd $sourceDir/community
git checkout develop
git pull
cd $sourceDir/enterprise
git checkout develop
git pull
cd $pwdDir
echo "begin the static check >>>>>>"
cppcheck -q --enable=all --output-file=$outputf -rp=$sourceDir $sourceDir 
rm -rf $homeDir/report/*
mv $outputf $homeDir/report/
cd $homeDir/report/
errcount=`grep "\[" static_check_report20191204 |wc -l`
echo "There is totally $errcount errors or warnings from the static check" >>$outputf
echo "static check has finished and result is in the report directory"
