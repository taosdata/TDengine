currentDate=`date +%Y%m%d`
outputf=static_check_report$currentDate
recordf=record_static_check
homeDir=/home/ubuntu/fpan/workspace
sourceDir=$homeDir/TDinternal
pyDir=$sourceDir/enterprise/src/kit/tools/test

cd $sourceDir/community
git checkout develop
git pull
cd $sourceDir/enterprise
git checkout develop
sudo git pull
echo "begin the static check >>>>>>"
cppcheck -q --enable=all --output-file=$outputf -rp=$homeDir $sourceDir 
git checkout feature/fangtest
sudo git pull
#git merge develop
cd $homeDir
cp $homeDir/report/$recordf ./
python2 $pyDir/sepQFromSc.py $outputf
rm -rf $homeDir/report/static*
mv *$outputf* $homeDir/report/
mv $recordf $homeDir/report/
echo "static check has finished and result is in the report directory"
