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
git checkout feature/fangtest
git merge develop
cp $homeDir/report/record* ./
python2 sepQFromSc.py $outputf
rm -rf $homeDir/report/static*
mv *$outputf* $homeDir/report/
mv record* $homeDir/report/
echo "static check has finished and result is in the report directory"
