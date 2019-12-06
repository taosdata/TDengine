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
python2 cleanDepsFromSC.py $outputf
rm -rf $homeDir/report/static*
mv $outputf* $homeDir/report/
cd $homeDir/report/
errcount=`grep "\[" $outputf |wc -l`
echo "There is totally $errcount errors or warnings" >>$outputf
errcount=`grep "\[" $outputf.nodeps |wc -l`
echo "There is totally $errcount errors or warnings" >>$outputf.nodeps
echo "static check has finished and result is in the report directory"
