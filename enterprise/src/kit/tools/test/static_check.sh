#!bin/bash

# this script works on machine 192.168.1.116

homeDir=/home/ubuntu/workspace
sourceDir=$homeDir/TDinternal
debugDir=$sourceDir/debugtest
pyDir=$homeDir/py
tempDir=$homeDir/temp
recordDir=$homeDir/report/static_check
llvm=$homeDir/llvm
clangtidybin=$llvm/build/bin
clangtidypy=$llvm/tools/clang/tools/extra/clang-tidy/tool




echo "update the code from github internal"
cd $sourceDir/community
git checkout develop
sudo git pull
cd $sourceDir/enterprise
git checkout develop
sudo git pull

echo "building >>>>>>>>"
cd $debugDir
sudo rm -rf *
sudo cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..
sudo make

echo "checking >>>>>>>>>"
cd $tempDir
sudo rm -rf *
python $clangtidypy/run-clang-tidy.py -clang-tidy-binary $clangtidybin/clang-tidy -p $debugDir >temp
python $clangtidypy/run-clang-tidy.py -clang-tidy-binary $clangtidybin/clang-tidy -checks=-*,misc-taos* -p $debugDir >taostemp
python $pyDir/formatCheck.py -p $debugDir


echo "output result >>>>>>"
cp $recordDir/record_check ./
rm -rf $recordDir/error*
rm -rf $recordDir/note*
rm -rf $recordDir/warn*
rm -rf $recordDir/taos*
python $pyDir/checkAnalyze.py
cp * $recordDir/

