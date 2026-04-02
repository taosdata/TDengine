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




cd $tempDir
python $clangtidypy/run-clang-tidy.py -clang-tidy-binary $clangtidybin/clang-tidy -checks=-*,misc-taos* -p $debugDir >taostemp

