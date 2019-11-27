pwdDir=`pwd`
sourceDir=/home/ubuntu/fpan/workspace/TDinternal
debugDir=$sourceDir/debug
pyDir=$sourceDir/enterprise/tests/pytest
echo "clean up previous binary file >>>>>>>>>"
cd $sourceDir
lcov --directory . -z
find ./ -name '*.c.gcno' |xargs rm -r
echo "build >>>>>>>>>"
cd $debugDir
rm -rf *
cmake .. -DCOVER=true
make
cd $debugDir/build/lib
sudo cp libtaos.so* /usr/lib
echo "test >>>>>>>>>"
cd $pyDir
python2 generate_testscript.py
./testlist.sh
cd $sourceDir
gcovr -r . -o coverage.xml 
mv coverage.xml $pwdDir/
