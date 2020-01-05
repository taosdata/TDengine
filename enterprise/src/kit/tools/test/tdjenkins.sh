homeDir=/home/ubuntu/workspace
sourceDir=$homeDir/TDinternal
debugDir=$sourceDir/debug
pyDir=$sourceDir/enterprise/tests/pytest
simDir=$sourceDir/enterprise/tests/jenkins

echo "update the code from github internal"
cd $sourceDir/community
git checkout develop
git pull
cd $sourceDir/enterprise
git checkout develop
git pull

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
git checkout feature/fangtest
git pull
#cd $pyDir
#python2 generate_testscript.py
cd $simDir
rm fangTest.txt
cp fangTest.template fangTest.txt
#cat $pyDir/testlist.sh >> fangTest.txt
./tjenkins -p -f fangTest.txt
cd $simDir
