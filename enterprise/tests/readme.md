##Test
###Prepare (compile tdengine and install python driver)
0. rootDir=./
1. cd ..
2. mkdir debug
3. cd debug
4. cmake ..
5. cmake --build .
6. cd ../src/connector/python/linux
7. pip install python2/


###Run single test
1. cd $rootDir/jenkins
2. vim testList.txt 
3. copy any row of command in the testList.txt and run it in current directory

