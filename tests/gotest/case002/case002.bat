@echo off
<<<<<<< HEAD
echo ==== start run cases002.go
=======
echo ==== start run cases001.go
>>>>>>> origin/master

del go.*
go mod init demotest
go build
demotest.exe -h %1 -p %2
cd ..

