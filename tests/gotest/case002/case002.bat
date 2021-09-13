@echo off
echo ==== start run cases002.go

del go.*
go mod init demotest
go build
demotest.exe -h %1 -p %2
cd ..

