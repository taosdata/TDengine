@echo off
echo ==== start run nanosupport.go

del go.*
go mod init nano
go mod tidy
go build
nano.exe -h %1 -p %2
cd ..
