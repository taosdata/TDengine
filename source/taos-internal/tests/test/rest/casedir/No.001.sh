#!/bin/bash
ignoreTime=1
rm ./No.001.rpt
bash ../httpRun.sh No.001 createDnode
sleep 4s
#bash ../httpRun.sh No.001 showMnode $ignoreTime
sleep 4s
bash ../httpRun.sh No.001 showDnode $ignoreTime
bash ../httpRun.sh No.001 createUser
bash ../httpRun.sh No.001 createAccount
bash ../httpRun.sh No.001 createDb2
bash ../httpRun.sh No.001 createTable
bash ../httpRun.sh No.001 createTableNoDb
sleep 1
bash ../httpRun.sh No.001 insertData1
sleep 1
bash ../httpRun.sh No.001 selectData1 $ignoreTime

