#!/bin/bash
root=$1
token=`bash $root/common/token`
http --print HBhb POST 10.0.2.15:8080/sql/db2 sql='create table meter2 (ts timestamp, speed int);' --auth-type=jwt --auth="$token"


