#!/bin/bash
root=$1
token=`bash $root/common/token`
http --print HBhb POST 10.0.2.15:8080/sql/db2 sql='insert into meter2 values (now, 100);' --auth-type=jwt --auth="$token"

