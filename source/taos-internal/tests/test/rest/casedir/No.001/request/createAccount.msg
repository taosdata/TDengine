#!/bin/bash
root=$1
token=`bash $root/common/token`
http --print HBhb POST 10.0.2.15:8080/sql sql='create account demo2 PASS 123;' --auth-type=jwt --auth="$token"

