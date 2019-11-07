#!/bin/bash
root=$1
token=`bash $root/common/token`
http --print HBhb DELETE 10.0.2.15:8080/sql sql='create dnode 10.0.4.15;' --auth-type=jwt --auth="$token"

