#!/bin/bash
root=$1
token=`bash $root/common/token`
http --print HBhb POST 10.0.2.15:8080/ sql='create dnode 10.0.4.15;' --auth-type=jwt --auth="$token"

