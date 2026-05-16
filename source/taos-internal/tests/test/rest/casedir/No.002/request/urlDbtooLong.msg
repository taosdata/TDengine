#!/bin/bash
root=$1
token=`bash $root/common/token`
http --print HBhb POST 10.0.2.15:8080/sql/abcdefghijklmnopqrstuvwxyz sql='create database abcdefghijklmnopqrstuvwxyz;' --auth-type=jwt --auth="$token"


