#!/bin/bash
root=$1
http --print HBhb POST 10.0.2.15:8080/sql/db2 sql='select * from meter2;'

