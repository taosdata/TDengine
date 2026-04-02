curl -d "create database $1" "http://127.0.0.1:8388/"
curl -d "create table $2(ts timestamp, temperature tinyint, pressure smallint)" "http://127.0.0.1:8388/$1"
curl -d "insert into $2 values(now+4s, 17, 120)" "http://127.0.0.1:8388/$1/sql"
curl -d "insert into $2 values(now+4s, 18, 110)" "http://127.0.0.1:8388/$1/sql"
curl -d "insert into $2 values(now+4s, 19, 100)" "http://127.0.0.1:8388/$1/sql"
curl -d "select * from $2" "http://127.0.0.1:8388/$1/sql"

