#!/bin/bash
ulimit -c unlimited

# insert
python3 ./test.py $1 -f insert/basic.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f insert/bigint.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f insert/nchar.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f insert/multi.py
python3 ./test.py $1 -s && sleep 1

# table
python3 ./test.py $1 -f table/column_name.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f table/column_num.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f table/db_table.py
python3 ./test.py $1 -s && sleep 1

# import
python3 ./test.py $1 -f import_merge/importDataLastSub.py
python3 ./test.py $1 -s && sleep 1

#tag 
python3 ./test.py $1 -f tag_lite/filter.py
python3 ./test.py $1 -s && sleep 1

#query
python3 ./test.py $1 -f query/filter.py
python3 ./test.py $1 -s && sleep 1

# client
python3 ./test.py $1 -f client/client.py
python3 ./test.py $1 -s && sleep 1

# connector
python3 ./test.py $1 -f connector/lua.py
