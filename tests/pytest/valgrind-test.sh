#!/bin/bash
python3 ./test.py $1 -f insert/basic.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f insert/int.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f insert/float.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f insert/bigint.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f insert/bool.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f insert/double.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f insert/smallint.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f insert/tinyint.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f insert/binary.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f insert/date.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f insert/nchar.py
python3 ./test.py $1 -s && sleep 1

python3 ./test.py $1 -f table/column_name.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f table/column_num.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f table/db_table.py
python3 ./test.py $1 -s && sleep 1

python3 ./test.py $1 -f import_merge/importDataLastSub.py
python3 ./test.py $1 -s && sleep 1
