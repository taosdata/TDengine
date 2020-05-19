#!/bin/bash
# insert
python3 ./test.py -g -f insert/basic.py
python3 ./test.py -g -s && sleep 1
python3 ./test.py -g -f insert/int.py
python3 ./test.py -g -s && sleep 1
python3 ./test.py -g -f insert/float.py
python3 ./test.py -g -s && sleep 1
python3 ./test.py -g -f insert/bigint.py
python3 ./test.py -g -s && sleep 1
python3 ./test.py -g -f insert/bool.py
python3 ./test.py -g -s && sleep 1
python3 ./test.py -g -f insert/double.py
python3 ./test.py -g -s && sleep 1
python3 ./test.py -g -f insert/smallint.py
python3 ./test.py -g -s && sleep 1
python3 ./test.py -g -f insert/tinyint.py
python3 ./test.py -g -s && sleep 1
python3 ./test.py -g -f insert/binary.py
python3 ./test.py -g -s && sleep 1
python3 ./test.py -g -f insert/date.py
python3 ./test.py -g -s && sleep 1
python3 ./test.py -g -f insert/nchar.py
python3 ./test.py -g -s && sleep 1
python3 ./test.py -g -f insert/multi.py
python3 ./test.py -g -s && sleep 1

# table
python3 ./test.py -g -f table/column_name.py
python3 ./test.py -g -s && sleep 1
python3 ./test.py -g -f table/column_num.py
python3 ./test.py -g -s && sleep 1
python3 ./test.py -g -f table/db_table.py
python3 ./test.py -g -s && sleep 1

# import
python3 ./test.py -g -f import_merge/importDataLastSub.py
python3 ./test.py -g -s && sleep 1

#tag 
python3 ./test.py $1 -f tag_lite/filter.py
python3 ./test.py $1 -s && sleep 1
