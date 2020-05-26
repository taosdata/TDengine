#!/bin/bash
# insert
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
python3 ./test.py $1 -f import_merge/importHead.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f import_merge/importLastT.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f import_merge/importSpan.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f import_merge/importTail.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f import_merge/importTRestart.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f import_merge/importInsertThenImport.py
python3 ./test.py $1 -s && sleep 1

#tag 
python3 ./test.py $1 -f tag_lite/filter.py
python3 ./test.py $1 -s && sleep 1

#query
python3 ./test.py $1 -f query/filter.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f query/filterCombo.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f query/queryNormal.py
python3 ./test.py $1 -s && sleep 1
python3 ./test.py $1 -f query/queryError.py
python3 ./test.py $1 -s && sleep 1

