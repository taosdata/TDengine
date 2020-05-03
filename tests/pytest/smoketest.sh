#!/bin/bash
python3 ./test.py $1 -f insert/basic.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/int.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/float.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/bigint.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/bool.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/double.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/smallint.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/tinyint.py
python3 ./test.py -s $1
sleep 1

python3 ./test.py $1 -f table/column_name.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f table/column_num.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f table/db_table.py
python3 ./test.py -s $1
sleep 1

#python3 ./test.py $1 -f import_merge/importDataLastTO.py 
#python3 ./test.py -s $1
#sleep 1
#python3 ./test.py $1 -f import_merge/importDataLastT.py 
#python3 ./test.py -s $1
#sleep 1
python3 ./test.py $1 -f import_merge/importDataTO.py 
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importDataT.py 
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importHeadOverlap.py 
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importHeadPartOverlap.py 
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importHORestart.py 
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importHPORestart.py 
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importHRestart.py 
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importLastSub.py 
python3 ./test.py -s $1
sleep 1
