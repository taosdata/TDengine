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
python3 ./test.py $1 -f insert/binary.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/date.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/nchar.py
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

python3 ./test.py $1 -f import_merge/importDataLastTO.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importDataLastT.py
python3 ./test.py -s $1
sleep 1
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

python3 ./test.py $1 -f import_merge/importBlock1HO.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock1HPO.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock1H.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock1S.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock1Sub.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock1TO.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock1TPO.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock1T.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock2HO.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock2HPO.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock2H.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock2S.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock2Sub.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock2TO.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock2TPO.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlock2T.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importBlockbetween.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importCacheFileSub.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importCacheFileTO.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importCacheFileT.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importDataLastSub.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importHead.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importLastTO.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importLastT.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importSpan.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importSRestart.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importSubRestart.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importTailOverlap.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importTail.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importTORestart.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importTPORestart.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f import_merge/importTRestart.py
python3 ./test.py -s $1
sleep 1
