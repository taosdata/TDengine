#!/bin/bash
python3 ./test.py $1 -f insert/basic.py
python3 ./test.py $1 -f insert/int.py
python3 ./test.py $1 -f insert/float.py
python3 ./test.py $1 -f insert/bigint.py
python3 ./test.py $1 -f insert/bool.py
python3 ./test.py $1 -f insert/double.py
python3 ./test.py $1 -f insert/smallint.py
python3 ./test.py $1 -f insert/tinyint.py
python3 ./test.py $1 -f insert/date.py
python3 ./test.py $1 -f insert/binary.py
python3 ./test.py $1 -f insert/nchar.py
python3 ./test.py $1 -f insert/nchar-boundary.py
python3 ./test.py $1 -f insert/nchar-unicode.py
python3 ./test.py $1 -f insert/multi.py

python3 ./test.py $1 -f table/column_name.py
python3 ./test.py $1 -f table/column_num.py
python3 ./test.py $1 -f table/db_table.py
python3 ./test.py $1 -f table/tablename-boundary.py

# tag
python3 ./test.py $1 -f tag_lite/filter.py
python3 ./test.py $1 -f tag_lite/create-tags-boundary.py
python3 ./test.py $1 -f tag_lite/3.py
python3 ./test.py $1 -f tag_lite/4.py
python3 ./test.py $1 -f tag_lite/5.py
python3 ./test.py $1 -f tag_lite/6.py
python3 ./test.py $1 -f tag_lite/add.py
python3 ./test.py $1 -f tag_lite/bigint.py
python3 ./test.py $1 -f tag_lite/binary_binary.py
python3 ./test.py $1 -f tag_lite/binary.py
python3 ./test.py $1 -f tag_lite/bool_binary.py
python3 ./test.py $1 -f tag_lite/bool_int.py
python3 ./test.py $1 -f tag_lite/bool.py
python3 ./test.py $1 -f tag_lite/change.py
python3 ./test.py $1 -f tag_lite/column.py
python3 ./test.py $1 -f tag_lite/commit.py
python3 ./test.py $1 -f tag_lite/create.py
python3 ./test.py $1 -f tag_lite/datatype.py
python3 ./test.py $1 -f tag_lite/datatype-without-alter.py
python3 ./test.py $1 -f tag_lite/delete.py
python3 ./test.py $1 -f tag_lite/double.py
python3 ./test.py $1 -f tag_lite/float.py
python3 ./test.py $1 -f tag_lite/int_binary.py
python3 ./test.py $1 -f tag_lite/int_float.py
python3 ./test.py $1 -f tag_lite/int.py
python3 ./test.py $1 -f tag_lite/set.py
python3 ./test.py $1 -f tag_lite/smallint.py
python3 ./test.py $1 -f tag_lite/tinyint.py

python3 ./test.py $1 -f dbmgmt/database-name-boundary.py

python3 ./test.py $1 -f import_merge/importBlock1HO.py
python3 ./test.py $1 -f import_merge/importBlock1HPO.py
python3 ./test.py $1 -f import_merge/importBlock1H.py
python3 ./test.py $1 -f import_merge/importBlock1S.py
python3 ./test.py $1 -f import_merge/importBlock1Sub.py
python3 ./test.py $1 -f import_merge/importBlock1TO.py
python3 ./test.py $1 -f import_merge/importBlock1TPO.py
python3 ./test.py $1 -f import_merge/importBlock1T.py
python3 ./test.py $1 -f import_merge/importBlock2HO.py
python3 ./test.py $1 -f import_merge/importBlock2HPO.py
python3 ./test.py $1 -f import_merge/importBlock2H.py
python3 ./test.py $1 -f import_merge/importBlock2S.py
python3 ./test.py $1 -f import_merge/importBlock2Sub.py
python3 ./test.py $1 -f import_merge/importBlock2TO.py
python3 ./test.py $1 -f import_merge/importBlock2TPO.py
python3 ./test.py $1 -f import_merge/importBlock2T.py
python3 ./test.py $1 -f import_merge/importBlockbetween.py
python3 ./test.py $1 -f import_merge/importCacheFileHO.py
python3 ./test.py $1 -f import_merge/importCacheFileHPO.py
python3 ./test.py $1 -f import_merge/importCacheFileH.py
python3 ./test.py $1 -f import_merge/importCacheFileS.py
python3 ./test.py $1 -f import_merge/importCacheFileSub.py
python3 ./test.py $1 -f import_merge/importCacheFileTO.py
python3 ./test.py $1 -f import_merge/importCacheFileTPO.py
python3 ./test.py $1 -f import_merge/importCacheFileT.py
python3 ./test.py $1 -f import_merge/importDataH2.py
python3 ./test.py $1 -f import_merge/importDataHO2.py
python3 ./test.py $1 -f import_merge/importDataHO.py
python3 ./test.py $1 -f import_merge/importDataHPO.py
python3 ./test.py $1 -f import_merge/importDataLastHO.py
python3 ./test.py $1 -f import_merge/importDataLastHPO.py
python3 ./test.py $1 -f import_merge/importDataLastH.py
python3 ./test.py $1 -f import_merge/importDataLastS.py
python3 ./test.py $1 -f import_merge/importDataLastSub.py
python3 ./test.py $1 -f import_merge/importDataLastTO.py
python3 ./test.py $1 -f import_merge/importDataLastTPO.py
python3 ./test.py $1 -f import_merge/importDataLastT.py
python3 ./test.py $1 -f import_merge/importDataS.py
python3 ./test.py $1 -f import_merge/importDataSub.py
python3 ./test.py $1 -f import_merge/importDataTO.py
python3 ./test.py $1 -f import_merge/importDataTPO.py
python3 ./test.py $1 -f import_merge/importDataT.py
python3 ./test.py $1 -f import_merge/importHeadOverlap.py
python3 ./test.py $1 -f import_merge/importHeadPartOverlap.py
python3 ./test.py $1 -f import_merge/importHead.py
python3 ./test.py $1 -f import_merge/importHORestart.py
python3 ./test.py $1 -f import_merge/importHPORestart.py
python3 ./test.py $1 -f import_merge/importHRestart.py
python3 ./test.py $1 -f import_merge/importLastHO.py
python3 ./test.py $1 -f import_merge/importLastHPO.py
python3 ./test.py $1 -f import_merge/importLastH.py
python3 ./test.py $1 -f import_merge/importLastS.py
python3 ./test.py $1 -f import_merge/importLastSub.py
python3 ./test.py $1 -f import_merge/importLastTO.py
python3 ./test.py $1 -f import_merge/importLastTPO.py
python3 ./test.py $1 -f import_merge/importLastT.py
python3 ./test.py $1 -f import_merge/importSpan.py
python3 ./test.py $1 -f import_merge/importSRestart.py
python3 ./test.py $1 -f import_merge/importSubRestart.py
python3 ./test.py $1 -f import_merge/importTailOverlap.py
python3 ./test.py $1 -f import_merge/importTailPartOverlap.py
python3 ./test.py $1 -f import_merge/importTail.py
python3 ./test.py $1 -f import_merge/importToCommit.py
python3 ./test.py $1 -f import_merge/importTORestart.py
python3 ./test.py $1 -f import_merge/importTPORestart.py
python3 ./test.py $1 -f import_merge/importTRestart.py

# user
python3 ./test.py $1 -f user/user_create.py
python3 ./test.py $1 -f user/pass_len.py

# table
#python3 ./test.py $1 -f table/del_stable.py

#query
python3 ./test.py $1 -f query/filter.py

