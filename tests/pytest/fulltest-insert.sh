#!/bin/bash
ulimit -c unlimited
#======================p1-start===============

python3 testCompress.py
python3 testNoCompress.py
python3 ./test.py -f import_merge/importBlock1HO.py
python3 ./test.py -f import_merge/importBlock1HPO.py
python3 ./test.py -f import_merge/importBlock1H.py
python3 ./test.py -f import_merge/importBlock1S.py
python3 ./test.py -f import_merge/importBlock1Sub.py
python3 ./test.py -f import_merge/importBlock1TO.py
python3 ./test.py -f import_merge/importBlock1TPO.py
python3 ./test.py -f import_merge/importBlock1T.py
python3 ./test.py -f import_merge/importBlock2HO.py
python3 ./test.py -f import_merge/importBlock2HPO.py
python3 ./test.py -f import_merge/importBlock2H.py
python3 ./test.py -f import_merge/importBlock2S.py
python3 ./test.py -f import_merge/importBlock2Sub.py
python3 ./test.py -f import_merge/importBlock2TO.py
python3 ./test.py -f import_merge/importBlock2TPO.py
python3 ./test.py -f import_merge/importBlock2T.py
python3 ./test.py -f import_merge/importBlockbetween.py
python3 ./test.py -f import_merge/importCacheFileHO.py
python3 ./test.py -f import_merge/importCacheFileHPO.py
python3 ./test.py -f import_merge/importCacheFileH.py
python3 ./test.py -f import_merge/importCacheFileS.py
python3 ./test.py -f import_merge/importCacheFileSub.py
python3 ./test.py -f import_merge/importCacheFileTO.py
python3 ./test.py -f import_merge/importCacheFileTPO.py
python3 ./test.py -f import_merge/importCacheFileT.py
python3 ./test.py -f import_merge/importDataH2.py
python3 ./test.py -f import_merge/importDataHO2.py
python3 ./test.py -f import_merge/importDataHO.py
python3 ./test.py -f import_merge/importDataHPO.py
python3 ./test.py -f import_merge/importDataLastHO.py
python3 ./test.py -f import_merge/importDataLastHPO.py
python3 ./test.py -f import_merge/importDataLastH.py
python3 ./test.py -f import_merge/importDataLastS.py
python3 ./test.py -f import_merge/importDataLastSub.py
python3 ./test.py -f import_merge/importDataLastTO.py
python3 ./test.py -f import_merge/importDataLastTPO.py
python3 ./test.py -f import_merge/importDataLastT.py
python3 ./test.py -f import_merge/importDataS.py
python3 ./test.py -f import_merge/importDataSub.py
python3 ./test.py -f import_merge/importDataTO.py
python3 ./test.py -f import_merge/importDataTPO.py
python3 ./test.py -f import_merge/importDataT.py
python3 ./test.py -f import_merge/importHeadOverlap.py
python3 ./test.py -f import_merge/importHeadPartOverlap.py
python3 ./test.py -f import_merge/importHead.py
python3 ./test.py -f import_merge/importHORestart.py
python3 ./test.py -f import_merge/importHPORestart.py
python3 ./test.py -f import_merge/importHRestart.py
python3 ./test.py -f import_merge/importLastHO.py
python3 ./test.py -f import_merge/importLastHPO.py
python3 ./test.py -f import_merge/importLastH.py
python3 ./test.py -f import_merge/importLastS.py
python3 ./test.py -f import_merge/importLastSub.py
python3 ./test.py -f import_merge/importLastTO.py
python3 ./test.py -f import_merge/importLastTPO.py
python3 ./test.py -f import_merge/importLastT.py
python3 ./test.py -f import_merge/importSpan.py
python3 ./test.py -f import_merge/importSRestart.py
python3 ./test.py -f import_merge/importSubRestart.py
python3 ./test.py -f import_merge/importTailOverlap.py
python3 ./test.py -f import_merge/importTailPartOverlap.py
python3 ./test.py -f import_merge/importTail.py
python3 ./test.py -f import_merge/importToCommit.py
python3 ./test.py -f import_merge/importTORestart.py
python3 ./test.py -f import_merge/importTPORestart.py
python3 ./test.py -f import_merge/importTRestart.py
python3 ./test.py -f import_merge/importInsertThenImport.py
python3 ./test.py -f import_merge/importCSV.py
python3 ./test.py -f import_merge/import_update_0.py
python3 ./test.py -f import_merge/import_update_1.py
python3 ./test.py -f import_merge/import_update_2.py
python3 ./test.py -f insert/basic.py
python3 ./test.py -f insert/int.py
python3 ./test.py -f insert/float.py
python3 ./test.py -f insert/bigint.py
python3 ./test.py -f insert/bool.py
python3 ./test.py -f insert/double.py
python3 ./test.py -f insert/smallint.py
python3 ./test.py -f insert/tinyint.py
python3 ./test.py -f insert/date.py
python3 ./test.py -f insert/binary.py
python3 ./test.py -f insert/nchar.py
#python3 ./test.py -f insert/nchar-boundary.py
python3 ./test.py -f insert/nchar-unicode.py
python3 ./test.py -f insert/multi.py
python3 ./test.py -f insert/randomNullCommit.py
python3 insert/retentionpolicy.py
python3 ./test.py -f insert/alterTableAndInsert.py
python3 ./test.py -f insert/insertIntoTwoTables.py
python3 ./test.py -f insert/before_1970.py
python3 ./test.py -f insert/special_character_show.py
python3 bug2265.py
python3 ./test.py -f insert/bug3654.py
python3 ./test.py -f insert/insertDynamicColBeforeVal.py
python3 ./test.py -f insert/in_function.py
python3 ./test.py -f insert/modify_column.py
#python3 ./test.py -f insert/line_insert.py
python3 ./test.py -f insert/specialSql.py
python3 ./test.py -f insert/timestamp.py
python3 ./test.py -f insert/metadataUpdate.py
python3 ./test.py -f insert/unsignedInt.py
python3 ./test.py -f insert/unsignedBigint.py
python3 ./test.py -f insert/unsignedSmallint.py
python3 ./test.py -f insert/unsignedTinyint.py
python3 ./test.py -f insert/insertFromCSV.py
python3 ./test.py -f insert/boundary2.py
python3 ./test.py -f insert/insert_locking.py
python3 test.py -f insert/insert_before_use_db.py
python3 ./test.py -f insert/flushwhiledrop.py
python3 ./test.py -f insert/verifyMemToDiskCrash.py
#python3 ./test.py -f insert/schemalessInsert.py
#python3 ./test.py -f insert/openTsdbJsonInsert.py
python3 ./test.py -f insert/openTsdbTelnetLinesInsert.py
python3 ./test.py -f update/merge_commit_data.py
python3 ./test.py -f update/allow_update.py
python3 ./test.py -f update/allow_update-0.py
python3 ./test.py -f update/append_commit_data.py
python3 ./test.py -f update/append_commit_last-0.py
python3 ./test.py -f update/append_commit_last.py
python3 ./test.py -f update/merge_commit_data2.py
python3 ./test.py -f update/merge_commit_data2_update0.py
python3 ./test.py -f update/merge_commit_last-0.py
python3 ./test.py -f update/merge_commit_last.py
python3 ./test.py -f update/update_options.py
python3 ./test.py -f update/merge_commit_data-0.py
python3 ./test.py -f wal/addOldWalTest.py
# python3 ./test.py -f wal/sdbComp.py





