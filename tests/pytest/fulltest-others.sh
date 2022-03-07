#!/bin/bash
ulimit -c unlimited
#======================p1-start===============
#python3 ./test.py -f dbmgmt/database-name-boundary.py
python3 test.py -f dbmgmt/nanoSecondCheck.py
#
python3 ./test.py -f tsdb/tsdbComp.py
# user
python3 ./test.py -f user/user_create.py
python3 ./test.py -f user/pass_len.py
#======================p1-end===============
#======================p2-start===============
# perfbenchmark
python3 ./test.py -f perfbenchmark/bug3433.py
#python3 ./test.py -f perfbenchmark/bug3589.py
#python3 ./test.py -f perfbenchmark/taosdemoInsert.py
#alter table
python3 ./test.py -f alter/alter_table_crash.py
python3 ./test.py -f alter/alterTabAddTagWithNULL.py
python3 ./test.py -f alter/alterTimestampColDataProcess.py
#======================p2-end===============
#======================p3-start===============
python3 ./test.py -f alter/alter_table.py
python3 ./test.py -f alter/alter_debugFlag.py
python3 ./test.py -f alter/alter_keep.py
python3 ./test.py -f alter/alter_cacheLastRow.py
python3 ./test.py -f alter/alter_create_exception.py
python3 ./test.py -f alter/alterColMultiTimes.py
#======================p3-end===============
#======================p4-start===============
python3 ./test.py -f account/account_create.py
# client
python3 ./test.py -f client/client.py
python3 ./test.py -f client/version.py
python3 ./test.py -f client/alterDatabase.py
python3 ./test.py -f client/noConnectionErrorTest.py
python3 ./test.py -f client/taoshellCheckCase.py
# python3 ./test.py -f client/change_time_1_1.py
# python3 ./test.py -f client/change_time_1_2.py
python3 client/twoClients.py
python3 testMinTablesPerVnode.py
# topic
python3 ./test.py -f topic/topicQuery.py
#======================p4-end===============
#======================p5-start===============
python3 ./test.py -f ../system-test/0-management/1-stable/create_col_tag.py
python3 ./test.py -f ../develop-test/0-management/3-tag/json_tag.py
#======================p5-end===============