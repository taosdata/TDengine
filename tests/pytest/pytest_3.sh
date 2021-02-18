#!/bin/bash
ulimit -c unlimited


python3 ./test.py -f insert/randomNullCommit.py

# user
python3 ./test.py -f user/user_create.py
python3 ./test.py -f user/pass_len.py

# stable
python3 ./test.py -f stable/query_after_reset.py

#query
python3 ./test.py -f query/filter.py
python3 ./test.py -f query/filterCombo.py
python3 ./test.py -f query/queryNormal.py
python3 ./test.py -f query/queryError.py
python3 ./test.py -f query/filterAllIntTypes.py
python3 ./test.py -f query/filterFloatAndDouble.py
python3 ./test.py -f query/filterOtherTypes.py
python3 ./test.py -f query/querySort.py
python3 ./test.py -f query/queryJoin.py
python3 ./test.py -f query/select_last_crash.py
python3 ./test.py -f query/queryNullValueTest.py
python3 ./test.py -f query/queryInsertValue.py
python3 ./test.py -f query/queryConnection.py
python3 ./test.py -f query/queryCountCSVData.py
python3 ./test.py -f query/natualInterval.py
python3 ./test.py -f query/bug1471.py
#python3 ./test.py -f query/dataLossTest.py
python3 ./test.py -f query/bug1874.py
python3 ./test.py -f query/bug1875.py
python3 ./test.py -f query/bug1876.py
python3 ./test.py -f query/bug2218.py
python3 ./test.py -f query/bug2117.py
python3 ./test.py -f query/bug2118.py
python3 ./test.py -f query/bug2143.py
python3 ./test.py -f query/sliding.py
python3 ./test.py -f query/unionAllTest.py
python3 ./test.py -f query/bug2281.py
python3 ./test.py -f query/bug2119.py
python3 ./test.py -f query/isNullTest.py
python3 ./test.py -f query/queryWithTaosdKilled.py
python3 ./test.py -f query/floatCompare.py

#stream
python3 ./test.py -f stream/metric_1.py
python3 ./test.py -f stream/metric_n.py
python3 ./test.py -f stream/new.py
python3 ./test.py -f stream/stream1.py
python3 ./test.py -f stream/stream2.py
#python3 ./test.py -f stream/parser.py
python3 ./test.py -f stream/history.py
python3 ./test.py -f stream/sys.py
python3 ./test.py -f stream/table_1.py
python3 ./test.py -f stream/table_n.py

#alter table
python3 ./test.py -f alter/alter_table_crash.py

# client
python3 ./test.py -f client/client.py
python3 ./test.py -f client/version.py
python3 ./test.py -f client/alterDatabase.py
python3 ./test.py -f client/noConnectionErrorTest.py

# Misc
python3 testCompress.py
python3 testNoCompress.py
python3 testMinTablesPerVnode.py


python3 queryCount.py
python3 ./test.py -f query/queryGroupbyWithInterval.py
python3 client/twoClients.py
python3 test.py -f query/queryInterval.py
python3 test.py -f query/queryFillTest.py

# tools
python3 test.py -f tools/taosdemoTest.py
python3 test.py -f tools/taosdumpTest.py
python3 test.py -f tools/lowaTest.py
#python3 test.py -f tools/taosdemoTest2.py

# subscribe
python3 test.py -f subscribe/singlemeter.py
#python3 test.py -f subscribe/stability.py  
python3 test.py -f subscribe/supertable.py

