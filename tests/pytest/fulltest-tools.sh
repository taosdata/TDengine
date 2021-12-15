#!/bin/bash
ulimit -c unlimited
#======================p1-start===============
#======================p1-end===============

# tools
python3 test.py -f tools/taosdumpTest.py
python3 test.py -f tools/taosdumpTest2.py

python3 test.py -f tools/taosdemoTest.py
python3 test.py -f tools/taosdemoTestWithoutMetric.py
python3 test.py -f tools/taosdemoTestWithJson.py
python3 test.py -f tools/taosdemoTestLimitOffset.py
python3 test.py -f tools/taosdemoTestTblAlt.py
python3 test.py -f tools/taosdemoTestSampleData.py
python3 test.py -f tools/taosdemoTestInterlace.py
# python3 test.py -f tools/taosdemoTestQuery.py
python3 ./test.py -f tools/taosdemoTestdatatype.py

# nano support
python3 test.py -f tools/taosdemoAllTest/NanoTestCase/taosdemoTestSupportNanoInsert.py
python3 test.py -f tools/taosdemoAllTest/NanoTestCase/taosdemoTestSupportNanoQuery.py
python3 test.py -f tools/taosdemoAllTest/NanoTestCase/taosdemoTestSupportNanosubscribe.py
python3 test.py -f tools/taosdemoAllTest/NanoTestCase/taosdemoTestInsertTime_step.py
python3 test.py -f tools/taosdumpTestNanoSupport.py
python3 test.py -f tools/taosdemoAllTest/taosdemoTestInsertWithJson.py
python3 test.py -f tools/taosdemoAllTest/taosdemoTestQueryWithJson.py
python3 test.py -f tools/taosdemoAllTest/taosdemoTestInsertAllType.py
python3 test.py -f tools/taosdemoAllTest/TD-4985/query-limit-offset.py
python3 test.py -f tools/taosdemoAllTest/TD-5213/insert4096columns_not_use_taosdemo.py
python3 test.py -f tools/taosdemoAllTest/TD-5213/insertSigcolumnsNum4096.py
#python3 test.py -f tools/taosdemoAllTest/TD-10539/create_taosdemo.py
python3 test.py -f tools/taosdemoAllTest/taosdemoTestInsertWithJsonStmt.py











