#!/bin/bash
ulimit -c unlimited
#======================p1-start===============

# tools
python3 test.py -f tools/taosdumpTest.py
python3 test.py -f tools/taosdumpTest2.py
#python3 test.py -f tools/taosdemoTest.py
#python3 test.py -f tools/taosdemoTestWithoutMetric.py
#python3 test.py -f tools/taosdemoTestWithJson.py
#======================p1-end===============
#======================p2-start===============
#python3 test.py -f tools/taosdemoTestLimitOffset.py
#python3 test.py -f tools/taosdemoTestTblAlt.py
#python3 test.py -f tools/taosdemoTestSampleData.py
#python3 test.py -f tools/taosdemoTestInterlace.py
#python3 test.py -f tools/taosdemoTestQuery.py
#python3 ./test.py -f tools/taosdemoTestdatatype.py
#======================p2-end===============
#======================p3-start===============

# nano support
#python3 test.py -f tools/taosdemoAllTest/NanoTestCase/taosdemoTestSupportNanoInsert.py
#python3 test.py -f tools/taosdemoAllTest/NanoTestCase/taosdemoTestSupportNanoQuery.py
#python3 test.py -f tools/taosdemoAllTest/NanoTestCase/taosdemoTestSupportNanosubscribe.py
#python3 test.py -f tools/taosdemoAllTest/NanoTestCase/taosdemoTestInsertTime_step.py
python3 test.py -f tools/taosdumpTestNanoSupport.py
#python3 test.py -f tools/taosdemoAllTest/taosdemoTestInsertWithJson.py
#======================p3-end===============
#======================p4-start===============
#python3 test.py -f tools/taosdemoAllTest/taosdemoTestQueryWithJson.py
#python3 test.py -f tools/taosdemoAllTest/taosdemoTestInsertAllType.py
#python3 test.py -f tools/taosdemoAllTest/TD-4985/query-limit-offset.py
#python3 test.py -f tools/taosdemoAllTest/TD-5213/insert4096columns_not_use_taosdemo.py
#python3 test.py -f tools/taosdemoAllTest/TD-5213/insertSigcolumnsNum4096.py
#python3 test.py -f tools/taosdemoAllTest/taosdemoTestInsertWithJsonStmt.py
#python3 test.py -f tools/taosdemoAllTest/taosdemoTestInsertWithJsonSml.py
#python3 test.py -f tools/taosdemoAllTest/taosdemoTestInsertShell.py
#======================p4-end===============
#======================p5-start===============

#======================p5-end===============










