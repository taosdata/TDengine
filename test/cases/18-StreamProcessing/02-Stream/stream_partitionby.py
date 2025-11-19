import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, etool
from datetime import datetime
from datetime import date


class Test_STREAM_PartitionBy:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_partition_by(self):
        """Stream nevados

        Refer: NULL

        Catalog:
            - Streams:PartitionBy

        Since: v3.3.7.0

        Labels: common,ci

        Jira: https://jira.taosdata.com:18080/browse/TD-37059

        History:
            - 2025-09-04 Mark Wang Created

        """

        #
        #  main test
        #

        # env
        tdStream.createSnode()

        # create streams
        self.createtables()
        
        # create streams
        self.createStreams()

        # check stream status
        self.checkStreamStatus()

        # insert trigger data
        self.writeTriggerData()

        # verify results
        self.verifyResults()


    #
    # ---------------------   main flow frame    ----------------------
    #

    # 
    # 1. create vtables
    #
    def createtables(self):
        sqls = [
            "create database test;",
            "use test;",
            "create table stream_trigger_st (ts timestamp, id int) tags (gid nchar(32), t int);",
            "create table stream_trigger_ct0 using stream_trigger_st tags ('1.2.3', 1);",
            "create table stream_trigger_ct1 using stream_trigger_st tags ('a.bc.d', 2);",
        ]
        tdSql.executes(sqls)
        tdLog.info(f"create table successfully.")
        

    # 
    # 2. create streams
    #
    def createStreams(self):

        sqls = [
            "create stream test.s1 session (ts, 1s) from test.stream_trigger_st partition by substring_index(gid,'.',2) stream_options(fill_history) into test.stream_out_str as select _twstart, avg(id) from %%trows;",
            "create stream test.s2 session (ts, 1s) from test.stream_trigger_st partition by t+12 stream_options(fill_history) into test.stream_out_int as select _twstart, avg(id) from %%trows;",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create streams successfully.")

    # 
    # 3. wait stream ready
    #
    def checkStreamStatus(self):
        print("wait stream ready ...")
        tdStream.checkStreamStatus()
        tdLog.info(f"check stream status successfully.")

    # 
    # 4. write trigger data
    #
    def writeTriggerData(self):
        sqls = [
            "insert into test.stream_trigger_ct0 values ('2025-01-01 00:00:00', 0), ('2025-01-01 00:00:11', 1), ('2025-01-01 00:00:22', 2);",
            "insert into test.stream_trigger_ct1 values ('2025-01-01 00:00:00', 0), ('2025-01-01 00:00:11', 1), ('2025-01-01 00:00:22', 2);",
        ]
        tdSql.executes(sqls)
        tdLog.info(f"create table successfully.")


    # 
    # 5. verify results
    #
    def verifyResults(self):
        # sleep
        time.sleep(5)

        result_sql = f"select * from test.stream_out_str where `substring_index(gid,'.',2)`='1.2' "

        tdSql.query(result_sql)
        tdSql.checkRows(2)
        
        result_sql = f"select * from test.stream_out_int where `t+12`=13"

        tdSql.query(result_sql)
        tdSql.checkRows(2)

        tdLog.info(f"verify stream ................................. successfully.")