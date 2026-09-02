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
            "create stream test.s1 session (ts, 1s) from test.stream_trigger_st partition by substring_index(gid,'.',2) stream_options(fill_history) into test.stream_out_str as select _twstart, avg(id), %%1 from %%trows;",
            "create stream test.s2 session (ts, 1s) from test.stream_trigger_st partition by t+12 stream_options(fill_history) into test.stream_out_int OUTPUT_SUBTABLE(CONCAT('hm_', cast(t+12 as varchar(32)))) as select _twstart, avg(id) from %%trows;",
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
        # wait until both partitions of s1 have produced their session windows.
        # `_twstart` and the `%%1` placeholder column both need backtick-quoting
        # to be referenced by name in an ordinary SELECT.
        sql = (
            "select `_twstart`, `avg(id)`, `%%1`, `substring_index(gid,'.',2)` "
            "from test.stream_out_str "
            "order by `substring_index(gid,'.',2)`, `_twstart`"
        )
        tdSql.checkRowsLoop(4, sql, loopCount=100, waitTime=0.5)

        # verify s1 data row by row, column by column: avg(id) matches the
        # single-point session window at each _twstart, and the %%1 placeholder
        # resolves to the same value as the partition-by tag expr
        tdSql.checkData(0, 0, "2025-01-01 00:00:00.000")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, "1.2")
        tdSql.checkData(0, 3, "1.2")

        tdSql.checkData(1, 0, "2025-01-01 00:00:11.000")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, "1.2")
        tdSql.checkData(1, 3, "1.2")

        tdSql.checkData(2, 0, "2025-01-01 00:00:00.000")
        tdSql.checkData(2, 1, 0)
        tdSql.checkData(2, 2, "a.bc")
        tdSql.checkData(2, 3, "a.bc")

        tdSql.checkData(3, 0, "2025-01-01 00:00:11.000")
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, "a.bc")
        tdSql.checkData(3, 3, "a.bc")

        # wait until both partitions of s2 have produced their session windows
        sql = (
            "select `_twstart`, `avg(id)`, `t+12`, tbname "
            "from test.stream_out_int "
            "order by tbname, `_twstart`"
        )
        tdSql.checkRowsLoop(4, sql, loopCount=100, waitTime=0.5)

        # verify s2 data row by row, column by column: avg(id) matches the
        # single-point session window at each _twstart, and OUTPUT_SUBTABLE
        # resolves tbname to hm_<t+12>
        tdSql.checkData(0, 0, "2025-01-01 00:00:00.000")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 13)
        tdSql.checkData(0, 3, "hm_13")

        tdSql.checkData(1, 0, "2025-01-01 00:00:11.000")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 13)
        tdSql.checkData(1, 3, "hm_13")

        tdSql.checkData(2, 0, "2025-01-01 00:00:00.000")
        tdSql.checkData(2, 1, 0)
        tdSql.checkData(2, 2, 14)
        tdSql.checkData(2, 3, "hm_14")

        tdSql.checkData(3, 0, "2025-01-01 00:00:11.000")
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 14)
        tdSql.checkData(3, 3, "hm_14")

        tdLog.info(f"verify stream ................................. successfully.")