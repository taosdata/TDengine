import os
import platform
import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tmqCom


class TestTmpSnapshot1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tmq_snapshot1(self):
        """1 consumer: from snapshot

        test scenario, please refer to https://jira.taosdata.com:18090/pages/viewpage.action?pageId=135120406, firstly insert data, then start consume
        1. basic1.sim: vgroups=1, one topic for one consumer
        2. basic2.sim: vgroups=1, multi topics for one consumer
        3. basic3.sim: vgroups=4, one topic for one consumer
        4. basic4.sim: vgroups=4, multi topics for one consumer
        5. snapshot1.sim: vgroups=1, multi topics for one consumer, consume from snapshot


        Catalog:
            - Subscribe

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-9 Simon Guan migrated from tsim/tmq/snapshot1.sim

        """

        #### test scenario, please refer to https://jira.taosdata.com:18090/pages/viewpage.action?pageId=135120406
        # basic1Of2Cons.sim: vgroups=1, one topic for 2 consumers, firstly insert data, then start consume. Include six topics
        # basic2Of2ConsOverlap.sim: vgroups=1, multi topics for 2 consumers, firstly insert data, then start consume. Include six topics
        # basic3Of2Cons.sim: vgroups=4, one topic for 2 consumers, firstly insert data, then start consume. Include six topics
        # basic4Of2Cons.sim: vgroups=4, multi topics for 2 consumers, firstly insert data, then start consume. Include six topics

        # notes1: Scalar function: ABS/ACOS/ASIN/ATAN/CEIL/COS/FLOOR/LOG/POW/ROUND/SIN/SQRT/TAN
        # The above use cases are combined with where filter conditions, such as: where ts > "2017-08-12 18:25:58.128Z" and sin(a) > 0.5;
        #
        # notes2: not support aggregate functions(such as sum/count/min/max) and time-windows(interval).
        #

        self.prepareBasicEnv_1vgrp()

        # ---- global parameters start ----#
        dbName = "db"
        vgroups = 1
        stbPrefix = "stb"
        ctbPrefix = "ctb"
        ntbPrefix = "ntb"
        stbNum = 1
        ctbNum = 10
        ntbNum = 10
        rowsPerCtb = 10
        tstart = 1640966400000  # 2022-01-01 00:00:"00+000"
        # ---- global parameters end ----#

        pullDelay = 2
        ifcheckdata = 1
        ifmanualcommit = 1
        showMsg = 1
        showRow = 0

        tdSql.execute(f"use {dbName}")

        tdLog.info(f"== create topics from super table")
        tdSql.execute(f"create topic topic_stb_column as select ts, c3 from stb")
        tdSql.execute(f"create topic topic_stb_all as select ts, c1, c2, c3 from stb")
        tdSql.execute(
            f"create topic topic_stb_function as select ts, abs(c1), sin(c2) from stb"
        )

        tdLog.info(f"== create topics from child table")
        tdSql.execute(f"create topic topic_ctb_column as select ts, c3 from ctb0")
        tdSql.execute(f"create topic topic_ctb_all as select * from ctb0")
        tdSql.execute(
            f"create topic topic_ctb_function as select ts, abs(c1), sin(c2) from ctb0"
        )

        tdLog.info(f"== create topics from normal table")
        tdSql.execute(f"create topic topic_ntb_column as select ts, c3 from ntb0")
        tdSql.execute(f"create topic topic_ntb_all as select * from ntb0")
        tdSql.execute(
            f"create topic topic_ntb_function as select ts, abs(c1), sin(c2) from ntb0"
        )

        tdSql.query("show topics")
        tdSql.checkRows(9)

        #'group.id:cgrp1,enable.auto.commit:false,auto.commit.interval.ms:6000,auto.offset.reset:earliest'
        keyList = "'group.id:cgrp1,enable.auto.commit:false,auto.commit.interval.ms:6000,auto.offset.reset:earliest'"
        tdLog.info(f"key list:  {keyList}")

        topicNum = 2

        # =============================== start consume =============================#
        cdbName = "cdb0"
        tdSql.execute(f"create database {cdbName} vgroups 1")
        tdSql.execute(f"use {cdbName}")

        tdLog.info(f"== create consume info table and consume result table for stb")
        tdSql.execute(
            f"create table consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"
        )
        tdSql.execute(
            f"create table consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"
        )

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdLog.info(f"================ test consume from stb")
        tdLog.info(
            f"== overlap toipcs: topic_stb_column + topic_stb_all, topic_stb_function + topic_stb_all"
        )
        topicList = "'topic_stb_column,topic_stb_all'"

        consumerId = 0
        totalMsgOfOneTopic = ctbNum * rowsPerCtb
        totalMsgOfStb = totalMsgOfOneTopic * topicNum
        expectmsgcnt = 1000000
        tdSql.execute(
            f"insert into consumeinfo values (now , {consumerId} , {topicList} , {keyList} , {totalMsgOfStb} , {ifcheckdata} , {ifmanualcommit} )"
        )
        topicList = "'topic_stb_all,topic_stb_function'"
        consumerId = 1
        tdSql.execute(
            f"insert into consumeinfo values (now+1s , {consumerId} , {topicList} , {keyList} , {totalMsgOfStb} , {ifcheckdata} , {ifmanualcommit} )"
        )

        tdLog.info(f"== start consumer to pull msgs from stb")
        tmqCom.startTmqSimProcess(pullDelay, dbName, showMsg, showRow, cdbName,valgrind=0,alias=0,snapshot=1)



        tdLog.info(f"== check consume result")
        while True:
            tdSql.query(f"select * from consumeresult")
            tdLog.info(f"==> rows: {tdSql.getRows()})")

            if tdSql.getRows() == 2:
                tdSql.checkAssert(tdSql.getData(0, 1) + tdSql.getData(1, 1) == 1)
                tdSql.checkAssert(tdSql.getData(0, 3) >= totalMsgOfOneTopic)
                tdSql.checkAssert(tdSql.getData(0, 3) <= totalMsgOfStb)
                tdSql.checkAssert(tdSql.getData(1, 3) >= totalMsgOfOneTopic)
                tdSql.checkAssert(tdSql.getData(1, 3) <= totalMsgOfStb)

                totalMsgCons = totalMsgOfOneTopic + totalMsgOfStb
                sumOfRows = tdSql.getData(0, 3) + tdSql.getData(1, 3)
                tdSql.checkAssert(totalMsgCons == sumOfRows)
                tdSql.execute(f"drop database {cdbName}")
                break
            time.sleep(1)

        #######################################################################################
        # clear consume info and consume result
        # run tsim/tmq/clearConsume.sim
        # because drop table function no stable, so by create new db for consume info and result. Modify it later
        cdbName = "cdb1"
        tdSql.execute(f"create database {cdbName} vgroups 1")
        tdSql.execute(f"use {cdbName}")

        tdLog.info(f"== create consume info table and consume result table for ctb")
        tdSql.execute(
            f"create table consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"
        )
        tdSql.execute(
            f"create table consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"
        )

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        #######################################################################################

        tdLog.info(f"================ test consume from ctb")
        tdLog.info(
            f"== overlap toipcs: topic_ctb_column + topic_ctb_all, topic_ctb_function + topic_ctb_all"
        )
        topicList = "'topic_ctb_column,topic_ctb_all'"
        consumerId = 0

        totalMsgOfOneTopic = rowsPerCtb
        totalMsgOfCtb = totalMsgOfOneTopic * topicNum
        expectmsgcnt = 1000000
        tdSql.execute(
            f"insert into consumeinfo values (now , {consumerId} , {topicList} , {keyList} , {totalMsgOfCtb} , {ifcheckdata} , {ifmanualcommit} )"
        )
        topicList = "'topic_ctb_function,topic_ctb_all'"
        consumerId = 1
        tdSql.execute(
            f"insert into consumeinfo values (now+1s , {consumerId} , {topicList} , {keyList} , {totalMsgOfCtb} , {ifcheckdata} , {ifmanualcommit} )"
        )

        tdLog.info(f"== start consumer to pull msgs from ctb")
        tmqCom.startTmqSimProcess(pullDelay, dbName, showMsg, showRow, cdbName,valgrind=0,alias=0,snapshot=1)


        tdLog.info(f"== check consume result")
        while True:
            tdSql.query(f"select * from consumeresult")
            tdLog.info(f"==> rows: {tdSql.getRows()})")

            if tdSql.getRows() == 2:
                tdSql.checkAssert(tdSql.getData(0, 1) + tdSql.getData(1, 1) == 1)
                tdSql.checkAssert(
                    tdSql.getData(0, 3) + tdSql.getData(1, 3)
                    == totalMsgOfOneTopic + totalMsgOfCtb
                )
                tdSql.execute(f"drop database {cdbName}")
                break
            time.sleep(1)

        #######################################################################################
        # clear consume info and consume result
        # run tsim/tmq/clearConsume.sim
        # because drop table function no stable, so by create new db for consume info and result. Modify it later
        cdbName = "cdb2"
        tdSql.execute(f"create database {cdbName} vgroups 1")
        tdSql.execute(f"use {cdbName}")

        tdLog.info(f"== create consume info table and consume result table for ntb")
        tdSql.execute(
            f"create table consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"
        )
        tdSql.execute(
            f"create table consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"
        )

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        #######################################################################################

        tdLog.info(f"================ test consume from ntb")
        tdLog.info(
            f"== overlap toipcs: topic_ntb_column + topic_ntb_all, topic_ntb_function + topic_ntb_all"
        )
        topicList = "'topic_ntb_column,topic_ntb_all'"

        consumerId = 0
        totalMsgOfOneTopic = rowsPerCtb
        totalMsgOfNtb = totalMsgOfOneTopic * topicNum
        expectmsgcnt = 1000000
        tdSql.execute(
            f"insert into consumeinfo values (now , {consumerId} , {topicList} , {keyList} , {totalMsgOfNtb} , {ifcheckdata} , {ifmanualcommit} )"
        )

        topicList = "'topic_ntb_function,topic_ntb_all'"
        consumerId = 1
        tdSql.execute(
            f"insert into consumeinfo values (now+1s , {consumerId} , {topicList} , {keyList} , {totalMsgOfNtb} , {ifcheckdata} , {ifmanualcommit} )"
        )

        tdLog.info(f"== start consumer to pull msgs from ntb")
        tmqCom.startTmqSimProcess(pullDelay, dbName, showMsg, showRow, cdbName,valgrind=0,alias=0,snapshot=1)

        tdLog.info(f"== check consume result from ntb")
        while True:
            tdSql.query(f"select * from consumeresult")
            tdLog.info(f"==> rows: {tdSql.getRows()})")

            if tdSql.getRows() == 2:
                tdSql.checkAssert(tdSql.getData(0, 1) + tdSql.getData(1, 1) == 1)
                tdSql.checkAssert(
                    tdSql.getData(0, 3) + tdSql.getData(1, 3)
                    == totalMsgOfOneTopic + totalMsgOfNtb
                )
                tdSql.execute(f"drop database {cdbName}")
                break
            time.sleep(1)

        tdSql.query(f"select * from performance_schema.perf_consumers")
        tdSql.checkRows(0)
        tmqCom.stopTmqSimProcess("tmq_sim")

    def prepareBasicEnv_1vgrp(self):

        # ---- global parameters start ----#
        dbName = "db"
        vgroups = 1
        stbPrefix = "stb"
        ctbPrefix = "ctb"
        ntbPrefix = "ntb"
        stbNum = 1
        ctbNum = 10
        ntbNum = 10
        rowsPerCtb = 10
        tstart = 1640966400000  # 2022-01-01 00:00:"00+000"
        # ---- global parameters end ----#

        tdLog.info(f"create database {dbName} vgroups {vgroups}")
        tdSql.execute(f"create database {dbName} vgroups {vgroups}")

        # wait database ready
        clusterComCheck.checkDbReady(dbName)
        tdSql.execute(f"use {dbName}")

        tdLog.info(f"create consume info table and consume result table")
        tdSql.execute(
            f"create table consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"
        )
        tdSql.execute(
            f"create table consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"
        )

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table {stbPrefix} (ts timestamp, c1 int, c2 float, c3 binary(16)) tags (t1 int)"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"create child table, normal table and insert data")
        i = 0

        while i < ctbNum:
            ctb = ctbPrefix + str(i)
            ntb = ntbPrefix + str(i)

            tdSql.execute(f"create table {ctb} using {stbPrefix} tags( {i} )")
            tdSql.execute(
                f"create table {ntb} (ts timestamp, c1 int, c2 float, c3 binary(16))"
            )

            x = 0
            while x < rowsPerCtb:
                binary = "'binary-" + str(i) + "'"
                tdSql.execute(
                    f"insert into {ctb} values ({tstart} , {i} , {x} , {binary} )"
                )
                tdSql.execute(
                    f"insert into {ntb} values ({tstart} , {i} , {x} , {binary} )"
                )
                tstart = tstart + 1
                x = x + 1

            i = i + 1
            tstart = 1640966400000
