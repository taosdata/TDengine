import os
import time
import platform
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tmqCom


class TestTmpBasic1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tmq_basic1(self):
        """1 consumer: vgroups=1 topics=1

        test scenario, please refer to https://jira.taosdata.com:18090/pages/viewpage.action?pageId=135120406, firstly insert data, then start consume
        1. basic1.sim: vgroups=1, one topic for one consumer
        2. basic2.sim: vgroups=1, multi topics for one consumer
        3. basic3.sim: vgroups=4, one topic for one consumer
        4. basic4.sim: vgroups=4, multi topics for one consumer
        5. snapshot1.sim: vgroups=1, multi topics for one consumer, consume from snapshot

        notes1: Scalar function: ABS/ACOS/ASIN/ATAN/CEIL/COS/FLOOR/LOG/POW/ROUND/SIN/SQRT/TAN
        The above use cases are combined with where filter conditions, such as: where ts > "2017-08-12 18:25:58.128Z" and sin(a) > 0.5;
        
        notes2: not support aggregate functions(such as sum/count/min/max) and time-windows(interval).

        Catalog:
            - Subscribe

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-9 Simon Guan migrated from tsim/tmq/basic1.sim

        """

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

        pullDelay = 3
        ifcheckdata = 1
        ifmanualcommit = 1
        showMsg = 1
        showRow = 0

        tdSql.execute(f"use {dbName}")

        tdLog.info(f"create topics from super table")
        tdSql.execute(f"create topic topic_stb_column as select ts, c3 from stb")
        tdSql.execute(f"create topic topic_stb_all as select ts, c1, c2, c3 from stb")
        tdSql.execute(
            f"create topic topic_stb_function as select ts, abs(c1), sin(c2) from stb"
        )

        tdLog.info(f"create topics from child table")
        tdSql.execute(f"create topic topic_ctb_column as select ts, c3 from ctb0")
        tdSql.execute(f"create topic topic_ctb_all as select * from ctb0")
        tdSql.execute(
            f"create topic topic_ctb_function as select ts, abs(c1), sin(c2) from ctb0"
        )

        tdLog.info(f"create topics from normal table")
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

        cdb_index = 0

        tdLog.info(f"test consume from stb")
        #######################################################################################
        # clear consume info and consume result
        # run tsim/tmq/clearConsume.sim
        # because drop table function no stable, so by create new db for consume info and result. Modify it later
        loop_cnt = 0
        while True:
            cdb_index = cdb_index + 1
            cdbName = "cdb" + str(cdb_index)
            tdSql.execute(f"create database {cdbName} vgroups 1")
            tdSql.execute(f"use {cdbName}")

            tdLog.info(f"create consume info table and consume result table")
            tdSql.execute(
                f"create table consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"
            )
            tdSql.execute(
                f"create table consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"
            )

            tdSql.query(f"show tables")
            tdSql.checkRows(2)

            if loop_cnt == 0:
                tdLog.info(f"scenario 1: topic_stb_column")
                topicList = "'topic_stb_column'"
            elif loop_cnt == 1:
                tdLog.info(f"scenario 2: topic_stb_all")
                topicList = "'topic_stb_all'"
            elif loop_cnt == 2:
                tdLog.info(f"scenario 3: topic_stb_function")
                topicList = "'topic_stb_function'"
            else:
                break

            consumerId = 0
            totalMsgOfStb = ctbNum * rowsPerCtb
            expectmsgcnt = 1
            tdSql.execute(
                f"insert into consumeinfo values (now , {consumerId} , {topicList} , {keyList} , {totalMsgOfStb} , {ifcheckdata} , {ifmanualcommit} )"
            )

            tdLog.info(f"start consumer to pull msgs from stb")
            tmqCom.startTmqSimProcess(pullDelay, dbName, showMsg, showRow, cdbName)

            tdLog.info(f"check consume result")

            while True:
                tdSql.query(f"select * from consumeresult")
                tdLog.info(f"==> rows: {tdSql.getRows()})")

                if tdSql.getRows() == 1:
                    tdSql.checkData(0, 1, consumerId)
                    # tdSql.checkData(0, 2, expectmsgcnt)
                    tdSql.checkData(0, 3, totalMsgOfStb)
                    tdSql.execute(f"drop database {cdbName}")
                    break
                time.sleep(1)

            loop_cnt = loop_cnt + 1

        tdLog.info(f"==============test consume from ctb")
        #######################################################################################
        # clear consume info and consume result
        # run tsim/tmq/clearConsume.sim
        # because drop table function no stable, so by create new db for consume info and result. Modify it later
        loop_cnt = 0
        while True:
            cdb_index = cdb_index + 1
            cdbName = "cdb" + str(cdb_index)
            tdSql.execute(f"create database {cdbName} vgroups 1")
            tdSql.execute(f"use {cdbName}")

            tdLog.info(f"create consume info table and consume result table")
            tdSql.execute(
                f"create table consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"
            )
            tdSql.execute(
                f"create table consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"
            )

            tdSql.query(f"show tables")
            tdSql.checkRows(2)

            #######################################################################################

            if loop_cnt == 0:
                tdLog.info(f"scenario 1: topic_ctb_column")
                topicList = "'topic_ctb_column'"
            elif loop_cnt == 1:
                tdLog.info(f"scenario 2: topic_ctb_all")
                topicList = "'topic_ctb_all'"
            elif loop_cnt == 2:
                tdLog.info(f"scenario 3: topic_ctb_function")
                topicList = "'topic_ctb_function'"
            else:
                break

            consumerId = 0
            totalMsgOfCtb = rowsPerCtb
            expectmsgcnt = 1
            tdSql.execute(
                f"insert into consumeinfo values (now , {consumerId} , {topicList} , {keyList} , {totalMsgOfCtb} , {ifcheckdata} , {ifmanualcommit} )"
            )

            tdLog.info(f"start consumer to pull msgs from ctb")
            tmqCom.startTmqSimProcess(pullDelay, dbName, showMsg, showRow, cdbName)

            tdLog.info(f"check consume result")
            while True:
                tdSql.query(f"select * from consumeresult")
                tdLog.info(f"==> rows: {tdSql.getRows()})")

                if tdSql.getRows() == 1:
                    tdSql.checkData(0, 1, consumerId)
                    # tdSql.checkData(0, 2, expectmsgcnt)
                    tdSql.checkData(0, 3, totalMsgOfCtb)
                    tdSql.execute(f"drop database {cdbName}")
                    break
                time.sleep(1)

            loop_cnt = loop_cnt + 1

        tdLog.info(f"==============test consume from ntb")
        #######################################################################################
        # clear consume info and consume result
        # run tsim/tmq/clearConsume.sim
        # because drop table function no stable, so by create new db for consume info and result. Modify it later
        loop_cnt = 0
        while True:
            cdb_index = cdb_index + 1
            cdbName = "cdb" + str(cdb_index)
            tdSql.execute(f"create database {cdbName} vgroups 1")
            tdSql.execute(f"use {cdbName}")

            tdLog.info(f"create consume info table and consume result table")
            tdSql.execute(
                f"create table consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"
            )
            tdSql.execute(
                f"create table consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"
            )

            tdSql.query(f"show tables")
            tdSql.checkRows(2)

            #######################################################################################

            if loop_cnt == 0:
                tdLog.info(f"scenario 1: topic_ntb_column")
                topicList = "'topic_ntb_column'"
            elif loop_cnt == 1:
                tdLog.info(f"scenario 2: topic_ntb_all")
                topicList = "'topic_ntb_all'"
            elif loop_cnt == 2:
                tdLog.info(f"scenario 3: topic_ntb_function")
                topicList = "'topic_ntb_function'"
            else:
                break

            consumerId = 0
            totalMsgOfNtb = rowsPerCtb
            expectmsgcnt = 1
            tdSql.execute(
                f"insert into consumeinfo values (now , {consumerId} , {topicList} , {keyList} , {totalMsgOfNtb} , {ifcheckdata} , {ifmanualcommit} )"
            )

            tdLog.info(f"start consumer to pull msgs from ntb")
            tmqCom.startTmqSimProcess(pullDelay, dbName, showMsg, showRow, cdbName)

            tdLog.info(f"check consume result from ntb")
            while True:
                tdSql.query(f"select * from consumeresult")
                tdLog.info(f"==> rows: {tdSql.getRows()})")

                if tdSql.getRows() == 1:
                    tdSql.checkData(0, 1, consumerId)
                    # tdSql.checkData(0, 2, expectmsgcnt)
                    tdSql.checkData(0, 3, totalMsgOfNtb)
                    tdSql.execute(f"drop database {cdbName}")
                    break
                time.sleep(1)

            loop_cnt = loop_cnt + 1

        # ------ not need stop consumer, because it exit after pull msg overthan expect msg
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
