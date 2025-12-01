import time
import math
import random
import os
from new_test_framework.utils import tdLog, tdSql, tdStream, etool, sc, eutil
from datetime import datetime
from datetime import date


class Test_IDMP_Meters:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_usecase_em(self):
        """IDMP: meters scenario

        1. IDMP trigger table is super vtable
        2. IDMP trigger table is vtable
        3. IDMP trigger mode: period, sliding, event, session, interval, count, state
        4. IDMP trigger group: partition by tbname, tag column, tbname and columns
        5. IDMP trigger condition: on window open, on window close, on event
        6. IDMP trigger action: notify, calc, calc and notify
        7. IDMP notify on: window open, window close, both open and close
        8. IDMP output table: super table , normal table
        9. IDMP stream Options: IGNORE_DISORDER, CALC_NOTIF_ONLY, LOW_LATENCY_CALC,PRE_FILTER, FORCE_OUTPUT, IGNORE_NODATA_TRIGGER

        Refer: https://taosdata.feishu.cn/wiki/Zkb2wNkHDihARVkGHYEcbNhmnxb

        Catalog:
            - Streams:UseCases

        Since: v3.3.7.0

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-7-10 Alex Duan Created
        """

        #
        #  main test
        #

        tdSql.execute(f"alter all dnodes 'debugflag 131';")

        # env
        tdStream.createSnode()

        # prepare data
        self.prepare()

        # create vtables
        self.createVtables()

        # create streams
        self.createStreams()

        # check stream status
        self.checkStreamStatus()

        # insert trigger data
        self.writeTriggerData()

        # check errors
        self.checkErrors()

        # verify results
        self.verifyResults()

        # write trigger data again
        self.writeTriggerDataAgain()

        # verify results
        self.verifyResultsAgain()

        """
        # restart dnode
        self.restartDnode()

        # verify results after restart
        self.verifyResults()

        # write trigger data after restart
        self.writeTriggerAfterRestart()

        # verify results after restart
        self.verifyResultsAfterRestart()
        """

    #
    # ---------------------   main flow frame    ----------------------
    #

    #
    # prepare data
    #
    def prepare(self):
        # create database out
        tdSql.execute("create database out")

        # name
        self.db = "assert01"
        self.vdb = "tdasset"
        self.stb = "electricity_meters"
        self.start = 1752563000000
        self.start_current = 10
        self.start_voltage = 260

        self.start2 = 1752574200000
        self.notifyFailed = (
            "failed to get stream notify handle of ws://idmp:6042/recv/?key="
        )

        # import data
        etool.taosdump(
            f"-i {os.path.join(os.path.dirname(__file__), 'meters_data', 'data')}"
        )
        tdLog.info(f"import data to db={self.db} successfully.")

    #
    # 1. create vtables
    #
    def createVtables(self):
        sqls = [
            "create database tdasset;",
            "use tdasset;",
            "CREATE STABLE `vst_智能电表_1` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `电流` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `电压` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `功率` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `相位` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium') TAGS (`_ignore_path` VARCHAR(20), `地址` VARCHAR(50), `单元` TINYINT, `楼层` TINYINT, `设备ID` VARCHAR(20), `path1` VARCHAR(512)) SMA(`ts`,`电流`) VIRTUAL 1;",
            "CREATE STABLE `vst_智能水表_1` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `流量` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `水压` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`_ignore_path` VARCHAR(20), `地址` VARCHAR(50), `path1` VARCHAR(512)) SMA(`ts`,`流量`) VIRTUAL 1;",
            "CREATE VTABLE `vt_em-1`  (`电流` FROM `asset01`.`em-1`.`current`,  `电压` FROM `asset01`.`em-1`.`voltage`,  `功率` FROM `asset01`.`em-1`.`power`,  `相位` FROM `asset01`.`em-1`.`phase`)  USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 1, 2,   'em202502200010001', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-2`  (`电流` FROM `asset01`.`em-2`.`current`,  `电压` FROM `asset01`.`em-2`.`voltage`,  `功率` FROM `asset01`.`em-2`.`power`,  `相位` FROM `asset01`.`em-2`.`phase`)  USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 1, 2,   'em202502200010002', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-3`  (`电流` FROM `asset01`.`em-3`.`current`,  `电压` FROM `asset01`.`em-3`.`voltage`,  `功率` FROM `asset01`.`em-3`.`power`,  `相位` FROM `asset01`.`em-3`.`phase`)  USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 1, 2,   'em202502200010003', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-4`  (`电流` FROM `asset01`.`em-4`.`current`,  `电压` FROM `asset01`.`em-4`.`voltage`,  `功率` FROM `asset01`.`em-4`.`power`,  `相位` FROM `asset01`.`em-4`.`phase`)  USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 2, 2,   'em202502200010004', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-5`  (`电流` FROM `asset01`.`em-5`.`current`,  `电压` FROM `asset01`.`em-5`.`voltage`,  `功率` FROM `asset01`.`em-5`.`power`,  `相位` FROM `asset01`.`em-5`.`phase`)  USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 2, 2,   'em202502200010005', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-6`  (`电流` FROM `asset01`.`em-6`.`current`,  `电压` FROM `asset01`.`em-6`.`voltage`,  `功率` FROM `asset01`.`em-6`.`power`,  `相位` FROM `asset01`.`em-6`.`phase`)  USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道',   1, 2,   'em20250220001006',  '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-7`  (`电流` FROM `asset01`.`em-7`.`current`,  `电压` FROM `asset01`.`em-7`.`voltage`,  `功率` FROM `asset01`.`em-7`.`power`,  `相位` FROM `asset01`.`em-7`.`phase`)  USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道',   1, 2,   'em20250220001007',  '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-8`  (`电流` FROM `asset01`.`em-8`.`current`,  `电压` FROM `asset01`.`em-8`.`voltage`,  `功率` FROM `asset01`.`em-8`.`power`,  `相位` FROM `asset01`.`em-8`.`phase`)  USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道',   1, 2,   'em20250220001008',  '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-9`  (`电流` FROM `asset01`.`em-9`.`current`,  `电压` FROM `asset01`.`em-9`.`voltage`,  `功率` FROM `asset01`.`em-9`.`power`,  `相位` FROM `asset01`.`em-9`.`phase`)  USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道',   1, 2,   'em20250220001009',  '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-10` (`电流` FROM `asset01`.`em-10`.`current`, `电压` FROM `asset01`.`em-10`.`voltage`, `功率` FROM `asset01`.`em-10`.`power`, `相位` FROM `asset01`.`em-10`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.三元桥街道', 1, 2,   'em202502200010010', '公共事业.北京.朝阳.三元桥街道');",
            "CREATE VTABLE `vt_em-11` (`电流` FROM `asset01`.`em-11`.`current`, `电压` FROM `asset01`.`em-11`.`voltage`, `功率` FROM `asset01`.`em-11`.`power`, `相位` FROM `asset01`.`em-11`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道',   11, 11, 'em202502200010011', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_em-12` (`电流` FROM `asset01`.`em-12`.`current`, `电压` FROM `asset01`.`em-12`.`voltage`, `功率` FROM `asset01`.`em-12`.`power`, `相位` FROM `asset01`.`em-12`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道',   11, 12, 'em202502200010012', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_em-13` (`电流` FROM `asset01`.`em-13`.`current`, `电压` FROM `asset01`.`em-13`.`voltage`, `功率` FROM `asset01`.`em-13`.`power`, `相位` FROM `asset01`.`em-13`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道',   11, 13, 'em202502200010013', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_em-14` (`电流` FROM `asset01`.`em-14`.`current`, `电压` FROM `asset01`.`em-14`.`voltage`, `功率` FROM `asset01`.`em-14`.`power`, `相位` FROM `asset01`.`em-14`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道',   11, 14, 'em202502200010014', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_em-15` (`电流` FROM `asset01`.`em-15`.`current`, `电压` FROM `asset01`.`em-15`.`voltage`, `功率` FROM `asset01`.`em-15`.`power`, `相位` FROM `asset01`.`em-15`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道',   1, 15,  'em202502200010015', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_wm-1`  (`流量` FROM `asset01`.`wm-1`.`rate`, `水压` FROM `asset01`.`wm-1`.`pressure`) USING `vst_智能水表_1` (`_ignore_path`, `地址`, `path1`) TAGS (NULL, '北京.朝阳.三元桥街道', '公共事业.北京.朝阳.三元桥街道');",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} vtable successfully.")

    #
    # 2. create streams
    #
    def createStreams(self):

        sqls = [
            "CREATE STREAM `tdasset`.`ana_stream6_sub5` COUNT_WINDOW(5,5,`电流`)       FROM `tdasset`.`vt_em-6` STREAM_OPTIONS(DELETE_RECALC)                          NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub5` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend AND `电流` IS NOT NULL",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} streams successfully.")

    #
    #  check errors
    #
    def checkErrors(self):

        sqls = [
            # stream5
            "CREATE STREAM `tdasset`.`err_stream5_sub1` SESSION(ts, 10m) FROM `tdasset`.`vst_智能电表_1` NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_err` (ts, col2 PRIMARY KEY, col3, col4, col5) TAGS ( mytbname varchar(100) as tbname) AS SELECT ts,tbname,`电压`,`电流`,`地址` FROM `tdasset`.`vst_智能电表_1` WHERE ts >=_twstart AND ts <_twend",
            "CREATE STREAM `tdasset`.`err_stream5_sub2` SESSION(ts, 10m) FROM `tdasset`.`vst_智能电表_1` NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_err` (ts, col2 PRIMARY KEY, col3, col4)                                               AS SELECT ts,tbname,`电压`,`电流`,`地址` FROM %%trows",
            "CREATE STREAM `tdasset`.`err_stream5_sub3` SESSION(ts, 10m) FROM `tdasset`.`vst_智能电表_1` NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_err` (ts, `tbname` PRIMARY KEY, wstart, wend, row, vol_sum, cur, addr)                AS SELECT _twstart,tbname, _twstart, _twend, _twrownum, SUM(`电压`), `电流`, `地址` FROM %%trows",
            # stream6
            "CREATE STREAM `tdasset`.`err_stream6_sub1` COUNT_WINDOW(5, 5,`电流`, `地址`)  FROM `tdasset`.`vt_em-6`                                 STREAM_OPTIONS(IGNORE_DISORDER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream_err` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`err_stream6_sub2` COUNT_WINDOW(5,5,`电流`, tbname)  FROM `tdasset`.`vt_em-6`                                 STREAM_OPTIONS(IGNORE_DISORDER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream_err` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`err_stream6_sub3` COUNT_WINDOW(5,5,`电流`,`电压`)    FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname,`电压`                                 NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream_err` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`err_stream6_sub4` COUNT_WINDOW(5,5,`电流`,`电压`)    FROM `tdasset`.`vst_智能电表_1` PARTITION BY `地址`                                        NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream_err` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`err_stream6_sub5` COUNT_WINDOW(5,5,`电流`,`电压`)    FROM `tdasset`.`vst_智能电表_1`                                                           NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream_err` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            # ***** bug TD-37665 *****
            # "CREATE STREAM `tdasset`.`err_stream6_sub5` COUNT_WINDOW(5,5,`电流`,`电压`)   FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname,`地址`                                 NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub4` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            # ***** bug TD-37529 *****
            # "CREATE STREAM `tdasset`.`err_stream11_sub1` INTERVAL(10s, 1s) SLIDING(4s, 1s) FROM `tdasset`.`vt_em-11`  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream11_sub2` AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
        ]

        tdSql.errors(sqls)
        tdLog.info(f"check {len(sqls)} errors sql successfully.")

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
        print("writeTriggerData ...")
        # stream6
        self.trigger_stream6()

    #
    # 5. verify results
    #
    def verifyResults(self):
        self.verify_stream6_sub5()

    #
    # 6. write trigger data again
    #
    def writeTriggerDataAgain(self):
        print("writeTriggerDataAgain ...")
        # stream6
        self.trigger_stream6_again()

    #
    # 7. verify results again
    #
    def verifyResultsAgain(self):
        # wait for stream processing
        time.sleep(3)
        print("verifyResultsAgain ...")

        # stream6
        self.verify_stream6_again()

    #
    # 8. restart dnode
    #
    def restartDnode(self):
        # restart
        tdLog.info("restart dnode ...")
        sc.dnodeRestartAll()

        # wait stream ready
        tdLog.info("wait stream ready after dnode restart ...")
        self.checkStreamStatus()

        tdLog.info("dnode restarted successfully.")

    #
    # 9. write trigger after restart
    #
    def writeTriggerAfterRestart(self):
        pass

    #
    # 10. verify results after restart
    #
    def verifyResultsAfterRestart(self):
        pass

    def checkTaosdLog(self, key):
        cnt = eutil.findTaosdLog(key)
        if cnt <= 0:
            tdLog.exit(f"check taosd log failed, key={key} not found.")
        else:
            print(f"check taosd log success, key:{key} found cnt:{cnt}.")

    #
    # ---------------------   find other bugs   ----------------------
    #

    # virtual table ts is null
    def check_vt_ts(self):
        # vt_em-4
        tdSql.checkResultsByFunc(
            sql="SELECT *  FROM tdasset.`vt_em-4` WHERE `电流` is null;",
            func=lambda: tdSql.getRows() == 120
            and tdSql.compareData(0, 0, 1752574200000)
            and tdSql.compareData(0, 2, 400)
            and tdSql.compareData(0, 3, 200),
        )

    def getSlidingWindow(self, start, step, cnt):
        wins = []
        x = int(start / step)
        i = 0

        while len(wins) < cnt:
            win = (x + i) * step
            if win >= start:
                wins.append(win)
            # move next
            i += 1

        return wins

    #
    # ---------------------   stream trigger    ----------------------
    #

    #
    #  stream6 trigger
    #
    def trigger_stream6(self):
        ts = self.start2
        table = "asset01.`em-6`"
        step = 1 * 60 * 1000  # 1 minute

        # write to windows 1 ~ 2
        count = 10
        cols = "ts,voltage"
        orderVals = [200]
        ts = tdSql.insertOrderVal(table, ts, step, count, cols, orderVals)

        # save disTs
        disTs = ts

        # write end window 5
        count = 2
        ts += 10 * step
        win5Vals = [600]
        win5Ts = tdSql.insertOrderVal(table, ts, step, count, cols, win5Vals)

        # flush db to write disorder data
        tdSql.flushDb("asset01")
        tdSql.flushDb(self.vdb)

        # write disorder window 3
        ts = disTs
        count = 5
        orderVals = [400]
        ts = tdSql.insertOrderVal(table, ts, step, count, cols, orderVals)

        # write current
        cols = "ts,current,voltage,power"

        # blank 10
        ts += 10 * step

        # write
        count = 2
        fixedVals = "10, 10, 1"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        count = 2
        fixedVals = "null, 20, 1"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        count = 2
        fixedVals = "30, null, 1"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        count = 2
        fixedVals = "null, 40, 1"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        count = 2
        fixedVals = "50, null, 1"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        count = 2
        fixedVals = "null, null, 1"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        count = 10
        fixedVals = "60, 60, 1"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

    #
    #  again stream6 trigger
    #
    def trigger_stream6_again(self):
        ts = self.start2
        table = "asset01.`em-6`"
        step = 1 * 60 * 1000  # 1 minute

        # write to windows 1 ~ 2
        count = 10
        cols = "ts,voltage"
        orderVals = [2000]
        ts = tdSql.insertOrderVal(table, ts, step, count, cols, orderVals)

        # save disTs
        disTs = ts

        # write end window 5
        count = 2
        ts += 10 * step
        win5Vals = [6000]
        win5Ts = tdSql.insertOrderVal(table, ts, step, count, cols, win5Vals)

        # write disorder window 3
        ts = disTs
        count = 5
        orderVals = [4000]
        ts = tdSql.insertOrderVal(table, ts, step, count, cols, orderVals)

        # delete
        count = 15
        sql = f"delete from {table} where ts >= {self.start2} and ts < {self.start2 + count * step}"
        print(sql)
        tdSql.execute(sql)

    #
    # ---------------------   verify    ----------------------
    #

    def verify_stream6_sub5(self):
        # check
        result_sql = f"select * from tdasset.`result_stream6_sub5` "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 3)

        # check data
        data = [
            # ts          cnt wrow curcnt volcnt minvol, maxvol, sumpower
            [1752575700000, 5, 5, 5, 2, 10, 10, 5],
            [1752576240000, 5, 5, 5, 4, 60, 60, 5],
            [1752576660000, 5, 5, 5, 5, 60, 60, 5],
        ]
        tdSql.checkDataMem(result_sql, data)

        print("verify stream6_sub5 ............................ successfully.")

    #
    # verify stream6 again
    #
    def verify_stream6_again(self):
        # disorder update and del no effected for count windows
        self.verify_stream6_sub5()