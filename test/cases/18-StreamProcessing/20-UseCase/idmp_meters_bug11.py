import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, etool, sc
from datetime import datetime
from datetime import date


class Test_IDMP_Meters:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_usecase_em(self):
        """Nevados

        Refer: https://taosdata.feishu.cn/wiki/Zkb2wNkHDihARVkGHYEcbNhmnxb

        Catalog:
            - Streams:UseCases

        Since: v3.3.7.0

        Labels: common,ci,skip

        Jira: https://jira.taosdata.com:18080/browse/TD-36363

        History:
            - 2025-7-10 Alex Duan Created

        """

        #
        #  main test
        #

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
        #self.checkErrors()

        # verify results
        self.verifyResults()

        # write trigger data again
        self.writeTriggerDataAgain()

        # verify results
        self.verifyResultsAgain()

        '''
        # restart dnode
        self.restartDnode()

        # verify results after restart
        self.verifyResults()

        # write trigger data after restart
        self.writeTriggerAfterRestart()

        # verify results after restart
        self.verifyResultsAfterRestart()
        '''


    #
    # ---------------------   main flow frame    ----------------------
    #

    # 
    # prepare data
    #
    def prepare(self):
        # name
        self.db = "assert01"
        self.vdb = "tdasset"
        self.stb = "electricity_meters"
        self.start = 1752563000000
        self.start_current = 10
        self.start_voltage = 260

        self.start2 = 1752574200000

        # import data
        etool.taosdump(f"-i cases/41-StreamProcessing/20-UseCase/meters_data/data/")

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
            "CREATE VTABLE `vt_em-1` (`电流` FROM `asset01`.`em-1`.`current`, `电压` FROM `asset01`.`em-1`.`voltage`, `功率` FROM `asset01`.`em-1`.`power`, `相位` FROM `asset01`.`em-1`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 1, 2, 'em202502200010001', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-2` (`电流` FROM `asset01`.`em-2`.`current`, `电压` FROM `asset01`.`em-2`.`voltage`, `功率` FROM `asset01`.`em-2`.`power`, `相位` FROM `asset01`.`em-2`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 1, 2, 'em202502200010002', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-3` (`电流` FROM `asset01`.`em-3`.`current`, `电压` FROM `asset01`.`em-3`.`voltage`, `功率` FROM `asset01`.`em-3`.`power`, `相位` FROM `asset01`.`em-3`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 1, 2, 'em202502200010003', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-4` (`电流` FROM `asset01`.`em-4`.`current`, `电压` FROM `asset01`.`em-4`.`voltage`, `功率` FROM `asset01`.`em-4`.`power`, `相位` FROM `asset01`.`em-4`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 2, 2, 'em202502200010004', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-5` (`电流` FROM `asset01`.`em-5`.`current`, `电压` FROM `asset01`.`em-5`.`voltage`, `功率` FROM `asset01`.`em-5`.`power`, `相位` FROM `asset01`.`em-5`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 2, 2, 'em202502200010005', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-6` (`电流` FROM `asset01`.`em-6`.`current`, `电压` FROM `asset01`.`em-6`.`voltage`, `功率` FROM `asset01`.`em-6`.`power`, `相位` FROM `asset01`.`em-6`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道', 1, 2, 'em20250220001006', '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-7` (`电流` FROM `asset01`.`em-7`.`current`, `电压` FROM `asset01`.`em-7`.`voltage`, `功率` FROM `asset01`.`em-7`.`power`, `相位` FROM `asset01`.`em-7`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道', 1, 2, 'em20250220001007', '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-8` (`电流` FROM `asset01`.`em-8`.`current`, `电压` FROM `asset01`.`em-8`.`voltage`, `功率` FROM `asset01`.`em-8`.`power`, `相位` FROM `asset01`.`em-8`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道', 1, 2, 'em20250220001008', '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-9` (`电流` FROM `asset01`.`em-9`.`current`, `电压` FROM `asset01`.`em-9`.`voltage`, `功率` FROM `asset01`.`em-9`.`power`, `相位` FROM `asset01`.`em-9`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道', 1, 2, 'em20250220001009', '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-10` (`电流` FROM `asset01`.`em-10`.`current`, `电压` FROM `asset01`.`em-10`.`voltage`, `功率` FROM `asset01`.`em-10`.`power`, `相位` FROM `asset01`.`em-10`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.三元桥街道', 1, 2, 'em202502200010010', '公共事业.北京.朝阳.三元桥街道');",
            "CREATE VTABLE `vt_em-11` (`电流` FROM `asset01`.`em-11`.`current`, `电压` FROM `asset01`.`em-11`.`voltage`, `功率` FROM `asset01`.`em-11`.`power`, `相位` FROM `asset01`.`em-11`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道', 11, 11, 'em202502200010011', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_em-12` (`电流` FROM `asset01`.`em-12`.`current`, `电压` FROM `asset01`.`em-12`.`voltage`, `功率` FROM `asset01`.`em-12`.`power`, `相位` FROM `asset01`.`em-12`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道', 11, 12, 'em202502200010012', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_em-13` (`电流` FROM `asset01`.`em-13`.`current`, `电压` FROM `asset01`.`em-13`.`voltage`, `功率` FROM `asset01`.`em-13`.`power`, `相位` FROM `asset01`.`em-13`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道', 11, 13, 'em202502200010013', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_em-14` (`电流` FROM `asset01`.`em-14`.`current`, `电压` FROM `asset01`.`em-14`.`voltage`, `功率` FROM `asset01`.`em-14`.`power`, `相位` FROM `asset01`.`em-14`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道', 11, 14, 'em202502200010014', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_em-15` (`电流` FROM `asset01`.`em-15`.`current`, `电压` FROM `asset01`.`em-15`.`voltage`, `功率` FROM `asset01`.`em-15`.`power`, `相位` FROM `asset01`.`em-15`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道', 1, 15, 'em202502200010015', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_wm-1` (`流量` FROM `asset01`.`wm-1`.`rate`, `水压` FROM `asset01`.`wm-1`.`pressure`) USING `vst_智能水表_1` (`_ignore_path`, `地址`, `path1`) TAGS (NULL, '北京.朝阳.三元桥街道', '公共事业.北京.朝阳.三元桥街道');"
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} vtable successfully.")
        

    # 
    # 2. create streams
    #
    def createStreams(self):

        sqls = [
            # stream7 state
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream7_sub4` STATE_WINDOW(`电压`) TRUE_FOR(60s) FROM `tdasset`.`vt_em-7` STREAM_OPTIONS(DELETE_RECALC)                                      NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream7_sub4` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-7` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream7_sub5` STATE_WINDOW(`电压`) TRUE_FOR(60s) FROM `tdasset`.`vt_em-7` STREAM_OPTIONS(PRE_FILTER(`电流`>300 and `电流`<=600)|DELETE_RECALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream7_sub5` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-7` WHERE ts >= _twstart AND ts <=_twend",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} streams successfully.")

    #
    #  check errors
    #
    def checkErrors(self):

        sqls = [
            # stream11 sliding window
            # ***** bug TD-37529 *****
            # "CREATE STREAM IF NOT EXISTS `tdasset`.`err_stream11_sub2` INTERVAL(10s, 1s) SLIDING(4s, 1s) FROM `tdasset`.`vt_em-11`  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream11_sub2` AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
        ]

        tdSql.error(sqls)
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
        # stream7
        self.trigger_stream7()


    # 
    # 5. verify results
    #
    def verifyResults(self):
        print("wait 10s ...")
        time.sleep(10)
        print("verifyResults ...")

    # 
    # 6. write trigger data again
    #
    def writeTriggerDataAgain(self):
        print("writeTriggerDataAgain ...")
        # stream7
        self.trigger_stream7_again()


    # 
    # 7. verify results again
    #
    def verifyResultsAgain(self):
        # wait for stream processing
        time.sleep(3)
        print("verifyResultsAgain ...")
        # stream7
        self.verify_stream7_again()

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

    # ---------------------   stream trigger    ----------------------



     #
    #  stream7 trigger
    #
    def trigger_stream7(self):
        ts    = self.start2
        table = "asset01.`em-7`"
        step  = 1 * 60 * 1000 # 1 minute
        cols  = "ts,current,voltage,power"

        # write to windows 1
        count = 2
        fixedVals = "100, 100, 100"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        count = 2
        fixedVals = "200, 200, 200"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        count = 2
        fixedVals = "NULL, NULL, NULL"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        self.delTs7 = ts
        count = 2
        fixedVals = "400, 400, 400"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        # end trigger
        tsEnd = self.start2 + 20 * step
        count = 2
        fixedVals = "1000, 1000, 1000"
        tsEnd = tdSql.insertFixedVal(table, tsEnd, step, count, cols, fixedVals)

        # disorder continue ts
        self.updateTs7 = ts
        count = 2
        fixedVals = "500, 500, 500"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        count = 2
        fixedVals = "600, 600, 600"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        # update 
        count = 2
        fixedVals = "501, 501, 501"
        tdSql.insertFixedVal(table, self.updateTs7, step, count, cols, fixedVals)


    #
    #  stream7 trigger again
    #
    def trigger_stream7_again(self):
        # update 
        table = "asset01.`em-7`"
        step  = 1 * 60 * 1000 # 1 minute
        cols  = "ts,current,voltage,power"

        count = 2
        fixedVals = "502, 502, 502"
        tdSql.insertFixedVal(table, self.updateTs7, step, count, cols, fixedVals)

        # del
        count = 2
        sql = f"delete from {table} where ts >= {self.delTs7} and ts < {self.delTs7 + count * step};"
        print(sql)
        tdSql.execute(sql)


    #
    # ---------------------   verify    ----------------------
    #

        #
    # verify stream7
    #
    def verify_stream7_again(self):

        # check

        # del sub4
        result_sql = f"select * from tdasset.`result_stream7_sub4` "
        tdSql.checkResultsByFunc(
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 4
        )

        # check data
        data = [
            # ts          cnt    
            [1752574200000, 2, 100, 200],
            [1752574320000, 2, 200, 400],
            [1752574680000, 2, 502, 1004],
            [1752574800000, 2, 600, 1200]
        ]
        tdSql.checkDataMem(result_sql, data)

        # del sub5
        result_sql = f"select * from tdasset.`result_stream7_sub5` "
        tdSql.checkResultsByFunc(
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 1752574680000)
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 502)
            and tdSql.compareData(0, 3, 1004)
        )

        tdLog.info(f"verify stream7_again ............................. successfully.")
        