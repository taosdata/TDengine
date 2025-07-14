import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, etool
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

        Labels: common,ci

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

        # wait stream processing
        self.waitStreamProcessing()

        # verify results
        self.verifyResults()

        # write trigger data again
        self.writeTriggerDataAgain()

        # wait stream processing
        self.waitStreamProcessing()

        # verify results
        self.verifyResultsAgain()

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
        etool.taosdump(f"-i cases/13-StreamProcessing/20-UseCase/meters_data/data/")

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
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream1`  event_window( start with `电压` > 250 end with `电压` <= 250 ) TRUE_FOR(10m) FROM `tdasset`.`vt_em-1`  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream1` AS SELECT _twstart+0s AS output_timestamp, avg(`电压`) AS `平均电压`  FROM tdasset.`vt_em-1`  WHERE ts >= _twstart AND ts <=_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream1_sub1`  event_window( start with `电压` > 250 end with `电压` <= 250 ) TRUE_FOR(10m) FROM `tdasset`.`vt_em-1`  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN) INTO `tdasset`.`result_stream1_sub1` AS SELECT _twstart+0s AS output_timestamp, avg(`电压`) AS `平均电压`  FROM tdasset.`vt_em-1`  WHERE ts >= _twstart AND ts <=_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream1_sub2`  event_window( start with `电压` > 250 end with `电压` <= 250 ) TRUE_FOR(10m) FROM `tdasset`.`vt_em-1`  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_CLOSE) INTO `tdasset`.`result_stream1_sub2` AS SELECT _twstart+0s AS output_timestamp, avg(`电压`) AS `平均电压`  FROM tdasset.`vt_em-1`  WHERE ts >= _twstart AND ts <=_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream2`  interval(1h)  sliding(5m) FROM `tdasset`.`vt_em-2`  notify('ws://idmp:6042/eventReceive') ON(window_open|window_close) INTO `tdasset`.`result_stream2` AS SELECT _twstart+0s AS output_timestamp, max(`电流`) AS `最大电流` FROM tdasset.`vt_em-2`  WHERE ts >=_twstart AND ts <=_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream3`  event_window( start with `电流` > 100 end with `电流` <= 100 ) TRUE_FOR(5m) FROM `tdasset`.`vt_em-3` NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream3` AS SELECT _twstart+0s AS output_timestamp, AVG(`电流`) AS `平均电流` FROM tdasset.`vt_em-3`  WHERE ts >= _twstart AND ts <=_twend",

            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4` AS SELECT _twstart+0s as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend ",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub1` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` OPTIONS(IGNORE_DISORDER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub1` AS SELECT _twstart+0s as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub2` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN) INTO `tdasset`.`result_stream4_sub2` AS SELECT _twstart + 10a as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub3` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN) INTO `tdasset`.`result_stream4_sub3` AS SELECT _twstart + 10s as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub4` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN) INTO `tdasset`.`result_stream4_sub4` AS SELECT _twstart + 10m as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub5` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN) INTO `tdasset`.`result_stream4_sub5` AS SELECT _twstart + 10h as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub6` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN) INTO `tdasset`.`result_stream4_sub6` AS SELECT _twstart + 10d as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub7` INTERVAL(600s)  SLIDING(1h) FROM `tdasset`.`vt_em-4` OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub7` AS SELECT _twstart as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend AND `电流` is null",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub8` INTERVAL(1a)  SLIDING(1a) FROM `tdasset`.`vt_em-4` OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub8` AS SELECT _twstart as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend AND `电流` is null",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub9` INTERVAL(1d)  SLIDING(60s) FROM `tdasset`.`vt_em-4` OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub9` AS SELECT _twstart as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend AND `电流` is null",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} streams successfully.")

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
        # strem1
        self.trigger_stream1()
        # stream2
        self.trigger_stream2()
        # stream3
        self.trigger_stream3()
        # stream4
        self.trigger_stream4()


    # 
    # 5. wait stream processing
    #
    def waitStreamProcessing(self):
        tdLog.info("wait for check result sleep 5s ...")
        time.sleep(5)

    # 
    # 6. verify results
    #
    def verifyResults(self):
        self.verify_stream1()
        self.verify_stream2()
        self.verify_stream3()
        self.verify_stream4()


    # 
    # 7. write trigger data again
    #
    def writeTriggerDataAgain(self):
        # stream4
        self.trigger_stream4_again()


    # 
    # 8. verify results again
    #
    def verifyResultsAgain(self):
        # stream4
        self.verify_stream4_again()

    # ---------------------   stream trigger    ----------------------

    #
    #  stream1 trigger 
    #
    def trigger_stream1(self):

        # 1~20 minutes no trigger
        ts = self.start
        # voltage = 100
        sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 100);"
        tdSql.execute(sql, show=True)

        # voltage = 300
        for i in range(20):
            ts += 1*60*1000
            sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 300);"
            tdSql.execute(sql, show=True)

        sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 100);"
        tdSql.execute(sql, show=True)

        # voltage = 100
        ts += 1*60*1000
        sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 100);"
        tdSql.execute(sql, show=True)


        # voltage = 400
        for i in range(11):
            ts += 1*60*1000
            sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 400);"
            tdSql.execute(sql, show=True)

        # voltage = 100
        ts += 1*60*1000
        sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 100);"
        tdSql.execute(sql, show=True)

        # high-lower not trigger
        for i in range(30):
            ts += 1*60*1000
            if i % 2 == 0:
                voltage = 250 - i
            else:
                voltage = 250 + i
            sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, {voltage});"
            tdSql.execute(sql, show=True)

    #
    #  stream2 trigger 
    #
    def trigger_stream2(self):
        ts = self.start
        current = self.start_current
        voltage = self.start_voltage
        power   = 200
        phase   = 0

        cnt     = 11 # 
        for i in range(cnt):
            ts += 1 * 60 * 1000
            current += 1
            voltage += 1
            power   += 1
            phase   += 1
            sql = f"insert into asset01.`em-2` values({ts}, {current}, {voltage}, {power}, {phase});"
            tdSql.execute(sql, show=True)

    #
    #  stream3 trigger 
    #
    def trigger_stream3(self):
        ts = self.start
        current = 100
        voltage = 220
        power   = 200
        phase   = 0

        # enter condiction
        cnt     = 10
        for i in range(cnt):
            ts += 1 * 60 * 1000
            current += 1
            voltage += 1
            power   += 1
            phase   += 1
            sql = f"insert into asset01.`em-3` values({ts}, {current}, {voltage}, {power}, {phase});"
            tdSql.execute(sql, show=True)

        # leave condiction
        cnt     = 10 #
        current = 100
        for i in range(cnt):
            ts += 1 * 60 * 1000
            current -= 1
            voltage += 1
            power   += 1
            phase   += 1
            sql = f"insert into asset01.`em-3` values({ts}, {current}, {voltage}, {power}, {phase});"
            tdSql.execute(sql, show=True)

        cnt     = 20
        current = 200
        for i in range(cnt):
            ts += 1 * 60 * 1000
            current += 1
            voltage += 1
            power   += 1
            phase   += 1
            sql = f"insert into asset01.`em-3` values({ts}, {current}, {voltage}, {power}, {phase});"
            tdSql.execute(sql, show=True)

        # lower
        ts += 1*60*1000
        sql = f"insert into asset01.`em-3`(ts, current) values({ts}, 50);"
        tdSql.execute(sql, show=True)


    #
    #  stream4 trigger 
    #
    def trigger_stream4(self):
        ts = self.start2
        table = "asset01.`em-4`"
        step  = 1 * 60 * 1000 # 1 minute
        count = 120
        cols = "ts,voltage,power"
        vals = "400,200"
        tdSql.insertFixedVal(table, ts, step, count, cols, vals)


    #
    #  stream4 trigger again
    #
    def trigger_stream4_again(self):
        ts = self.start2 + 30 * 1000  # offset 30 seconds
        table = "asset01.`em-4`"
        step  = 1 * 60 * 1000 # 1 minute
        count = 119
        cols = "ts,voltage,power"
        vals = "200,100"
        tdSql.insertFixedVal(table, ts, step, count, cols, vals)




    #
    # ---------------------   verify    ----------------------
    #

    #
    # verify stream1
    #
    def verify_stream1(self):
        # result_stream1
        result_sql = f"select * from {self.vdb}.`result_stream1` "
        result_sql_sub1 = f"select * from {self.vdb}.`result_stream1_sub1` "
        result_sql_sub2 = f"select * from {self.vdb}.`result_stream1_sub2` "

        ''' bug1
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-07-15 15:04:20")
            and tdSql.compareData(1, 0, "2025-07-15 15:25:20")
            and tdSql.compareData(0, 1, 300)
            and tdSql.compareData(1, 1, 400)
        )
        '''

        # result_stream1_sub1
        tdSql.checkResultsBySql(
            sql=result_sql,
            exp_sql=result_sql_sub1
        )

        # result_stream1_sub2
        tdSql.checkResultsBySql(
            sql=result_sql,
            exp_sql=result_sql_sub1
        )

        tdLog.info("verify stream1 successfully.")

    #
    # verify stream2
    #
    def verify_stream2(self):
        # result_stream2
        result_sql = f"select * from {self.vdb}.`result_stream2` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-07-15 14:05:00")
            and tdSql.compareData(1, 0, "2025-07-15 14:10:00")
            and tdSql.compareData(0, 1, 11)
            and tdSql.compareData(1, 1, 16)
        )

        tdLog.info("verify stream2 successfully.")

    #
    # verify stream3
    #
    def verify_stream3(self):
        # result_stream3
        result_sql = f"select * from {self.vdb}.`result_stream3` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-07-15 15:04:20")
            and tdSql.compareData(1, 0, "2025-07-15 15:24:20")
        )

        tdLog.info("verify stream3 successfully.")


    #
    # verify stream4
    #
    def verify_stream4(self, tables=None):
        # result_stream4/result_stream4_sub1
        objects = [
            "result_stream4",
            "result_stream4_sub1"
        ]
        
        if tables is not None:
            objects = tables

        for obj in objects:
            result_sql = f"select * from {self.vdb}.`{obj}` "
            tdLog.info(result_sql)
            tdSql.checkResultsByFunc (
                sql = result_sql, 
                func = lambda: tdSql.getRows() == 11
            )

            ts = self.start2
            for i in range(tdSql.getRows()):
                tdSql.checkData(i, 0, ts)
                tdSql.checkData(i, 1, 10)
                tdSql.checkData(i, 2, 400)
                tdSql.checkData(i, 3, 2000)
                ts += 10 * 60 * 1000 # 10 minutes

        tdLog.info(f"verify stream4 {objects} successfully.")

        if tables is not None:
            # verify special table
            return

        # verify stream4_sub2 ~ 6
        offsets = [
            10,                      # a
            10 * 1000,               # s
            10 * 60 * 1000,          # m
            10 * 60 * 60 * 1000,     # h
            10 * 24 * 60 * 60 * 1000 # d
        ]

        for i in range(2, 7):
            result_sql = f"select * from {self.vdb}.`result_stream4_sub{i}` "
            tdLog.info(result_sql)
            tdSql.checkResultsByFunc (
                sql = result_sql, 
                func = lambda: tdSql.getRows() == 11
            )

            ts = self.start2 + offsets[i - 2]
            for j in range(tdSql.getRows()):
                tdSql.checkData(j, 0, ts)
                tdSql.checkData(j, 1, 10)       
                tdSql.checkData(j, 2, 400)
                tdSql.checkData(j, 3, 2000)
                ts += 10 * 60 * 1000 # 10 minutes   
        tdLog.info(f"verify stream4_sub2 ~ 6 successfully.")

        # verify virtual table ts null
        # ***** bug3 ****
        #self.check_vt_ts()

    #
    # verify stream4 again
    #
    def verify_stream4_again(self):
        # result_stream4
        ts = self.start2
        result_sql = f"select * from {self.vdb}.`result_stream4` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 11
        )

        for i in range(tdSql.getRows()):
            tdSql.checkData(i, 0, ts)
            tdSql.checkData(i, 1, 20)
            tdSql.checkData(i, 2, 300)
            tdSql.checkData(i, 3, 3000)
            ts += 10 * 60 * 1000 # 10 minutes

        self.verify_stream4(tables=["result_stream4_sub1"])

        ''' ***** bug2 *****
        # restart dnode
        tdLog.info("restart dnode to verify stream4_sub1 ...")
        sc.dnodeRestartAll()

        # result_stream4_sub1
        for i in range(10):
            # write 
            sqls = [
                "INSERT INTO asset01.`em-4`(ts,voltage,power) VALUES(1752574230000,2000,1000);",
                "INSERT INTO asset01.`em-4`(ts,voltage,power) VALUES(1752574230000,2001,10000);",
                "INSERT INTO asset01.`em-4`(ts,voltage,power) VALUES(1752581310000,2002,1001);"
            ]
            tdSql.executes(sqls)

            tdLog.info(f"loop check i={i} sleep 3s...")
            time.sleep(5)

            # verify
            self.verify_stream4(tables=["result_stream4_sub1"])
        '''    

        tdLog.info("verify stream4 again successfully.")


    #
    # ---------------------   find other bugs   ----------------------
    #
    
    # virtual table ts is null
    def check_vt_ts(self):
        # vt_em-4
        tdSql.checkResultsByFunc (
            sql  = "SELECT *  FROM tdasset.`vt_em-4` WHERE `电流` is null;",
            func = lambda: tdSql.getRows() == 239 
            and tdSql.compareData(0, 0, self.start2) 
            and tdSql.compareData(0, 1, 400)
            and tdSql.compareData(0, 2, 200)
        )        
