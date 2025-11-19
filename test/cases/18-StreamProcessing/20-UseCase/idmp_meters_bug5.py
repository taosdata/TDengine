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

        # verify results
        self.verifyResults()

        # write trigger data again
        self.writeTriggerDataAgain()

        # verify results
        self.verifyResultsAgain()


        '''
        # restart dnode
        self.restartDnode()

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
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream1`       event_window( start with `电压` > 250 end with `电压` <= 250 ) TRUE_FOR(10m) FROM `tdasset`.`vt_em-1`                                          NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream1`      AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, avg(`电压`) AS `平均电压`  FROM tdasset.`vt_em-1`  WHERE ts >= _twstart AND ts <_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream1_sub1`  event_window( start with `电压` > 250 end with `电压` <= 250 ) TRUE_FOR(10m) FROM `tdasset`.`vt_em-1` STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN))  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream1_sub1` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, avg(`电压`) AS `平均电压`  FROM tdasset.`vt_em-1`  WHERE ts >= _twstart AND ts <_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream1_sub2`  event_window( start with `电压` > 250 end with `电压` <= 250 ) TRUE_FOR(10m) FROM `tdasset`.`vt_em-1` STREAM_OPTIONS(EVENT_TYPE(WINDOW_CLOSE)) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_CLOSE)             INTO `tdasset`.`result_stream1_sub2` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, avg(`电压`) AS `平均电压`  FROM tdasset.`vt_em-1`  WHERE ts >= _twstart AND ts <_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream2`  interval(1h)  sliding(5m) FROM `tdasset`.`vt_em-2`  notify('ws://idmp:6042/eventReceive') ON(window_open|window_close) INTO `tdasset`.`result_stream2` AS SELECT _twstart+0s AS ts, max(`电流`) AS `最大电流` FROM tdasset.`vt_em-2`  WHERE ts >=_twstart AND ts <=_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream3`  event_window( start with `电流` > 100 end with `电流` <= 100 ) TRUE_FOR(5m) FROM `tdasset`.`vt_em-3` NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream3` AS SELECT _twstart+0s AS ts, AVG(`电流`) AS `平均电流` FROM tdasset.`vt_em-3`  WHERE ts >= _twstart AND ts <=_twend",
            # stream4
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4`      INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4`                                                  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4`      AS SELECT _twstart + 0s  as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <_twend ",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub1` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER)                  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub1` AS SELECT _twstart + 0s  as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub2` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub2` AS SELECT _twstart + 10a as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub3` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub3` AS SELECT _twstart + 10s as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub4` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub4` AS SELECT _twstart + 10m as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub5` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub5` AS SELECT _twstart + 10h as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub6` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub6` AS SELECT _twstart + 10d as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub7` INTERVAL(600s) SLIDING(1h)  FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub7` AS SELECT _twstart       as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend AND ts >= 1752574200000",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub9` INTERVAL(1d)   SLIDING(60s) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub9` AS SELECT _twstart       as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts < _twend AND ts >= 1752574200000",
            # stream5
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream5`      SESSION(ts, 10m) FROM `tdasset`.`vt_em-5` STREAM_OPTIONS(IGNORE_DISORDER)  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5`      AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, LAST(`电流`) AS `最后电流` FROM tdasset.`vt_em-5` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream5_sub1` SESSION(ts, 10m) FROM `tdasset`.`vt_em-5`                                  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub1` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, LAST(`电流`) AS `最后电流` FROM tdasset.`vt_em-5` WHERE ts >= _twstart AND ts <=_twend",
            # stream6
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream6`      COUNT_WINDOW(5) FROM `tdasset`.`vt_em-6` STREAM_OPTIONS(IGNORE_DISORDER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream6_sub1` COUNT_WINDOW(5) FROM `tdasset`.`vt_em-6`                                 NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub1` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend",
            # stream7
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream7` STATE_WINDOW(`电压`) TRUE_FOR(30s) FROM `tdasset`.`vt_em-7` STREAM_OPTIONS(IGNORE_DISORDER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream7` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, AVG(`电流`) AS `平均电流`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-7` WHERE ts >= _twstart AND ts <=_twend",
            # stream8
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream8`      PERIOD(1s, 0s) FROM `tdasset`.`vt_em-8`                                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8`      AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts,                                                                                                                 COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream8_sub1` PERIOD(1s, 0s) FROM `tdasset`.`vt_em-8` STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8_sub1` AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts,                                                                                                                 COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream8_sub2` PERIOD(1s, 0s) FROM `tdasset`.`vt_em-8`                                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8_sub2` AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts, CAST(_tprev_localtime/1000000 as timestamp) AS ts_prev, CAST(_tnext_localtime/1000000 as timestamp) AS ts_next, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream8_sub3` PERIOD(1s, 0s)                                                                NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8_sub3` AS SELECT (CAST(_tlocaltime/1000000 as timestamp) - 10000) AS ts, (CAST(_tprev_localtime/1000000 as timestamp) - 10000) AS ts_prev, (CAST(_tnext_localtime/1000000 as timestamp) - 10000) AS ts_next, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-8` WHERE ts >= CAST(_tlocaltime/1000000 as timestamp) - 10000 AND ts < CAST(_tnext_localtime/1000000 as timestamp) - 10000",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream8_sub4` PERIOD(1s, 0s) FROM `tdasset`.`vt_em-8` STREAM_OPTIONS(FORCE_OUTPUT)          NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8_sub4` AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts,                                                                                                                                  AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream8_sub5` PERIOD(1s, 0s) FROM `tdasset`.`vt_em-8`                                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8_sub5` AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts,                                                                                                                                  AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",


            # stream9
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream9` INTERVAL(1a) SLIDING(1a) FROM `tdasset`.`vt_em-9` STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream9` AS SELECT _twstart as ts,COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-9` WHERE ts >=_twstart AND ts <=_twend AND ts >= 1752574200000",

            # stream10 sliding
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream10`      SLIDING(10s, 0s) FROM `tdasset`.`vt_em-10`                                                                                            NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10`      AS SELECT _tcurrent_ts AS ts, _tprev_ts AS prev_ts, _tnext_ts AS next_ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream10_sub1` SLIDING(10s, 0s) FROM `tdasset`.`vt_em-10`                                STREAM_OPTIONS(IGNORE_NODATA_TRIGGER)                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub1` AS SELECT _tcurrent_ts AS ts, _tprev_ts AS prev_ts, _tnext_ts AS next_ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream10_sub2` SLIDING(10s, 0s) FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname,`地址` STREAM_OPTIONS(IGNORE_NODATA_TRIGGER)                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub2` AS SELECT _tcurrent_ts AS ts,                                             COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream10_sub3` SLIDING(10s, 0s) FROM `tdasset`.`vt_em-10`                                STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|PRE_FILTER(`电压`=100)) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub3` AS SELECT _tcurrent_ts AS ts,                                             COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream10_sub4` INTERVAL(10s) SLIDING(10s) FROM `tdasset`.`vt_em-10`                      STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|PRE_FILTER(`电压`=100)) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub4` AS SELECT _twstart AS ts,                                                 COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows;"
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream10_sub5` INTERVAL(10s) SLIDING(10s) FROM `tdasset`.`vt_em-10`                      STREAM_OPTIONS(IGNORE_NODATA_TRIGGER)                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub5` AS SELECT _twstart AS ts,                                                 COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows;"
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
        print("writeTriggerData ...")
        # strem1
        self.trigger_stream1()
        # stream2
        self.trigger_stream2()
        # stream3
        self.trigger_stream3()
        # stream4
        self.trigger_stream4()
        # stream5
        self.trigger_stream5()
        # stream6
        self.trigger_stream6()
        # stream7
        self.trigger_stream7()
        # stream8
        self.trigger_stream8()
        # stream9
        self.trigger_stream9()
        # stream10
        self.trigger_stream10()


    # 
    # 5. verify results
    #
    def verifyResults(self):
        print("verifyResults ...")
        self.verify_stream1()
        self.verify_stream2()
        self.verify_stream3()
        # JIRA TD-36815 fixed need open this check
        #self.verify_stream4()
        self.verify_stream5()
        self.verify_stream6()
        self.verify_stream7()
        self.verify_stream8()
        self.verify_stream9()
        self.verify_stream10()


    # 
    # 6. write trigger data again
    #
    def writeTriggerDataAgain(self):
        # stream4 
        self.trigger_stream4_again()
        # stream6
        self.trigger_stream6_again()


    # 
    # 7. verify results again
    #
    def verifyResultsAgain(self):
        # stream4
        # JIRA TD-36815 fixed need open this check
        # self.verify_stream4_again()
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

        ts += 1*60*1000
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
    #  stream5 trigger 
    #
    def trigger_stream5(self):
        ts = self.start2
        table = "asset01.`em-5`"
        step  = 1 * 60 * 1000 # 1 minute
        
        # first window have 3 + 5 = 10 rows
        count = 3
        cols = "ts,current,voltage,power"
        vals = "30,400,200"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # boundary of first window
        count = 4
        ts += 9 * step
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)
        # last
        count = 1
        vals = "31,401,201"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # save span ts
        spanTs = ts

        # trigger first windows close with 11 steps
        count = 1
        ts += 30 * step
        vals = "40,500,300"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # disorder data

        # from span write 2 rows
        count = 2
        disTs = spanTs + 5 * step
        orderVals = [36, 406, 206]
        disTs = tdSql.insertOrderVal(table, disTs, step, count, cols, orderVals)


    #
    #  stream6 trigger 
    #
    def trigger_stream6(self):
        ts = self.start2
        table = "asset01.`em-6`"
        step  = 1 * 60 * 1000 # 1 minute
        

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
        win5Ts   = tdSql.insertOrderVal(table, ts, step, count, cols, win5Vals)

        # flush db to write disorder data
        tdSql.flushDb("asset01")
        tdSql.flushDb(self.vdb)

        # write disorder window 3
        ts = disTs
        count = 5
        orderVals = [400]
        ts = tdSql.insertOrderVal(table, ts, step, count, cols, orderVals)

        # write window5 1 rows to tigger 

    #
    #  again stream6 trigger
    #
    def trigger_stream6_again(self):
        ts = self.start2
        table = "asset01.`em-6`"
        step  = 1 * 60 * 1000 # 1 minute
        

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
        win5Ts   = tdSql.insertOrderVal(table, ts, step, count, cols, win5Vals)

        # write disorder window 3
        ts = disTs
        count = 5
        orderVals = [4000]
        ts = tdSql.insertOrderVal(table, ts, step, count, cols, orderVals)

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
        fixedVals = "100, 200, 300"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        count = 2
        fixedVals = "200, 300, 400"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        count = 2
        fixedVals = "300, NULL, 500"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        count = 2
        fixedVals = "400, 500, 600"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        # end trigger
        count = 1
        fixedVals = "401, 501, 601"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

    #
    #  stream8 trigger
    #
    def trigger_stream8(self):
        ts    = self.start2
        table = "asset01.`em-8`"
        cols  = "ts,current,voltage,power"
        fixedVals = "100, 200, 300"

        # insert order 
        count = 10
        step  = 200
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        # delete no affected
        #sql = f"delete from asset01.`em-8` where ts >= {self.start2}"
        #tdSql.execute(sql)

        time.sleep(5)

        # write order
        count = 20
        sleepS = 0.2  # 0.2 seconds
        tdSql.insertNow(table, sleepS, count, cols, fixedVals)

        # write disorder
        #count = 10
        #ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        #update no affected
        #updateVals = "1000, 2000, 3000"
        #step  = 300
        #count = 5
        #tdSql.insertFixedVal(table, self.start2, step, count, cols, updateVals)

    #
    #  stream9 trigger 
    #
    def trigger_stream9(self):
        ts = self.start2
        table = "asset01.`em-9`"
        step  = 100 # 100ms
        count = 120
        cols = "ts,voltage,power"
        vals = "400,200"
        tdSql.insertFixedVal(table, ts, step, count, cols, vals)


    #
    #  stream10 trigger 
    #
    def trigger_stream10(self):
        ts = self.start2
        table = "asset01.`em-10`"
        cols = "ts,voltage,power"
        vals = "100,200"

        # batch1
        step  = 1000
        count = 11
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # skip 10
        ts += step * 10

        # batch2
        count = 10
        vals  = "200,200"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)


    #
    # ---------------------   verify    ----------------------
    #

    #
    # verify stream1
    #
    def verify_stream1(self):
        # result_stream1
        result_sql      = f"select * from tdasset.`result_stream1` "
        result_sql_sub1 = f"select * from tdasset.`result_stream1_sub1` "
        result_sql_sub2 = f"select * from tdasset.`result_stream1_sub2` "

        # result_stream1        
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, 1752563060000)
            and tdSql.compareData(0, 1, 20)   # cnt
            and tdSql.compareData(0, 2, 300)
            and tdSql.compareData(1, 0, 1752564380000)
            and tdSql.compareData(1, 1, 11)  # cnt
            and tdSql.compareData(1, 2, 400)
        )

        # result_stream_sub1
        tdSql.checkResultsByFunc (
            sql = result_sql_sub1, 
            func = lambda: tdSql.checkRows(17, show=True)
            and tdSql.compareData(0, 0, 1752563060000)
            and tdSql.compareData(0, 1, 0)    # cnt
        )

        # result_stream1_sub2
        tdSql.checkResultsBySql(
            sql     = result_sql,
            exp_sql = result_sql_sub2
        )

        tdLog.info("verify stream1 .................................. successfully.")

    #
    # verify stream2
    #
    def verify_stream2(self):
        # result_stream2
        result_sql = f"select * from tdasset.`result_stream2` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-07-15 14:05:00")
            and tdSql.compareData(1, 0, "2025-07-15 14:10:00")
            and tdSql.compareData(0, 1, 11)
            and tdSql.compareData(1, 1, 16)
        )

        tdLog.info("verify stream2 .................................. successfully.")

    #
    # verify stream3
    #
    def verify_stream3(self):
        # result_stream3
        result_sql = f"select * from tdasset.`result_stream3` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-07-15 15:04:20")
            and tdSql.compareData(1, 0, "2025-07-15 15:24:20")
        )

        tdLog.info("verify stream3 .................................. successfully.")


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
            result_sql = f"select * from tdasset.`{obj}` "
            tdSql.checkResultsByFunc (
                sql = result_sql, 
                func = lambda: tdSql.getRows() == 11
            )

            ts = self.start2
            for i in range(tdSql.getRows()):
                tdSql.checkData(i, 0, ts)
                tdSql.checkData(i, 1, 11)
                tdSql.checkData(i, 2, 400)
                tdSql.checkData(i, 3, 2000)
                ts += 10 * 60 * 1000 # 10 minutes

        tdLog.info(f"verify stream4 {objects} ....................... successfully.")

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
            result_sql = f"select * from tdasset.`result_stream4_sub{i}` "
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
        tdLog.info(f"verify stream4_sub2 ~ 6 ....................... successfully.")

        # verify stream4_sub7
        self.verify_stream4_sub7()

        # verify stream4_sub9
        self.verify_stream4_sub9()

        # verify virtual table ts null
        self.check_vt_ts()

    def getSlidingWindow(self, start, step, cnt):
        wins = []
        x = int(start/step)
        i = 0

        while len(wins) < cnt:
            win = (x + i) * step
            if win >= start:
                wins.append(win)
            # move next    
            i += 1        

        return wins

    def verify_stream4_sub7(self):
        # result_stream4_sub7
        wins = self.getSlidingWindow(self.start2, 1*60*60*1000, 1)
        result_sql = f"select * from tdasset.`result_stream4_sub7` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 1
            and tdSql.checkData(0, 0, wins[0])
            and tdSql.checkData(0, 1, 10)
            and tdSql.checkData(0, 2, 400)
            and tdSql.checkData(0, 3, 10*200)
        )


    def verify_stream4_sub9(self):
        # result_stream4_sub9
        ts = 1752487860000
        result_sql = f"select * from tdasset.`result_stream4_sub9` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 119,
            retry = 120
        )

        for i in range(tdSql.getRows()):
            tdSql.checkData(i, 0, ts + i * (1 * 60 * 1000))
            tdSql.checkData(i, 1, i + 1)
            tdSql.checkData(i, 2, 400)
            tdSql.checkData(i, 3, (i + 1) * 200)

        tdLog.info("verify stream4_sub9 ............................. successfully.")

    #
    # verify stream4 again
    #
    def verify_stream4_again(self):
        # result_stream4
        ts         = self.start2
        result_sql = f"select * from tdasset.`result_stream4` "
        tdSql.checkResultsByFunc (
            sql   = result_sql, 
            func  = lambda: tdSql.getRows() == 11,
            delay = 10
        )

        for i in range(tdSql.getRows()):
            tdSql.checkData(i, 0, ts)
            tdSql.checkData(i, 1, 20)
            tdSql.checkData(i, 2, 300)
            tdSql.checkData(i, 3, 3000)
            ts += 10 * 60 * 1000 # 10 minutes

        self.verify_stream4(tables=["result_stream4_sub1"])

        
        # restart dnode
        tdLog.info("restart dnode to verify stream4_sub1 ...")
        self.restartDnode()
        

        # result_stream4_sub1
        for i in range(3):
            # write 
            sqls = [
                "INSERT INTO asset01.`em-4`(ts,voltage,power) VALUES(1752574230000,2000,1000);",
                "INSERT INTO asset01.`em-4`(ts,voltage,power) VALUES(1752574230000,2001,10000);",
                "INSERT INTO asset01.`em-4`(ts,voltage,power) VALUES(1752581310000,2002,1001);"
            ]
            tdSql.executes(sqls)

            tdLog.info(f"loop check i={i} sleep 3s...")
            time.sleep(1)

            # verify
            self.verify_stream4(tables=["result_stream4_sub1"])


        tdLog.info("verify stream4 again ............................ successfully.")

    
    #
    # verify stream5
    #

    def verify_stream5(self):
        # result_stream5
        result_sql = f"select * from tdasset.`result_stream5` "
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, self.start2) # ts
            and tdSql.compareData(0, 1, 3 + 4 + 1)   # cnt
            and tdSql.compareData(0, 2, 31)          # last current
        )

        # sub
        self.verify_stream5_sub1()

        tdLog.info(f"verify stream5 ................................. successfully.")


    def verify_stream5_sub1(self):    
        # result_stream5_sub1
        result_sql = f"select * from tdasset.`result_stream5_sub1` "
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, self.start2) # ts
            and tdSql.compareData(0, 1, 3 + 4 + 1 + 2)   # cnt
            and tdSql.compareData(0, 2, 37)          # last current
        )

        tdLog.info(f"verify stream5 sub1 ............................ successfully.")


    #
    # verify stream6
    #

    def verify_stream6(self):
        # result_stream6
        result_sql = f"select * from tdasset.`result_stream6` "
        ts         = self.start2
        step       = 1 * 60 * 1000 # 1 minute
        cnt        = 5

        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 2
            # window1
            and tdSql.compareData(0, 0, ts)      # ts
            and tdSql.compareData(0, 1, 5)       # cnt
            and tdSql.compareData(0, 2, 200)     # min(voltage)
            and tdSql.compareData(0, 3, 204)     # max(voltage)
            # window2
            and tdSql.compareData(1, 0, ts + 5 * step) # ts
            and tdSql.compareData(1, 1, 5)       # cnt
            and tdSql.compareData(1, 2, 205)     # min(voltage)
            and tdSql.compareData(1, 3, 209)     # max(voltage)
        )

        # sub1
        exp_sql = f"select * from tdasset.`result_stream6_sub1` "

        tdSql.checkResultsBySql(result_sql, exp_sql)

        tdLog.info(f"verify stream6 ................................. successfully.")

    def verify_stream6_again(self):
        # no change
        self.verify_stream6()

    #
    # verify stream7
    #
    def verify_stream7(self):
        # result_stream7
        result_sql = f"select * from tdasset.`result_stream7` "
        ts         = self.start2
        step       = 1 * 60 * 1000 # 1 minute

        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 3
            # window1
            and tdSql.compareData(0, 0, ts)      # ts
            and tdSql.compareData(0, 1, 2)       # cnt
            and tdSql.compareData(0, 2, 100)     # avg(current)
            and tdSql.compareData(0, 3, 600)     # sum(power)
            # window2
            and tdSql.compareData(1, 0, ts + 2 * step) # ts
            and tdSql.compareData(1, 1, 2)       # cnt
            and tdSql.compareData(1, 2, 200)     # avg(current)
            and tdSql.compareData(1, 3, 800)     # sum(power)
            # window3 voltage is null ignore
            # window4
            and tdSql.compareData(2, 0, ts + 6 * step) # ts
            and tdSql.compareData(2, 1, 2)       # cnt
            and tdSql.compareData(2, 2, 400)     # avg(current)
            and tdSql.compareData(2, 3, 1200)    # sum(power)
        )

        tdLog.info(f"verify stream7 ................................. successfully.")


    #
    # verify stream8
    #
    def verify_stream8(self):
        # sleep
        time.sleep(5)

        # result_stream8
        result_sql = f"select * from tdasset.`result_stream8` "

        tdSql.query(result_sql)
        count = tdSql.getRows()
        sum   = 0

        for i in range(count):
            # row
            cnt = tdSql.getData(i, 1)
            if cnt > 0:
                tdSql.checkData(i, 2, 200)       # avg(voltage)
                tdSql.checkData(i, 3, cnt * 300) # sum(power)
            sum += cnt


        # ***** bug2 *****
        self.verify_stream8_sub2()
        self.verify_stream8_sub3()
        self.verify_stream8_sub4()
        return

        if sum != 30:
            tdLog.exit(f"stream8 not found expected data. expected sum(cnt) == 30, actual: {sum}")

        tdLog.info(f"verify stream8 ................................. successfully.")

        # sub
        self.verify_stream8_sub1()

    # verify stream8_sub1    
    def verify_stream8_sub1(self):
        # check sum
        tdSql.checkResultsByFunc (
            sql  = f"select sum(cnt) from tdasset.`result_stream8_sub1`",
            func = lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 30)
        )

        # check no data windows is zero
        # ***** bug1 *****
        '''
        tdSql.checkResultsByFunc (
            sql  = f"select * from tdasset.`result_stream8_sub1` where cnt = 0 ",
            func = lambda: tdSql.getRows() == 0
        )
        '''

        tdLog.info("verify stream8_sub1 ............................. successfully.")

    # verify stream8_sub2
    def verify_stream8_sub2(self):
        # ts - ts_prev = 1000
        tdSql.checkResultsByFunc (
            sql  = "select (cast(ts as bigint) - cast(ts_prev as bigint) ) as dif from tdasset.result_stream8_sub2",
            func = lambda: tdSql.getRows() > 1
        )
        for i in range(tdSql.getRows()):
            tdSql.checkData(i, 0, 1000)

        # ts_next - ts = 1000
        tdSql.checkResultsByFunc (
            sql  = "select (cast(ts_next as bigint) - cast(ts as bigint) ) as dif from tdasset.result_stream8_sub2",
            func = lambda: tdSql.getRows() > 1
        )
        for i in range(tdSql.getRows()):
            tdSql.checkData(i, 0, 1000)

        tdLog.info("verify stream8_sub2 ............................. successfully.")

    # verify stream8_sub3
    def verify_stream8_sub3(self):
        tdSql.checkResultsByFunc (
            sql  = "select sum(cnt) from tdasset.result_stream8_sub3",
            func = lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 20)
        )

        tdLog.info("verify stream8_sub3 ............................. successfully.")

    # verify stream8_sub4
    def verify_stream8_sub4(self):
        # check
        tdSql.checkResultsByFunc (
            sql  = "select * from result_stream8_sub4 where `平均电压` is  null",
            func = lambda: tdSql.getRows() >= 1
        )

        # check data with sub5
        tdSql.checkResultsBySql (
            sql     = "select * from result_stream8_sub4 where `平均电压` is not null",
            exp_sql = "select * from result_stream8_sub5"

        )

        tdLog.info("verify stream8_sub4 ............................. successfully.")

    #
    # verify stream9
    #
    def verify_stream9(self):
        # result_stream9
        tdSql.checkResultsBySql(
            sql     = f"select * from tdasset.`result_stream9` ", 
            exp_sql = f"select ts,1,voltage,power from asset01.`em-9` where ts >= 1752574200000;"
        )
        tdLog.info("verify stream9 .................................. successfully.")


    #
    # verify stream10
    #
    def verify_stream10(self):
        # result_stream10
        tdSql.checkResultsByFunc(
            sql  = f"select * from tdasset.`result_stream10` ", 
            func = lambda: tdSql.getRows() == 4
            # ts
            and tdSql.compareData(0, 0, 1752574200000)
            # cnt
            and tdSql.compareData(0, 3, 1)
            and tdSql.compareData(1, 3, 10)
            and tdSql.compareData(2, 3, 0)
            and tdSql.compareData(3, 3, 10)
        )

        # next_ts - ts = 10000
        tdSql.checkResultsByFunc (
            sql  = "select (cast(next_ts as bigint) - cast(ts as bigint) ) as dif from tdasset.result_stream10",
            func = lambda: tdSql.getRows() == 4
        )
        for i in range(tdSql.getRows()):
            tdSql.checkData(i, 0, 10000)

        # ts - prev_ts = 10000
        tdSql.checkResultsByFunc (
            sql  = "select (cast(ts as bigint) - cast(prev_ts as bigint) ) as dif from tdasset.result_stream10",
            func = lambda: tdSql.getRows() == 4
        )
        for i in range(tdSql.getRows()):
            tdSql.checkData(i, 0, 10000)

        tdLog.info("verify stream10 ................................. successfully.")

        # sub
        self.verify_stream10_sub1()
        self.verify_stream10_sub2()
        self.verify_stream10_sub4()
        # ***** bug4 *****
        #self.verify_stream10_sub3()

    def verify_stream10_sub1(self):
        # check
        tdSql.checkResultsByFunc(
            sql  = f"select * from tdasset.`result_stream10_sub1` ", 
            func = lambda: tdSql.getRows() == 3
            # ts
            and tdSql.compareData(0, 0, 1752574200000)
            # cnt
            and tdSql.compareData(0, 3, 1)
            and tdSql.compareData(1, 3, 10)
            and tdSql.compareData(2, 3, 10)
        )
        tdLog.info("verify stream10_sub1 ............................ successfully.")

    def verify_stream10_sub2(self):
        # check
        tdSql.checkResultsByFunc(
            sql  = f"select * from tdasset.`result_stream10_sub2` where tag_tbname='vt_em-10' ", 
            func = lambda: tdSql.getRows() == 3
            # ts
            and tdSql.compareData(0, 0, 1752574200000)
            # cnt
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(1, 1, 10)
            and tdSql.compareData(2, 1, 10)
        )
        tdLog.info("verify stream10_sub2 ............................ successfully.")

    def verify_stream10_sub3(self):
        # check
        tdSql.checkResultsByFunc(
            sql  = f"select * from tdasset.`result_stream10_sub3`", 
            func = lambda: tdSql.getRows() == 2
            # ts
            and tdSql.compareData(0, 0, 1752574200000)
            # cnt
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(1, 1, 10)
        )
        tdLog.info("verify stream10_sub2 ............................ successfully.")

    def verify_stream10_sub4(self):
        # check
        tdSql.checkResultsByFunc(
            sql  = f"select * from tdasset.`result_stream10_sub4`", 
            func = lambda: tdSql.getRows() == 2
            # ts
            and tdSql.compareData(0, 0, 1752574200000)
            # cnt
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(1, 1, 10)
        )
        tdLog.info("verify stream10_sub4 ............................ successfully.")

    #
    # ---------------------   find other bugs   ----------------------
    #
    
    # virtual table ts is null
    def check_vt_ts(self):
        # vt_em-4
        tdSql.checkResultsByFunc (
            sql  = "SELECT *  FROM tdasset.`vt_em-4` WHERE `电流` is null;",
            func = lambda: tdSql.getRows() == 120 
            and tdSql.compareData(0, 0, 1752574200000) 
            and tdSql.compareData(0, 2, 400)
            and tdSql.compareData(0, 3, 200)
        )