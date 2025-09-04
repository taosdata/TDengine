import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, etool, sc, eutil
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

        # check errors
        self.checkErrors()

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
        self.notifyFailed = "failed to get stream notify handle of ws://idmp:6042/recv/?key="

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
            "CREATE VTABLE `vt_wm-1`  (`流量` FROM `asset01`.`wm-1`.`rate`, `水压` FROM `asset01`.`wm-1`.`pressure`) USING `vst_智能水表_1` (`_ignore_path`, `地址`, `path1`) TAGS (NULL, '北京.朝阳.三元桥街道', '公共事业.北京.朝阳.三元桥街道');"
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} vtable successfully.")
        

    # 
    # 2. create streams
    #
    def createStreams(self):

        sqls = [

            # stream1 event 
            "CREATE STREAM `tdasset`.`ana_stream1`       event_window( start with `电压` > 250 end with `电压` <= 250 ) TRUE_FOR(10m) FROM `tdasset`.`vt_em-1`                                          NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream1`      AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, avg(`电压`) AS `平均电压`  FROM tdasset.`vt_em-1`  WHERE ts >= _twstart AND ts <_twend;",
            "CREATE STREAM `tdasset`.`ana_stream1_sub1`  event_window( start with `电压` > 250 end with `电压` <= 250 ) TRUE_FOR(10m) FROM `tdasset`.`vt_em-1` STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN))  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream1_sub1` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, avg(`电压`) AS `平均电压`  FROM tdasset.`vt_em-1`  WHERE ts >= _twstart AND ts <_twend;",
            "CREATE STREAM `tdasset`.`ana_stream1_sub2`  event_window( start with `电压` > 250 end with `电压` <= 250 ) TRUE_FOR(10m) FROM `tdasset`.`vt_em-1` STREAM_OPTIONS(EVENT_TYPE(WINDOW_CLOSE)) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_CLOSE)             INTO `tdasset`.`result_stream1_sub2` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, avg(`电压`) AS `平均电压`  FROM tdasset.`vt_em-1`  WHERE ts >= _twstart AND ts <_twend;",
            # stream2 sliding window
            "CREATE STREAM `tdasset`.`ana_stream2`  interval(1h)  sliding(5m) FROM `tdasset`.`vt_em-2`  notify('ws://idmp:6042/eventReceive') ON(window_open|window_close) INTO `tdasset`.`result_stream2` AS SELECT _twstart+0s AS ts, max(`电流`) AS `最大电流` FROM tdasset.`vt_em-2`  WHERE ts >=_twstart AND ts <=_twend;",
            
            # stream3 event
            "CREATE STREAM `tdasset`.`ana_stream3`      event_window( start with `电压` > 250 end with `电压` <= 250 )                                TRUE_FOR(5m) FROM `tdasset`.`vt_em-3`                                      NOTIFY('ws://idmp:6042/eventReceive')          ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream3`      AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电流`) AS `最小电流`, MAX(`电流`) AS `最大电流`, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-3` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`ana_stream3_sub1` event_window( start with `电压` > 250 end with `电压` <= 250 )                                TRUE_FOR(5m) FROM `tdasset`.`vt_em-3`                                      NOTIFY('ws://idmp:6042/eventReceive')          ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream3_sub1` AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电流`) AS `最小电流`, MAX(`电流`) AS `最大电流`, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream3_sub2` event_window( start with `电压` > 250 and `电流` > 50 end with `电压` <= 250 or `电流` <= 50 ) TRUE_FOR(5m) FROM `tdasset`.`vt_em-3`                                      NOTIFY('ws://idmp:6042/eventReceive')          ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream3_sub2` AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电流`) AS `最小电流`, MAX(`电流`) AS `最大电流`, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream3_sub3` event_window( start with `电压` > 250 and `电流` > 50 end with `电压` <= 250 or `电流` <= 50 ) TRUE_FOR(5m) FROM `tdasset`.`vt_em-3` STREAM_OPTIONS(PRE_FILTER(`相位`=1))  NOTIFY('ws://idmp:6042/eventReceive')          ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream3_sub3` AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电流`) AS `最小电流`, MAX(`电流`) AS `最大电流`, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream3_sub4` event_window( start with `电压` > 250 and `电流` > 50 end with `电压` <= 250 or `电流` <= 50 ) TRUE_FOR(5m) FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname             NOTIFY('ws://idmp:6042/eventReceive')          ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream3_sub4` AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电流`) AS `最小电流`, MAX(`电流`) AS `最大电流`, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream3_sub5` event_window( start with `电压` > 250 end with `电压` <= 250 )                                TRUE_FOR(5m) FROM `tdasset`.`vt_em-3`                                                                                                                   INTO `tdasset`.`result_stream3_sub5` AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电流`) AS `最小电流`, MAX(`电流`) AS `最大电流`, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream3_sub6` event_window( start with `电压` > 250 end with `电压` <= 250 )                                TRUE_FOR(5m) FROM `tdasset`.`vt_em-3`                                      NOTIFY('ws://idmp:6042/recv/?key=stream3_sub6') ON(WINDOW_OPEN|WINDOW_CLOSE)",
            "CREATE STREAM `tdasset`.`ana_stream3_sub7` event_window( start with `电压` > 250 end with `电压` <= 250 )                                TRUE_FOR(5m) FROM `tdasset`.`vt_em-3`                                      NOTIFY('ws://idmp:6042/recv/?key=stream3_sub7') ON(WINDOW_CLOSE)            ",
            "CREATE STREAM `tdasset`.`ana_stream3_sub8` event_window( start with `电压` > 250 end with `电压` <= 250 )                                TRUE_FOR(5m) FROM `tdasset`.`vt_em-3`                                      NOTIFY('ws://idmp:6042/recv/?key=stream3_sub8') ON(WINDOW_OPEN)             ",

            # stream4 sliding window
            "CREATE STREAM `tdasset`.`ana_stream4`      INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4`                                                  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4`      AS SELECT _twstart + 0s  as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <_twend ",
            "CREATE STREAM `tdasset`.`ana_stream4_sub1` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER)                  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub1` AS SELECT _twstart + 0s  as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream4_sub2` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub2` AS SELECT _twstart + 10a as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <_twend",
            "CREATE STREAM `tdasset`.`ana_stream4_sub3` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub3` AS SELECT _twstart + 10s as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <_twend",
            "CREATE STREAM `tdasset`.`ana_stream4_sub4` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub4` AS SELECT _twstart + 10m as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <_twend",
            "CREATE STREAM `tdasset`.`ana_stream4_sub5` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub5` AS SELECT _twstart + 10h as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <_twend",
            "CREATE STREAM `tdasset`.`ana_stream4_sub6` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub6` AS SELECT _twstart + 10d as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <_twend",
            "CREATE STREAM `tdasset`.`ana_stream4_sub7` INTERVAL(600s) SLIDING(1h)  FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub7` AS SELECT _twstart       as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts < _twend AND ts >= 1752574200000",
            "CREATE STREAM `tdasset`.`ana_stream4_sub8` INTERVAL(1d)   SLIDING(60s) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub8` AS SELECT _twstart       as ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts < _twend AND ts >= 1752574200000",

            # stream5 session
            "CREATE STREAM `tdasset`.`ana_stream5`      SESSION(ts, 10m) FROM `tdasset`.`vt_em-5` STREAM_OPTIONS(IGNORE_DISORDER)       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5`      AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, LAST(`电流`) AS `最后电流` FROM tdasset.`vt_em-5` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`ana_stream5_sub1` SESSION(ts, 10m) FROM `tdasset`.`vt_em-5`                                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub1` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, LAST(`电流`) AS `最后电流` FROM tdasset.`vt_em-5` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`ana_stream5_sub2` SESSION(ts, 10m) FROM `tdasset`.`vt_em-5` STREAM_OPTIONS(PRE_FILTER(`电压`>300)) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub2` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, LAST(`电流`) AS `最后电流` FROM tdasset.`vt_em-5` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`ana_stream5_sub3` SESSION(ts, 10m) FROM `tdasset`.`vt_em-5` STREAM_OPTIONS(PRE_FILTER(`电压`>300)) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub3` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, LAST(`电流`) AS `最后电流` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream5_sub4` SESSION(ts, 10m) FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname,`地址`        NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub4` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, LAST(`电流`) AS `最后电流` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream5_sub5` SESSION(ts, 10m) FROM `tdasset`.`vst_智能电表_1`                                  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub5` (ts, cnt PRIMARY KEY, col3, col4,col5,col6,col7)  AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, _twrownum as wrownum, _twend as twend, FIRST(`电压`) AS `开始电压`, LAST(`电压`) AS `最后电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream5_sub6` SESSION(ts, 10m) FROM `tdasset`.`vst_智能电表_1`                                  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub6` (ts, col2 PRIMARY KEY, col3) AS SELECT ts,`电压`,`电流` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream5_sub7` SESSION(ts, 10m) FROM `tdasset`.`vst_智能电表_1`                                  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub6` (ts, col2 PRIMARY KEY, col3,col4,col5) AS SELECT ts,tbname,`电压`,`电流`,`地址` FROM `tdasset`.`vst_智能电表_1` WHERE ts >=_twstart AND ts <_twend",

            # stream6 count
            "CREATE STREAM `tdasset`.`ana_stream6`      COUNT_WINDOW(5)               FROM `tdasset`.`vt_em-6` STREAM_OPTIONS(PRE_FILTER(`电压`>=200)|IGNORE_DISORDER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6`      AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`ana_stream6_sub1` COUNT_WINDOW(5)               FROM `tdasset`.`vt_em-6` STREAM_OPTIONS(PRE_FILTER(`电压`>=200))                 NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub1` AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`ana_stream6_sub2` COUNT_WINDOW(5,5,`电流`)       FROM `tdasset`.`vt_em-6`                                                        NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub2` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream6_sub3` COUNT_WINDOW(5,5,`电流`,`电压`) FROM `tdasset`.`vt_em-6`                                                        NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub3` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream6_sub4` COUNT_WINDOW(5,5,`电流`,`电压`) FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname                              NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub4` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream6_sub5` COUNT_WINDOW(5,5,`电流`)       FROM `tdasset`.`vt_em-6` STREAM_OPTIONS(DELETE_RECALC)                          NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub5` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend",

            # stream7 state
            "CREATE STREAM `tdasset`.`ana_stream7`      STATE_WINDOW(`电压`) TRUE_FOR(60s) FROM `tdasset`.`vt_em-7` STREAM_OPTIONS(IGNORE_DISORDER)                                    NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream7`      AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-7` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`ana_stream7_sub1` STATE_WINDOW(`电压`) TRUE_FOR(59s) FROM `tdasset`.`vt_em-7`                                                                    NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream7_sub1` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream7_sub2` STATE_WINDOW(`电压`) TRUE_FOR(60s) FROM `tdasset`.`vt_em-7` STREAM_OPTIONS(PRE_FILTER(`电压`>400))                              NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream7_sub2` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-7` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`ana_stream7_sub3` STATE_WINDOW(`电压`) TRUE_FOR(60s) FROM `tdasset`.`vt_em-7` STREAM_OPTIONS(PRE_FILTER(`电流`>400 and `电流`<=600))               NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream7_sub3` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-7` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`ana_stream7_sub4` STATE_WINDOW(`电压`) TRUE_FOR(60s) FROM `tdasset`.`vt_em-7` STREAM_OPTIONS(DELETE_RECALC)                                      NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream7_sub4` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-7` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`ana_stream7_sub5` STATE_WINDOW(`电压`) TRUE_FOR(60s) FROM `tdasset`.`vt_em-7` STREAM_OPTIONS(PRE_FILTER(`电流`>300 and `电流`<=600)|DELETE_RECALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream7_sub5` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-7` WHERE ts >= _twstart AND ts <=_twend",

            # stream8 period
            "CREATE STREAM `tdasset`.`ana_stream8`      PERIOD(1s, 0s) FROM `tdasset`.`vt_em-8`                                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8`      AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts,                                                                                                                 COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream8_sub1` PERIOD(1s, 0s) FROM `tdasset`.`vt_em-8` STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8_sub1` AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts,                                                                                                                 COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream8_sub2` PERIOD(1s, 0s) FROM `tdasset`.`vt_em-8`                                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8_sub2` AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts, CAST(_tprev_localtime/1000000 as timestamp) AS ts_prev, CAST(_tnext_localtime/1000000 as timestamp) AS ts_next, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream8_sub3` PERIOD(1s, 0s)                                                                NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8_sub3` AS SELECT (CAST(_tlocaltime/1000000 as timestamp) - 10000) AS ts, (CAST(_tprev_localtime/1000000 as timestamp) - 10000) AS ts_prev, (CAST(_tnext_localtime/1000000 as timestamp) - 10000) AS ts_next, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-8` WHERE ts >= CAST(_tlocaltime/1000000 as timestamp) - 10000 AND ts < CAST(_tnext_localtime/1000000 as timestamp) - 10000",
            "CREATE STREAM `tdasset`.`ana_stream8_sub4` PERIOD(1s, 0s) FROM `tdasset`.`vt_em-8` STREAM_OPTIONS(FORCE_OUTPUT)          NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8_sub4` AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts,                                                                                                                                  AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream8_sub5` PERIOD(1s, 0s) FROM `tdasset`.`vt_em-8`                                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8_sub5` AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts,                                                                                                                                  AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream8_sub6` PERIOD(1s, 0s) FROM `tdasset`.`vt_em-8` STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) NOTIFY('ws://idmp:6042/recv/?key=stream8_sub6') ",

            # stream9 sliding window
            "CREATE STREAM `tdasset`.`ana_stream9` INTERVAL(1a) SLIDING(1a) FROM `tdasset`.`vt_em-9` STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream9` AS SELECT _twstart as ts,COUNT(*) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-9` WHERE ts >=_twstart AND ts <=_twend AND ts >= 1752574200000",

            # stream10 sliding
            "CREATE STREAM `tdasset`.`ana_stream10`      SLIDING(10s, 0s) FROM `tdasset`.`vt_em-10`                                                                                            NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10`      AS SELECT _tcurrent_ts AS ts, _tprev_ts AS prev_ts, _tnext_ts AS next_ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream10_sub1` SLIDING(10s, 0s) FROM `tdasset`.`vt_em-10`                                               STREAM_OPTIONS(IGNORE_NODATA_TRIGGER)                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub1` AS SELECT _tcurrent_ts AS ts, _tprev_ts AS prev_ts, _tnext_ts AS next_ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream10_sub2` SLIDING(10s, 0s) FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname,`地址`                STREAM_OPTIONS(IGNORE_NODATA_TRIGGER)                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub2` AS SELECT _tcurrent_ts AS ts,                                             COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream10_sub3` SLIDING(10s, 0s) FROM `tdasset`.`vt_em-10`                                               STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|PRE_FILTER(`电压`=100)) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub3` AS SELECT _tcurrent_ts AS ts,                                             COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream10_sub4` INTERVAL(10s) SLIDING(10s) FROM `tdasset`.`vt_em-10`                                     STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|PRE_FILTER(`电压`=100)) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub4` AS SELECT _twstart AS ts,                                                 COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream10_sub5` INTERVAL(10s) SLIDING(10s) FROM `tdasset`.`vt_em-10`                                     STREAM_OPTIONS(IGNORE_NODATA_TRIGGER)                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub5` AS SELECT _twstart AS ts,                                                 COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream10_sub6` SLIDING(10s, 1s) FROM `tdasset`.`vt_em-10`                                               STREAM_OPTIONS(IGNORE_NODATA_TRIGGER)                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub6` AS SELECT _tcurrent_ts AS ts, _tprev_ts AS prev_ts, _tnext_ts AS next_ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream10_sub7` INTERVAL(10s) SLIDING(10s) FROM `tdasset`.`vst_智能电表_1`  PARTITION BY tbname,`地址`      STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|DELETE_RECALC)         NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub7` AS SELECT _tcurrent_ts AS ts,                                             COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream10_sub8` INTERVAL(10s) SLIDING(10s) FROM asset01.electricity_meters PARTITION BY tbname,location  STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|DELETE_RECALC)         NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub8` AS SELECT _tcurrent_ts AS ts,                                             COUNT(*) AS cnt, AVG(`voltage`) AS `平均电压`, SUM(`power`) AS `功率和` FROM %%trows",

            # stream11 sliding window
            "CREATE STREAM `tdasset`.`ana_stream11`      INTERVAL(10s)     SLIDING(4s) FROM `tdasset`.`vt_em-11`                          STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO `tdasset`.`result_stream11`                                             AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream11_sub1` INTERVAL(10s, 1s) SLIDING(4s) FROM `tdasset`.`vt_em-11`                          STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO `tdasset`.`result_stream11_sub1`                                        AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream11_sub2` INTERVAL(10s, 4s) SLIDING(4s) FROM `tdasset`.`vt_em-11`                          STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO `tdasset`.`result_stream11_sub2`                                        AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream11_sub3` INTERVAL(5s,  1s) SLIDING(5s) FROM `tdasset`.`vt_em-11`                          STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO `tdasset`.`result_stream11_sub3`                                        AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream11_sub4` INTERVAL(5m)      SLIDING(5m) FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO `tdasset`.`result_stream11_sub4`                                        AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream11_sub5` INTERVAL(5m)      SLIDING(5m) FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO     `out`.`result_stream11_sub5` OUTPUT_SUBTABLE(CONCAT(tbname,'_out')) AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream11_sub6` INTERVAL(5m)      SLIDING(5m) FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO     `out`.`result_stream11_sub6` (col1,col2,col3,col4,col5,col6,col7)   AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} streams successfully.")

    #
    #  check errors
    #
    def checkErrors(self):

        sqls = [
            # stream11 sliding window
            "CREATE STREAM `tdasset`.`err_stream6_sub1` COUNT_WINDOW(5, 5,`电流`, `地址`)  FROM `tdasset`.`vt_em-6`                                 STREAM_OPTIONS(IGNORE_DISORDER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream_err` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`err_stream6_sub2` COUNT_WINDOW(5,5,`电流`, tbname)  FROM `tdasset`.`vt_em-6`                                 STREAM_OPTIONS(IGNORE_DISORDER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream_err` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`err_stream6_sub3` COUNT_WINDOW(5,5,`电流`,`电压`)    FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname,`电压`                                 NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream_err` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`err_stream6_sub4` COUNT_WINDOW(5,5,`电流`,`电压`)    FROM `tdasset`.`vst_智能电表_1` PARTITION BY `地址`                                        NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream_err` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`err_stream6_sub5` COUNT_WINDOW(5,5,`电流`,`电压`) FROM `tdasset`.`vst_智能电表_1`                                                              NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream_err` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            # ***** bug TD-37665 *****
            #"CREATE STREAM `tdasset`.`err_stream6_sub5` COUNT_WINDOW(5,5,`电流`,`电压`)   FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname,`地址`                                 NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub4` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM %%trows",

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
        # stream11
        self.trigger_stream11()


    # 
    # 5. verify results
    #
    def verifyResults(self):
        print("wait 10s ...")
        time.sleep(10)
        print("verifyResults ...")
        self.verify_stream5_sub6()


    # 
    # 6. write trigger data again
    #
    def writeTriggerDataAgain(self):
        print("writeTriggerDataAgain ...")
        # stream4 
        self.trigger_stream4_again()
        # stream6
        self.trigger_stream6_again()
        # stream7
        self.trigger_stream7_again()
        # stream10
        self.trigger_stream10_again()


    # 
    # 7. verify results again
    #
    def verifyResultsAgain(self):
        # wait for stream processing
        time.sleep(3)
        print("verifyResultsAgain ...")


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
        ts = self.start2
        table = "asset01.`em-3`"
        step  = 1 * 60 * 1000 # 1 minute
        cols = "ts,current,voltage,phase,power"

        # high
        count = 3
        vals = "60,350,1,100"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # null
        count = 3
        vals = "null,null,1,100"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # high
        count = 3
        vals = "60,350,1,100"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # lower
        count = 1
        vals = "40,200,1,100"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)


        # high
        count = 3
        vals = "30,400,1,100"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # null
        count = 3
        vals = "null,null,1,100"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # high
        count = 3
        vals = "70,400,1,100"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        count = 3
        vals = "70,null,1,100"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        count = 3
        vals = "80,500,0,100"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # lower
        count = 1
        vals = "null,100,0,100"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        count = 3
        vals = "90,600,1,100"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        count = 1
        vals = "10,100,0,100"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)


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

        # blank 9
        ts += 9 * step

        # boundary of first window
        count = 1
        vals = "31,300,200"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)
        
        # last
        count = 2
        vals = "32,401,201"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # save span ts
        spanTs = ts

        # blank 30 triggered      
        ts += 30 * step

        # trigger first windows close
        count = 2
        vals = "40,500,300"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # disorder write 2 rows from spanTs
        count = 2
        disTs = spanTs + 3 * step
        orderVals = [36, 406, 206]
        disTs = tdSql.insertOrderVal(table, disTs, step, count, cols, orderVals)

        # blank 12
        disTs +=  12 * step

        # disorder write 1 rows
        count = 2
        vals = "39,200,200"
        disTs = tdSql.insertFixedVal(table, disTs, step, count, cols, vals)

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

        # delete
        count = 15
        sql = f"delete from {table} where ts >= {self.start2} and ts < {self.start2 + count * step}"
        print(sql)
        tdSql.execute(sql)

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
    #  stream10 trigger again
    #
    def trigger_stream10_again(self):
        # drop em-11
        sql = "drop table asset01.`em-11`;"
        tdSql.execute(sql)
        print(sql)

    #
    #  stream11 trigger 
    #
    def trigger_stream11(self):
        ts = self.start2
        table = "asset01.`em-11`"
        cols = "ts,voltage,power"
        vals = "100,200"

        # batch1
        step  = 1000
        count = 11
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # blank
        count = 10 * 60
        ts +=  count * step

        # write end
        count = 1
        vals = "200,400"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

    #
    # ---------------------   verify    ----------------------
    #

    def verify_stream5_sub6(self):    
        # result_stream5_sub1
        result_sql = f"select * from tdasset.`result_stream5_sub6` "
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() > 0
        )
        print("verify stream5_sub6 ............................ successfully.")


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