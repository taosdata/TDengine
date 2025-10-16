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

        Labels: common,ci

        Jira: None

        History:
            - 2025-7-10 Alex Duan Created
        """

        #
        #  main test
        #

        tdSql.execute(f"alter all dnodes 'debugflag 135';")

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
            "CREATE STREAM `tdasset`.`ana_stream3_sub3` event_window( start with `voltage` > 250 and `current` > 50 end with `voltage` <= 250 or `current` <= 50 ) TRUE_FOR(5m) FROM `asset01`.`em-3` STREAM_OPTIONS(PRE_FILTER(`phase`=1)) INTO `tdasset`.`result_stream3_sub3` AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`current`) AS `最小电流`, MAX(`current`) AS `最大电流`, MIN(`voltage`) AS `最小电压`, MAX(`voltage`) AS `最大电压`, SUM(`power`) AS `功率和` FROM %%trows",
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
            "CREATE STREAM `tdasset`.`ana_stream5_sub3` SESSION(ts, 10m) FROM `asset01`.`em-5`    STREAM_OPTIONS(PRE_FILTER(`voltage`>300)) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub3` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, LAST(`current`) AS `最后电流` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream5_sub4` SESSION(ts, 10m) FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname,`地址`        NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub4` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, LAST(`电流`) AS `最后电流` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream5_sub5` SESSION(ts, 10m) FROM `tdasset`.`vst_智能电表_1`                                  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub5` (ts, row PRIMARY KEY, wstart, wend, vol_min, power_sum)       AS SELECT _twstart, _twrownum, _twstart, _twend, MIN(`电压`), SUM(`功率`)   FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream5_sub6` SESSION(ts, 10m) FROM `tdasset`.`vst_智能电表_1`                                  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub6` (ts, vol PRIMARY KEY, wstart, wend, row, power, addr)         AS SELECT ts, `电压`, _twstart, _twend, _twrownum, `功率`, `地址`            FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream5_sub7` SESSION(ts, 10m) FROM `tdasset`.`vt_em-5`                                        NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub7` (ts, vol PRIMARY KEY, wstart, wend, row, power, addr)         AS SELECT ts, `电压`, _twstart, _twend, _twrownum, `功率`, `地址`            FROM %%trows",
            # stream6 count
            "CREATE STREAM `tdasset`.`ana_stream6`      COUNT_WINDOW(5)               FROM `tdasset`.`vt_em-6` STREAM_OPTIONS(PRE_FILTER(`电压`>=200)|IGNORE_DISORDER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6`      AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`ana_stream6_sub1` COUNT_WINDOW(5)               FROM `tdasset`.`vt_em-6` STREAM_OPTIONS(PRE_FILTER(`电压`>=200))                 NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub1` AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM `tdasset`.`ana_stream6_sub2` COUNT_WINDOW(5,5,`电流`)       FROM `tdasset`.`vt_em-6`                                                        NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub2` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend AND `电流` IS NOT NULL",
            "CREATE STREAM `tdasset`.`ana_stream6_sub3` COUNT_WINDOW(5,5,`电流`,`电压`) FROM `tdasset`.`vt_em-6`                                                        NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub3` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend AND (`电流` IS NOT NULL OR `电压` IS NOT NULL)",
            "CREATE STREAM `tdasset`.`ana_stream6_sub4` COUNT_WINDOW(5,5,`电流`,`电压`) FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname                              NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub4` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend AND (`电流` IS NOT NULL OR `电压` IS NOT NULL)",
            "CREATE STREAM `tdasset`.`ana_stream6_sub5` COUNT_WINDOW(5,5,`电流`)       FROM `tdasset`.`vt_em-6`                                                        NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub5` AS SELECT _twstart AS ts, COUNT(*) AS cnt, _twrownum as wrownum, COUNT(`电流`) as curcnt, COUNT(`电压`) as volcnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend AND `电流` IS NOT NULL",
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
            "CREATE STREAM `tdasset`.`ana_stream10_sub3` SLIDING(10s, 0s) FROM `asset01`.`em-10`                                                  STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|PRE_FILTER(`voltage`=100)) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub3` AS SELECT _tcurrent_ts AS ts,                                         COUNT(*) AS cnt, AVG(`voltage`) AS `平均电压`, SUM(`power`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream10_sub4` INTERVAL(10s) SLIDING(10s) FROM `asset01`.`em-10`                                        STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|PRE_FILTER(`voltage`=100)) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub4` AS SELECT _twstart AS ts,                                             COUNT(*) AS cnt, AVG(`voltage`) AS `平均电压`, SUM(`power`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream10_sub5` INTERVAL(10s) SLIDING(10s) FROM `tdasset`.`vt_em-10`                                     STREAM_OPTIONS(IGNORE_NODATA_TRIGGER)                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub5` AS SELECT _twstart AS ts,                                                 COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream10_sub6` SLIDING(10s, 1s) FROM `tdasset`.`vt_em-10`                                               STREAM_OPTIONS(IGNORE_NODATA_TRIGGER)                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub6` AS SELECT _tcurrent_ts AS ts, _tprev_ts AS prev_ts, _tnext_ts AS next_ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream10_sub7` INTERVAL(10s) SLIDING(10s) FROM `tdasset`.`vst_智能电表_1`  PARTITION BY tbname,`地址`     STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|DELETE_RECALC)         NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub7` AS SELECT _twend AS ts,                                             COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream10_sub8` INTERVAL(10s) SLIDING(10s) FROM asset01.electricity_meters PARTITION BY tbname,location  STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|DELETE_RECALC)         NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream10_sub8` AS SELECT _twend AS ts,                                             COUNT(*) AS cnt, AVG(`voltage`) AS `平均电压`, SUM(`power`) AS `功率和` FROM %%trows",
            # stream11 sliding window
            "CREATE STREAM `tdasset`.`ana_stream11`      INTERVAL(10s)     SLIDING(4s) FROM `tdasset`.`vt_em-11`                          STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO `tdasset`.`result_stream11`                                             AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream11_sub1` INTERVAL(10s, 1s) SLIDING(4s) FROM `tdasset`.`vt_em-11`                          STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO `tdasset`.`result_stream11_sub1`                                        AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream11_sub2` INTERVAL(10s, 4s) SLIDING(4s) FROM `tdasset`.`vt_em-11`                          STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO `tdasset`.`result_stream11_sub2`                                        AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream11_sub3` INTERVAL(5s,  1s) SLIDING(5s) FROM `tdasset`.`vt_em-11`                          STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO `tdasset`.`result_stream11_sub3`                                        AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream11_sub4` INTERVAL(5m)      SLIDING(5m) FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO `tdasset`.`result_stream11_sub4`                                        AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream11_sub5` INTERVAL(5m)      SLIDING(5m) FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO     `out`.`result_stream11_sub5` OUTPUT_SUBTABLE(CONCAT(tbname,'_out')) AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM `tdasset`.`ana_stream11_sub6` INTERVAL(5m)      SLIDING(5m) FROM `tdasset`.`vst_智能电表_1` PARTITION BY tbname STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO     `out`.`result_stream11_sub6` (ts,col2,col3,col4,col5,col6,col7)     AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
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
            # TD-37529
            "CREATE STREAM `tdasset`.`err_stream11_sub1` INTERVAL(10s, 1s) SLIDING(4s, 1s) FROM `tdasset`.`vt_em-11`  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream11_sub2_err` AS SELECT _twstart AS ts, _twend as wend, _twduration as wduration, _twrownum as wrownum, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
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
        self.verify_stream1()
        self.verify_stream2()
        self.verify_stream3()
        self.verify_stream4()
        self.verify_stream5()
        self.verify_stream6()
        self.verify_stream7()
        self.verify_stream8()
        self.verify_stream9()
        self.verify_stream10()
        self.verify_stream11()

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

        # stream4
        self.verify_stream4_again()
        # stream6
        self.verify_stream6_again()
        # stream7
        self.verify_stream7_again()
        # stream10
        self.verify_stream10_again()

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
            ts += 1 * 60 * 1000
            sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 300);"
            tdSql.execute(sql, show=True)

        ts += 1 * 60 * 1000
        sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 100);"
        tdSql.execute(sql, show=True)

        # voltage = 100
        ts += 1 * 60 * 1000
        sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 100);"
        tdSql.execute(sql, show=True)

        # voltage = 400
        for i in range(11):
            ts += 1 * 60 * 1000
            sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 400);"
            tdSql.execute(sql, show=True)

        # voltage = 100
        ts += 1 * 60 * 1000
        sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 100);"
        tdSql.execute(sql, show=True)

        # high-lower not trigger
        for i in range(30):
            ts += 1 * 60 * 1000
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
        power = 200
        phase = 0

        cnt = 11  #
        for i in range(cnt):
            ts += 1 * 60 * 1000
            current += 1
            voltage += 1
            power += 1
            phase += 1
            sql = f"insert into asset01.`em-2` values({ts}, {current}, {voltage}, {power}, {phase});"
            tdSql.execute(sql, show=True)

    #
    #  stream3 trigger
    #
    def trigger_stream3(self):
        ts = self.start2
        table = "asset01.`em-3`"
        step = 1 * 60 * 1000  # 1 minute
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
        step = 1 * 60 * 1000  # 1 minute
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
        step = 1 * 60 * 1000  # 1 minute
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
        step = 1 * 60 * 1000  # 1 minute

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
        disTs += 12 * step

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
    #  stream7 trigger
    #
    def trigger_stream7(self):
        ts = self.start2
        table = "asset01.`em-7`"
        step = 1 * 60 * 1000  # 1 minute
        cols = "ts,current,voltage,power"

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

        # update 500 -> 501
        count = 2
        fixedVals = "501, 501, 501"
        tdSql.insertFixedVal(table, self.updateTs7, step, count, cols, fixedVals)

    #
    #  stream7 trigger again
    #
    def trigger_stream7_again(self):
        # update
        table = "asset01.`em-7`"
        step = 1 * 60 * 1000  # 1 minute
        cols = "ts,current,voltage,power"

        # update 501->502
        count = 2
        fixedVals = "502, 502, 502"
        tdSql.insertFixedVal(table, self.updateTs7, step, count, cols, fixedVals)

        # del 400
        count = 2
        sql = f"delete from {table} where ts >= {self.delTs7} and ts < {self.delTs7 + count * step};"
        print(sql)
        tdSql.execute(sql)

    #
    #  stream8 trigger
    #
    def trigger_stream8(self):
        ts = self.start2
        table = "asset01.`em-8`"
        cols = "ts,current,voltage,power"
        fixedVals = "100, 200, 300"

        # insert order
        count = 10
        step = 200
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        # delete no affected
        # sql = f"delete from asset01.`em-8` where ts >= {self.start2}"
        # tdSql.execute(sql)

        time.sleep(5)

        # write order
        count = 20
        sleepS = 0.2  # 0.2 seconds
        tdSql.insertNow(table, sleepS, count, cols, fixedVals)

        # write disorder
        # count = 10
        # ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)

        # update no affected
        # updateVals = "1000, 2000, 3000"
        # step  = 300
        # count = 5
        # tdSql.insertFixedVal(table, self.start2, step, count, cols, updateVals)

    #
    #  stream9 trigger
    #
    def trigger_stream9(self):
        ts = self.start2
        table = "asset01.`em-9`"
        step = 100  # 100ms
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
        step = 1000
        count = 11
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # skip 10
        ts += step * 10

        # batch2
        count = 10
        vals = "200,200"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

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
        step = 1000
        count = 11
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # blank
        count = 10 * 60
        ts += count * step

        # write end
        count = 1
        vals = "200,400"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

    #
    # ---------------------   verify    ----------------------
    #

    #
    # verify stream1
    #
    def verify_stream1(self):
        # result_stream1
        result_sql = f"select * from tdasset.`result_stream1` "
        result_sql_sub1 = f"select * from tdasset.`result_stream1_sub1` "
        result_sql_sub2 = f"select * from tdasset.`result_stream1_sub2` "

        # result_stream1
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, 1752563060000)
            and tdSql.compareData(0, 1, 20)  # cnt
            and tdSql.compareData(0, 2, 300)
            and tdSql.compareData(1, 0, 1752564380000)
            and tdSql.compareData(1, 1, 11)  # cnt
            and tdSql.compareData(1, 2, 400),
        )

        # result_stream_sub1
        tdSql.checkResultsByFunc(
            sql=result_sql_sub1,
            func=lambda: tdSql.checkRows(17, show=True)
            and tdSql.compareData(0, 0, 1752563060000)
            and tdSql.compareData(0, 1, 0),  # cnt
        )

        # result_stream1_sub2
        tdSql.checkResultsBySql(sql=result_sql, exp_sql=result_sql_sub2)

        print("verify stream1 ................................. successfully.")

    #
    # verify stream2
    #
    def verify_stream2(self):
        # result_stream2
        result_sql = f"select * from tdasset.`result_stream2` "
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-07-15 14:05:00")
            and tdSql.compareData(1, 0, "2025-07-15 14:10:00")
            and tdSql.compareData(0, 1, 11)
            and tdSql.compareData(1, 1, 16),
        )

        print("verify stream2 ................................. successfully.")

    #
    # verify stream3
    #
    def verify_stream3(self):
        # result_stream3
        result_sql = f"select * from tdasset.`result_stream3` "
        ts = self.start2
        step = 1 * 60 * 1000  # 1 minute
        cnt = 5

        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 2)

        # check data
        data = [
            # ts           cnt  curmin curmax volmin volmax, sumpower
            [1752574200000, 10, 40, 60, 200, 350, 1000],
            [1752574800000, 16, 30, 80, 100, 500, 1600],
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream3 ................................. successfully.")

        # other sub
        self.verify_stream3_sub1()
        self.verify_stream3_sub2()
        self.verify_stream3_sub3()
        self.verify_stream3_sub4()
        self.verify_stream3_sub5()

        # sub6 ~ 8
        keys = ["stream3_sub6", "stream3_sub7", "stream3_sub8"]
        for key in keys:
            # found in log
            string = self.notifyFailed + key
            self.checkTaosdLog(string)
            # result table expect not create
            tdSql.error(f"select * from result_{key}")

    def verify_stream3_sub1(self, tbname="result_stream3_sub1"):
        # check
        result_sql = f"select * from tdasset.`{tbname}` "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 2)

        # check data
        data = [
            # ts           cnt  curmin curmax volmin volmax, sumpower
            [1752574200000, 10, 40, 60, 200, 350, 1000],
            [1752574800000, 16, 30, 80, 100, 500, 1600],
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream3_sub1 ............................ successfully.")

    def verify_stream3_sub2(self):
        # check
        result_sql = f"select * from tdasset.`result_stream3_sub2` "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 2)

        # check data
        data = [
            # ts           cnt  curmin curmax volmin volmax, sumpower
            [1752574200000, 10, 40, 60, 200, 350, 1000],
            [1752575160000, 10, 70, 80, 100, 500, 1000],
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream3_sub2 ............................ successfully.")

    def verify_stream3_sub3(self):
        # check
        result_sql = f"select * from tdasset.`result_stream3_sub3` "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 1)

        # check data
        data = [
            # ts           cnt  curmin curmax volmin volmax, sumpower
            [1752574200000, 10, 40, 60, 200, 350, 1000]
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream3_sub3 ............................ successfully.")

    def verify_stream3_sub4(self):
        # check
        result_sql = (
            f"select * from tdasset.`result_stream3_sub4` where tag_tbname='vt_em-3'"
        )
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 2)

        # check data
        data = [
            # ts           cnt  curmin curmax volmin volmax, sumpower
            [1752574200000, 10, 40, 60, 200, 350, 1000],
            [1752575160000, 10, 70, 80, 100, 500, 1000],
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream3_sub4 ............................ successfully.")

    def verify_stream3_sub5(self):
        # same with sub1
        self.verify_stream3_sub1(tbname="result_stream3_sub5")

    #
    # verify stream4
    #
    def verify_stream4(self):
        # result_stream4/result_stream4_sub1
        objects = [
            # table                 cnt avg  sum
            ["result_stream4", 10, 400, 2000],
            ["result_stream4_sub1", 10, 400, 2000],
        ]
        nrow = 11

        for obj in objects:
            tbname = obj[0]
            cnt = obj[1]
            avg = obj[2]
            sum = obj[3]

            result_sql = f"select * from tdasset.`{tbname}` "
            ts = self.start2
            for i in range(nrow):
                tdSql.checkResultsByFunc(
                    sql=result_sql,
                    func=lambda: tdSql.getRows() == nrow
                    and tdSql.compareData(i, 0, ts)
                    and tdSql.compareData(i, 1, cnt)
                    and tdSql.compareData(i, 2, avg)
                    and tdSql.compareData(i, 3, sum),
                )

                ts += 10 * 60 * 1000  # 10 minutes

        print("verify stream4 ................................. successfully.")
        print("verify stream4 sub1 ............................ successfully.")

        # verify stream4_sub2 ~ 6
        offsets = [
            10,  # a
            10 * 1000,  # s
            10 * 60 * 1000,  # m
            10 * 60 * 60 * 1000,  # h
            10 * 24 * 60 * 60 * 1000,  # d
        ]

        for i in range(2, 7):
            result_sql = f"select * from tdasset.`result_stream4_sub{i}` "
            tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 11)

            ts = self.start2 + offsets[i - 2]
            for j in range(tdSql.getRows()):
                tdSql.checkData(j, 0, ts)
                tdSql.checkData(j, 1, 10)
                tdSql.checkData(j, 2, 400)
                tdSql.checkData(j, 3, 2000)
                ts += 10 * 60 * 1000  # 10 minutes
        print("verify stream4_sub2 ~ 6 ........................ successfully.")

        # verify stream4_sub7
        self.verify_stream4_sub7()

        # verify stream4_sub8
        self.verify_stream4_sub8()

        # verify virtual table ts null
        self.check_vt_ts()

    def verify_stream4_sub7(self):
        # result_stream4_sub7
        wins = self.getSlidingWindow(self.start2, 1 * 60 * 60 * 1000, 1)
        result_sql = f"select * from tdasset.`result_stream4_sub7` "
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.checkData(0, 0, wins[0])
            and tdSql.checkData(0, 1, 10)
            and tdSql.checkData(0, 2, 400)
            and tdSql.checkData(0, 3, 10 * 200),
        )

    def verify_stream4_sub8(self):
        # result_stream4_sub8
        ts = 1752487860000
        result_sql = f"select * from tdasset.`result_stream4_sub8` "
        tdSql.checkResultsByFunc(
            sql=result_sql, func=lambda: tdSql.getRows() == 119, retry=120
        )

        for i in range(tdSql.getRows()):
            tdSql.checkData(i, 0, ts + i * (1 * 60 * 1000))
            tdSql.checkData(i, 1, i + 1)
            tdSql.checkData(i, 2, 400)
            tdSql.checkData(i, 3, (i + 1) * 200)

        print("verify stream4_sub8 ............................ successfully.")

    #
    # verify stream4 again
    #
    def verify_stream4_again(self):
        # verify
        objects = [
            # table                 cnt avg  sum
            ["result_stream4", 20, 300, 3000],
            ["result_stream4_sub1", 10, 400, 2000],
        ]

        nrow = 11
        for obj in objects:
            tbname = obj[0]
            cnt = obj[1]
            avg = obj[2]
            sum = obj[3]

            result_sql = f"select * from tdasset.`{tbname}` "
            ts = self.start2
            for i in range(nrow):
                tdSql.checkResultsByFunc(
                    sql=result_sql,
                    func=lambda: tdSql.getRows() == nrow
                    and tdSql.compareData(i, 0, ts)
                    and tdSql.compareData(i, 1, cnt)
                    and tdSql.compareData(i, 2, avg)
                    and tdSql.compareData(i, 3, sum),
                )

                ts += 10 * 60 * 1000  # 10 minutes

        print("verify stream4 again ........................... successfully.")

    #
    # verify stream5
    #
    def verify_stream5(self):
        # result_stream5
        result_sql = f"select * from tdasset.`result_stream5` "
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, self.start2)  # ts
            and tdSql.compareData(0, 1, 3 + 1 + 2)  # cnt
            and tdSql.compareData(0, 2, 32),  # last current
        )
        print("verify stream5 ................................. successfully.")

        # sub
        self.verify_stream5_sub1()
        self.verify_stream5_sub2()
        self.verify_stream5_sub3()
        self.verify_stream5_sub4()
        self.verify_stream5_sub5()
        self.verify_stream5_sub6()
        self.verify_stream5_sub7()

    def verify_stream5_sub1(self):
        # result_stream5_sub1
        result_sql = f"select * from tdasset.`result_stream5_sub1` "
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            # row1
            and tdSql.compareData(0, 0, self.start2)  # ts
            and tdSql.compareData(0, 1, 3 + 1 + 2 + 2)  # cnt
            and tdSql.compareData(0, 2, 37)  # last current
            # row2
            and tdSql.compareData(1, 0, 1752576120000)  # ts
            and tdSql.compareData(1, 1, 2)  # cnt
            and tdSql.compareData(1, 2, 39),  # last current
        )

        print("verify stream5 sub1 ............................ successfully.")

    def verify_stream5_sub2(self):
        # check
        result_sql = f"select * from tdasset.`result_stream5_sub2` "
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            # row1
            and tdSql.compareData(0, 0, self.start2)  # ts
            and tdSql.compareData(0, 1, 3)  # cnt
            and tdSql.compareData(0, 2, 30)  # last current
            # row2
            and tdSql.compareData(1, 0, 1752574980000)  # ts
            and tdSql.compareData(1, 1, 2 + 2)  # cnt
            and tdSql.compareData(1, 2, 37),  # last current
        )

        print("verify stream5 sub2 ............................ successfully.")

    def verify_stream5_sub3(self):
        # check
        result_sql = f"select * from tdasset.`result_stream5_sub3` "
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            # row1
            and tdSql.compareData(0, 0, self.start2)  # ts
            and tdSql.compareData(0, 1, 3)  # cnt
            and tdSql.compareData(0, 2, 30)  # last current
            # row2
            and tdSql.compareData(1, 0, 1752574980000)  # ts
            and tdSql.compareData(1, 1, 2 + 2)  # cnt
            and tdSql.compareData(1, 2, 37),  # last current
        )

        print("verify stream5 sub3 ............................ successfully.")

    def verify_stream5_sub4(self):
        # check same with sub1
        result_sql = (
            f"select * from tdasset.`result_stream5_sub4` where tag_tbname='vt_em-5' "
        )
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            # row1
            and tdSql.compareData(0, 0, self.start2)  # ts
            and tdSql.compareData(0, 1, 3 + 1 + 2 + 2)  # cnt
            and tdSql.compareData(0, 2, 37)  # last current
            # row2
            and tdSql.compareData(1, 0, 1752576120000)  # ts
            and tdSql.compareData(1, 1, 2)  # cnt
            and tdSql.compareData(1, 2, 39),  # last current
        )

        print("verify stream5 sub4 ............................ successfully.")

    def verify_stream5_sub5(self):
        # check
        result_sql = f"select ts,row,wend from tdasset.`result_stream5_sub5` "
        tdSql.checkResultsByFunc (
            sql=result_sql, 
            func=lambda: tdSql.getRows() >= 2
        )

        print("verify stream5 sub5 ............................ successfully.")

    def verify_stream5_sub6(self):    
        # result_stream5_sub1
        result_sql = f"select * from tdasset.`result_stream5_sub6` "
        print(result_sql)
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() > 100
        )

        print("verify stream5_sub6 ............................ successfully.")

    def verify_stream5_sub7(self):
        # check
        sql1 = "select ts,vol,power from tdasset.`result_stream5_sub7` "
        sql2 = "select ts,`电压`,`功率` from `vt_em-5` where ts >= 1752563000000 and ts <=1752576180000 order by ts "
        tdSql.checkResultsBySql(sql1, sql2)

        print("verify stream5 sub7 ............................ successfully.")

    #
    # verify stream6
    #
    def verify_stream6(self):
        # result_stream6
        result_sql = f"select * from tdasset.`result_stream6` "
        ts = self.start2
        step = 1 * 60 * 1000  # 1 minute
        cnt = 5

        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            # window1
            and tdSql.compareData(0, 0, ts)  # ts
            and tdSql.compareData(0, 1, 5)  # cnt
            and tdSql.compareData(0, 2, 200)  # min(voltage)
            and tdSql.compareData(0, 3, 204)  # max(voltage)
            # window2
            and tdSql.compareData(1, 0, ts + 5 * step)  # ts
            and tdSql.compareData(1, 1, 5)  # cnt
            and tdSql.compareData(1, 2, 205)  # min(voltage)
            and tdSql.compareData(1, 3, 209),  # max(voltage)
        )
        print("verify stream6 ................................. successfully.")

        # sub1
        exp_sql = f"select * from tdasset.`result_stream6_sub1` "
        tdSql.checkResultsBySql(result_sql, exp_sql)
        print("verify stream6_sub1 ............................ successfully.")

        # other sub
        self.verify_stream6_sub2()
        self.verify_stream6_sub3()
        self.verify_stream6_sub4()
        self.verify_stream6_sub5()

    def verify_stream6_sub2(self):
        # check
        result_sql = f"select * from tdasset.`result_stream6_sub2` "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 3)

        # check data
        data = [
            # ts          cnt wrow curcnt volcnt minvol, maxvol, sumpower
            [1752575700000, 5, 5, 5, 2, 10, 10, 5],
            [1752576240000, 5, 5, 5, 4, 60, 60, 5],
            [1752576660000, 5, 5, 5, 5, 60, 60, 5],
        ]
        tdSql.checkDataMem(result_sql, data)

        print("verify stream6_sub2 ............................ successfully.")

    def verify_stream6_sub3(self, sql=None):
        # check
        if sql is None:
            result_sql = f"select * from tdasset.`result_stream6_sub3` "
        else:
            result_sql = sql

        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 6)

        # check data
        data = [
            # ts          cnt wrow curcnt volcnt minvol, maxvol, sumpower
            [1752574200000, 5, 5, 0, 5, 200, 204, None],
            [1752574500000, 5, 5, 0, 5, 205, 209, None],
            [1752575400000, 5, 5, 2, 5, 10, 601, 3],
            [1752575880000, 5, 5, 2, 3, 20, 40, 5],
            [1752576180000, 5, 5, 5, 3, 60, 60, 5],
            [1752576600000, 5, 5, 5, 5, 60, 60, 5],
        ]
        tdSql.checkDataMem(result_sql, data)

        print("verify stream6_sub3 ............................ successfully.")

    def verify_stream6_sub4(self):
        # check super table
        result_sql = (
            f"select * from tdasset.`result_stream6_sub4`  where tag_tbname='vt_em-6' "
        )
        self.verify_stream6_sub3(result_sql)

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
        self.verify_stream6()

    #
    # verify stream7
    #
    def verify_stream7(self, checkSub=True):
        # check
        result_sql = f"select * from tdasset.`result_stream7` "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 3)

        # check data
        data = [
            # ts          cnt
            [1752574200000, 2, 100, 200],
            [1752574320000, 2, 200, 400],
            [1752574560000, 2, 400, 800],
        ]
        tdSql.checkDataMem(result_sql, data)

        # sub
        if checkSub:
            self.verify_stream7_sub1()
            self.verify_stream7_sub2()
            self.verify_stream7_sub3()

        print("verify stream7 ................................. successfully.")

    def verify_stream7_sub1(self):
        # check
        result_sql = f"select * from tdasset.`result_stream7_sub1` "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 5)

        # check data
        data = [
            # ts          cnt
            [1752574200000, 2, 100, 200],
            [1752574320000, 2, 200, 400],
            [1752574560000, 2, 400, 800],
            [1752574680000, 2, 501, 1002],
            [1752574800000, 2, 600, 1200],
        ]
        tdSql.checkDataMem(result_sql, data)

        print("verify stream7_sub1 ............................ successfully.")

    def verify_stream7_sub2(self):
        # check
        result_sql = f"select * from tdasset.`result_stream7_sub2` "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 2)

        # check data
        data = [
            # ts          cnt
            [1752574680000, 2, 501, 1002],
            [1752574800000, 2, 600, 1200],
        ]
        tdSql.checkDataMem(result_sql, data)

        print("verify stream7_sub2 ............................ successfully.")

    def verify_stream7_sub3(self):
        # check
        result_sql = f"select * from tdasset.`result_stream7_sub3` "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 1)

        # check data
        data = [
            # ts          cnt
            [1752574680000, 2, 501, 1002]
        ]
        tdSql.checkDataMem(result_sql, data)

        print("verify stream7_sub3 ............................ successfully.")

    #
    # verify stream7
    #
    def verify_stream7_again(self):

        # check
        self.verify_stream7(checkSub=False)

        # upate sub3
        result_sql = f"select * from tdasset.`result_stream7_sub3` "
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 1752574680000)
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 502)
            and tdSql.compareData(0, 3, 1004),
        )

        # ***** bug11 *****
        '''
        # del sub4
        result_sql = f"select * from tdasset.`result_stream7_sub4` "
        data = [
            # ts          cnt    
            [1752574200000, 2, 100, 200],
            [1752574320000, 2, 200, 400],
            [1752574560000, 2, 400, 800],
            [1752574680000, 2, 502, 1004],
            [1752574800000, 2, 600, 1200]
        ]        
        tdSql.checkResultsByFunc(
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == len(data)
        )
        # check data
        tdSql.checkDataMem(result_sql, data)
        '''

        # del sub5        
        result_sql = f"select * from tdasset.`result_stream7_sub5` "
        data = [
            # ts          cnt    
            [1752574560000, 2, 400, 800],
            [1752574680000, 2, 502, 1004]
        ]        
        tdSql.checkResultsByFunc(
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == len(data)
        )
        # check data
        tdSql.checkDataMem(result_sql, data)
        
        print("verify stream7_again ............................. successfully.")

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
        sum = 0

        for i in range(count):
            # row
            cnt = tdSql.getData(i, 1)
            sum += cnt

        if sum != 30:
            tdLog.exit(
                f"stream8 not found expected data. expected sum(cnt) == 30, actual: {sum}"
            )

        # sub
        self.verify_stream8_sub1()
        self.verify_stream8_sub2()
        self.verify_stream8_sub3()
        self.verify_stream8_sub4()
        self.verify_stream8_sub6()

        print("verify stream8 ................................. successfully.")


    # verify stream8_sub1
    def verify_stream8_sub1(self):
        # check sum
        tdSql.checkResultsByFunc(
            sql=f"select sum(cnt) from tdasset.`result_stream8_sub1`",
            func=lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 0, 30)
        )

    
        # check no data windows is zero
        tdSql.checkResultsByFunc (
            sql  = f"select count(*) from tdasset.`result_stream8_sub1` where cnt = 0 ",
            func = lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 0)
        )

        print("verify stream8_sub1 ............................ successfully.")

    # verify stream8_sub2
    def verify_stream8_sub2(self):
        # ts - ts_prev = 1000
        tdSql.checkResultsByFunc(
            sql="select (cast(ts as bigint) - cast(ts_prev as bigint) ) as dif from tdasset.result_stream8_sub2",
            func=lambda: tdSql.getRows() > 1,
        )
        for i in range(tdSql.getRows()):
            tdSql.checkData(i, 0, 1000)

        # ts_next - ts = 1000
        tdSql.checkResultsByFunc(
            sql="select (cast(ts_next as bigint) - cast(ts as bigint) ) as dif from tdasset.result_stream8_sub2",
            func=lambda: tdSql.getRows() > 1,
        )
        for i in range(tdSql.getRows()):
            tdSql.checkData(i, 0, 1000)

        print("verify stream8_sub2 ............................ successfully.")

    # verify stream8_sub3
    def verify_stream8_sub3(self):
        tdSql.checkResultsByFunc(
            sql="select sum(cnt) from tdasset.result_stream8_sub3",
            func=lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 0, 20),
        )

        print("verify stream8_sub3 ............................ successfully.")

    # verify stream8_sub4
    def verify_stream8_sub4(self):
        # check
        tdSql.checkResultsByFunc(
            sql="select * from result_stream8_sub4 where `平均电压` is  null",
            func=lambda: tdSql.getRows() >= 1,
        )

        # check data with sub5
        tdSql.checkResultsBySql(
            sql="select avg(`平均电压`), sum(`功率和`) from result_stream8_sub4",
            exp_sql="select avg(`平均电压`), sum(`功率和`) from result_stream8_sub5",
        )

        print("verify stream8_sub4-5 .......................... successfully.")

    def verify_stream8_sub6(self):
        # have log
        key = "stream8_sub6"
        string = self.notifyFailed + key
        self.checkTaosdLog(string)
        # result table expect not create
        tdSql.error(f"select * from result_{key}")

        print("verify stream8_sub6 ............................ successfully.")

    #
    # verify stream9
    #
    def verify_stream9(self):
        # result_stream9
        tdSql.checkResultsBySql(
            sql=f"select * from tdasset.`result_stream9` ",
            exp_sql=f"select ts,1,voltage,power from asset01.`em-9` where ts >= 1752574200000;",
        )
        print("verify stream9 ................................. successfully.")

    #
    # verify stream10
    #
    def verify_stream10(self):
        # result_stream10
        tdSql.checkResultsByFunc(
            sql=f"select * from tdasset.`result_stream10` ",
            func=lambda: tdSql.getRows() == 4
            # ts
            and tdSql.compareData(0, 0, 1752574200000)
            # cnt
            and tdSql.compareData(0, 3, 1)
            and tdSql.compareData(1, 3, 10)
            and tdSql.compareData(2, 3, 0)
            and tdSql.compareData(3, 3, 10),
        )

        # next_ts - ts = 10000
        tdSql.checkResultsByFunc(
            sql="select (cast(next_ts as bigint) - cast(ts as bigint) ) as dif from tdasset.result_stream10",
            func=lambda: tdSql.getRows() == 4,
        )
        for i in range(tdSql.getRows()):
            tdSql.checkData(i, 0, 10000)

        # ts - prev_ts = 10000
        tdSql.checkResultsByFunc(
            sql="select (cast(ts as bigint) - cast(prev_ts as bigint) ) as dif from tdasset.result_stream10",
            func=lambda: tdSql.getRows() == 4,
        )
        for i in range(tdSql.getRows()):
            tdSql.checkData(i, 0, 10000)

        print("verify stream10 ................................ successfully.")

        # sub
        self.verify_stream10_sub1()
        self.verify_stream10_sub2()
        self.verify_stream10_sub3()
        self.verify_stream10_sub4()
        self.verify_stream10_sub5()
        self.verify_stream10_sub6()
        self.verify_stream10_sub7()
        self.verify_stream10_sub8()

    def verify_stream10_sub1(self):
        # check
        tdSql.checkResultsByFunc(
            sql=f"select * from tdasset.`result_stream10_sub1` ",
            func=lambda: tdSql.getRows() == 3
            # ts
            and tdSql.compareData(0, 0, 1752574200000)
            # cnt
            and tdSql.compareData(0, 3, 1)
            and tdSql.compareData(1, 3, 10)
            and tdSql.compareData(2, 3, 10),
        )
        print("verify stream10_sub1 ........................... successfully.")

    def verify_stream10_sub2(self):
        # check
        tdSql.checkResultsByFunc(
            sql="select * from tdasset.`result_stream10_sub2` where tag_tbname='vt_em-10' ",
            func=lambda: tdSql.getRows() == 3
            # ts
            and tdSql.compareData(0, 0, 1752574200000)
            # cnt
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(1, 1, 10)
            and tdSql.compareData(2, 1, 10),
        )

        print("verify stream10_sub2 ........................... successfully.")

    def verify_stream10_sub3(self):
        # check
        tdSql.checkResultsByFunc(
            sql=f"select * from tdasset.`result_stream10_sub3`",
            func=lambda: tdSql.getRows() == 2
            # ts
            and tdSql.compareData(0, 0, 1752574200000)
            # cnt
            and tdSql.compareData(0, 1, 1) 
            and tdSql.compareData(1, 1, 10),
        )
        print("verify stream10_sub3 ........................... successfully.")

    def verify_stream10_sub4(self):
        # check
        tdSql.checkResultsByFunc(
            sql=f"select * from tdasset.`result_stream10_sub4` ",
            func=lambda: tdSql.getRows() == 2
            # row1
            and tdSql.compareData(0, 0, 1752574200000)  # ts
            and tdSql.compareData(0, 1, 10)             # cnt
            and tdSql.compareData(0, 2, 100)            # avg
            and tdSql.compareData(0, 3, 2000)           # sum
            # row2
            and tdSql.compareData(1, 0, 1752574210000)  # ts
            and tdSql.compareData(1, 1, 1)              # cnt
            and tdSql.compareData(1, 2, 100)            # avg
            and tdSql.compareData(1, 3, 200)            # sum
        )
        print("verify stream10_sub4 ........................... successfully.")

    def verify_stream10_sub5(self):
        # check
        tdSql.checkResultsByFunc(
            sql=f"select * from tdasset.`result_stream10_sub5` ",
            func=lambda: tdSql.getRows() == 3
            # row1
            and tdSql.compareData(0, 0, 1752574200000)  # ts
            and tdSql.compareData(0, 1, 10)  # cnt
            and tdSql.compareData(0, 2, 100)  # avg
            and tdSql.compareData(0, 3, 2000)  # sum
            # row2
            and tdSql.compareData(1, 0, 1752574210000)  # ts
            and tdSql.compareData(1, 1, 1)  # cnt
            and tdSql.compareData(1, 2, 100)  # avg
            and tdSql.compareData(1, 3, 200)  # sum
            # row1
            and tdSql.compareData(2, 0, 1752574220000)  # ts
            and tdSql.compareData(2, 1, 9)  # cnt
            and tdSql.compareData(2, 2, 200)  # avg
            and tdSql.compareData(2, 3, 1800),  # sum
        )
        print("verify stream10_sub5 ........................... successfully.")

    def verify_stream10_sub6(self):
        # check
        result_sql = "select ts,cnt from tdasset.`result_stream10_sub6` "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 3)

        # check data
        data = [
            # ts          cnt
            [1752574201000, 2],
            [1752574211000, 9],
            [1752574221000, 1],
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream10_sub6 ........................... successfully.")

    def verify_stream10_sub7(self):
        # check
        tdSql.checkResultsByFunc(
            sql=f"select * from tdasset.`result_stream10_sub7` where tag_tbname='vt_em-11' ",
            func=lambda: tdSql.getRows() == 2,
        )

        print("verify stream10_sub7 ........................... successfully.")

    def verify_stream10_sub8(self):
        # check
        tdSql.checkResultsByFunc(
            sql=f"select * from tdasset.`result_stream10_sub8` where tag_tbname='em-11' ",
            func=lambda: tdSql.getRows() == 2,
        )

        #  sub7 == sub8
        cols = "ts,cnt,`平均电压`,`功率和`"
        sql1 = (
            f"select {cols} from tdasset.`result_stream10_sub7` order by tag_tbname,ts"
        )
        sql2 = (
            f"select {cols} from tdasset.`result_stream10_sub8` order by tag_tbname,ts"
        )
        tdSql.checkResultsBySql(
            sql1,
            sql2,
        )

        print("verify stream10_sub8 ........................... successfully.")

    #
    # verify stream10 again
    #
    def verify_stream10_again(self):
        # check drop child table
        tdSql.checkResultsByFunc(
            sql=f"select * from tdasset.`result_stream10_sub7` where tag_tbname='vt_em-11' ",
            func=lambda: tdSql.getRows() == 2
        )
        tdSql.checkResultsByFunc(
            sql=f"select * from tdasset.`result_stream10_sub8` where tag_tbname='em-11' ",
            func=lambda: tdSql.getRows() == 2
        )

    #
    # verify stream11
    #
    def verify_stream11(self):
        # check row
        result_sql = "select * from tdasset.`result_stream11` "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 5)
        # check data
        data = [
            # _twstart        _twend       dura  wrowcnt,cnt, avg, sum
            [1752574192000, 1752574202000, 10000, 2, 2, 100, 400],
            [1752574196000, 1752574206000, 10000, 6, 6, 100, 1200],
            [1752574200000, 1752574210000, 10000, 10, 10, 100, 2000],
            [1752574204000, 1752574214000, 10000, 7, 7, 100, 1400],
            [1752574208000, 1752574218000, 10000, 3, 3, 100, 600],
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream11 ................................ successfully.")

        # sub
        self.verify_stream11_sub1()
        self.verify_stream11_sub2()
        self.verify_stream11_sub3()
        self.verify_stream11_sub4()
        self.verify_stream11_sub5()
        self.verify_stream11_sub6()

    def verify_stream11_sub1(self):
        # check
        result_sql = "select * from tdasset.`result_stream11_sub1` "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 5)

        # check data
        data = [
            # _twstart        _twend       dura  wrowcnt,cnt, avg, sum
            [1752574193000, 1752574203000, 10000, 3, 3, 100, 600],
            [1752574197000, 1752574207000, 10000, 7, 7, 100, 1400],
            [1752574201000, 1752574211000, 10000, 10, 10, 100, 2000],
            [1752574205000, 1752574215000, 10000, 6, 6, 100, 1200],
            [1752574209000, 1752574219000, 10000, 2, 2, 100, 400],
        ]
        tdSql.checkDataMem(result_sql, data)

        print("verify stream11_sub1 ........................... successfully.")

    def verify_stream11_sub2(self):
        # check
        tdSql.checkResultsBySql(
            sql="select * from tdasset.`result_stream11_sub2` ",
            exp_sql="select * from tdasset.`result_stream11` ",
        )

        print("verify stream11_sub2 ........................... successfully.")

    def verify_stream11_sub3(self):
        # check
        result_sql = "select * from tdasset.`result_stream11_sub3` "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 3)

        # check data
        data = [
            # _twstart        _twend     dura wrowcnt,cnt, avg, sum
            [1752574196000, 1752574201000, 5000, 1, 1, 100, 200],
            [1752574201000, 1752574206000, 5000, 5, 5, 100, 1000],
            [1752574206000, 1752574211000, 5000, 5, 5, 100, 1000],
        ]
        tdSql.checkDataMem(result_sql, data)

        print("verify stream11_sub3 ........................... successfully.")

    def verify_stream11_sub4(self):
        # check
        result_sql = (
            "select * from tdasset.`result_stream11_sub4` where tag_tbname='vt_em-11' "
        )
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 1)

        # check data
        data = [
            # _twstart        _twend        dura wrowcnt,cnt, avg, sum,  tag_tbname
            [1752574200000, 1752574500000, 300000, 11, 11, 100, 2200, "vt_em-11"]
        ]
        tdSql.checkDataMem(result_sql, data)

        print("verify stream11_sub4 ........................... successfully.")

    def verify_stream11_sub5(self):
        # check
        result_sql = "show out.tables like 'vt_em-%_out' "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 9)
        print("verify stream11_sub5 ........................... successfully.")

    def verify_stream11_sub6(self):
        # check
        sql1 = "select * from out.`result_stream11_sub5` order by ts,tag_tbname "
        sql2 = "select * from out.`result_stream11_sub6` order by ts,tag_tbname "
        tdSql.checkResultsBySql(sql1, sql2)
        print("verify stream11_sub6 ........................... successfully.")
