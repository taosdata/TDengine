import time
import math
import random
import threading
from datetime import datetime
from datetime import date

from new_test_framework.utils import tdLog, tdSql, tdStream, etool
from new_test_framework.utils.srvCtl import *


class Test_BigPress:

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

        # prepare data
        self.prepare()

        # create vtables
        self.createVtables()

        # create streams
        self.createStreams()

        # check stream status
        self.checkStreamStatus()

        # write with taosBenchmark
        self.startWriteJob()

        # restart dnode
        self.startRestartJob()

        # verify results
        self.verifyResults()


    #
    # ---------------------   main flow frame    ----------------------
    #

    # 
    # prepare data
    #
    def prepare(self):
        # name
        self.start   = 1752600000000
        self.threads = []

        # create snode
        for i in range(5):
            tdSql.execute(f"create snode on dnode {i + 1}", show = True)

        # create meters db
        etool.benchmark(f"-f cases/41-StreamProcessing/20-UseCase/json/idmp_meters.json")
        tdLog.info(f"import data to db: asset01 successfully.")

        # create vehicle db
        etool.benchmark(f"-f cases/41-StreamProcessing/20-UseCase/json/idmp_vehicle.json")
        tdLog.info(f"import data to db: vehicle successfully.")

    # 
    # 1. create vtables
    #
    def createVtables(self):
        # meters
        sqls = [
            "create database tdasset vgroups 4 replica 3;",
            "use tdasset;",
            "CREATE STABLE `vst_智能电表_1` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `电流` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `电压` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `功率` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `相位` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium') TAGS (`_ignore_path` VARCHAR(20), `地址` VARCHAR(50), `单元` TINYINT, `楼层` TINYINT, `设备ID` VARCHAR(20), `path1` VARCHAR(512)) SMA(`ts`,`电流`) VIRTUAL 1;",
            "CREATE STABLE `vst_智能水表_1` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `流量` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `水压` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`_ignore_path` VARCHAR(20), `地址` VARCHAR(50), `path1` VARCHAR(512)) SMA(`ts`,`流量`) VIRTUAL 1;",
            "CREATE VTABLE `vt_em-1` (`电流` FROM `asset01`.`em_1`.`current`, `电压` FROM `asset01`.`em_1`.`voltage`, `功率` FROM `asset01`.`em_1`.`power`, `相位` FROM `asset01`.`em_1`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 1, 2, 'em202502200010001', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-2` (`电流` FROM `asset01`.`em_2`.`current`, `电压` FROM `asset01`.`em_2`.`voltage`, `功率` FROM `asset01`.`em_2`.`power`, `相位` FROM `asset01`.`em_2`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 1, 2, 'em202502200010002', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-3` (`电流` FROM `asset01`.`em_3`.`current`, `电压` FROM `asset01`.`em_3`.`voltage`, `功率` FROM `asset01`.`em_3`.`power`, `相位` FROM `asset01`.`em_3`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 1, 2, 'em202502200010003', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-4` (`电流` FROM `asset01`.`em_4`.`current`, `电压` FROM `asset01`.`em_4`.`voltage`, `功率` FROM `asset01`.`em_4`.`power`, `相位` FROM `asset01`.`em_4`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 2, 2, 'em202502200010004', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-5` (`电流` FROM `asset01`.`em_5`.`current`, `电压` FROM `asset01`.`em_5`.`voltage`, `功率` FROM `asset01`.`em_5`.`power`, `相位` FROM `asset01`.`em_5`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 2, 2, 'em202502200010005', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-6` (`电流` FROM `asset01`.`em_6`.`current`, `电压` FROM `asset01`.`em_6`.`voltage`, `功率` FROM `asset01`.`em_6`.`power`, `相位` FROM `asset01`.`em_6`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道', 1, 2, 'em20250220001006', '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-7` (`电流` FROM `asset01`.`em_7`.`current`, `电压` FROM `asset01`.`em_7`.`voltage`, `功率` FROM `asset01`.`em_7`.`power`, `相位` FROM `asset01`.`em_7`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道', 1, 2, 'em20250220001007', '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-8` (`电流` FROM `asset01`.`em_8`.`current`, `电压` FROM `asset01`.`em_8`.`voltage`, `功率` FROM `asset01`.`em_8`.`power`, `相位` FROM `asset01`.`em_8`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道', 1, 2, 'em20250220001008', '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-9` (`电流` FROM `asset01`.`em_9`.`current`, `电压` FROM `asset01`.`em_9`.`voltage`, `功率` FROM `asset01`.`em_9`.`power`, `相位` FROM `asset01`.`em_9`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道', 1, 2, 'em20250220001009', '公共事业.北京.朝阳.国贸街道');",
        ]
        tdSql.executes(sqls)
        tdLog.info(f"create db tdasset {len(sqls)} vtable successfully.")

        # vehicle
        sqls = [
            "create database idmp vgroups 4 replica 3;",
            "use idmp;",
            "CREATE STABLE `vst_车辆_652220` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `经度` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `纬度` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `高程` SMALLINT ENCODE 'simple8b' COMPRESS 'zlib' LEVEL 'medium', `速度` SMALLINT ENCODE 'simple8b' COMPRESS 'zlib' LEVEL 'medium', `方向` SMALLINT ENCODE 'simple8b' COMPRESS 'zlib' LEVEL 'medium', `报警标志` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `里程` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`_ignore_path` VARCHAR(20), `车辆资产模型` VARCHAR(128), `车辆ID` VARCHAR(32), `车牌号` VARCHAR(17), `车牌颜色` TINYINT, `终端制造商` VARCHAR(11), `终端ID` VARCHAR(15), `path2` VARCHAR(512)) SMA(`ts`,`经度`) VIRTUAL 1",
            "CREATE VTABLE `vt_1` (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_001', '京Z1NW34', 2, 'zd', '2551765954', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_2` (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_002', '京Z1NW84', 2, 'zd', '1819625826', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_3` (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_003', '京Z2NW48', 2, 'zd', '5206002832', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_4` (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_004', '京Z7A0Q7', 2, 'zd', '1663944041', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_5` (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_005', '京Z7A2Q5', 2, 'zd', '7942624528', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_6` (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_006', '京ZB86G7', 2, 'zd', '1960758157', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_7` (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_007', '京ZCR392', 2, 'zd', '6560472044', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_8` (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_008', '京ZD43R1', 2, 'zd', '3491377379', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_9` (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_009', '京ZD62R2', 2, 'zd', '8265223624', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_10`  (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_0010`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_0010`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_0010`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_0010`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_0010`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_0010`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_0010`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_010', '京ZD66G4', 2, 'zd', '3689589229', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_501` (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_0011`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_0011`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_0011`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_0011`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_0011`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_0011`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_0011`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.呼和浩特车队', '150100_001', '蒙Z0C3N7', 2, 'zd', '3689589230', '车辆场景.XX物流公司.华北分公司.呼和浩特车队')",
        ]
        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls) - 2} vtable successfully.")

        

    # 
    # 2. create streams
    #
    def createStreams(self):

        # meters 
        sqls = [
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream1`       event_window( start with `电压` > 250 end with `电压` <= 250 ) TRUE_FOR(10m) FROM `tdasset`.`vt_em-1`  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream1` AS SELECT _twstart+0s AS output_timestamp, avg(`电压`) AS `平均电压`  FROM tdasset.`vt_em-1`  WHERE ts >= _twstart AND ts <=_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream1_sub1`  event_window( start with `电压` > 250 end with `电压` <= 250 ) TRUE_FOR(10m) FROM `tdasset`.`vt_em-1`  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream1_sub1` AS SELECT _twstart+0s AS output_timestamp, avg(`电压`) AS `平均电压`  FROM tdasset.`vt_em-1`  WHERE ts >= _twstart AND ts <=_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream1_sub2`  event_window( start with `电压` > 250 end with `电压` <= 250 ) TRUE_FOR(10m) FROM `tdasset`.`vt_em-1`  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_CLOSE)             INTO `tdasset`.`result_stream1_sub2` AS SELECT _twstart+0s AS output_timestamp, avg(`电压`) AS `平均电压`  FROM tdasset.`vt_em-1`  WHERE ts >= _twstart AND ts <=_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream2`  interval(1h)  sliding(5m) FROM `tdasset`.`vt_em-2`  notify('ws://idmp:6042/eventReceive') ON(window_open|window_close) INTO `tdasset`.`result_stream2` AS SELECT _twstart+0s AS output_timestamp, max(`电流`) AS `最大电流` FROM tdasset.`vt_em-2`  WHERE ts >=_twstart AND ts <=_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream3`  event_window( start with `电流` > 100 end with `电流` <= 100 ) TRUE_FOR(5m) FROM `tdasset`.`vt_em-3` NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream3` AS SELECT _twstart+0s AS output_timestamp, AVG(`电流`) AS `平均电流` FROM tdasset.`vt_em-3`  WHERE ts >= _twstart AND ts <=_twend",
            # stream4
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4`      INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4`                                                  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4`      AS SELECT _twstart+0s as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <_twend ",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub1` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER)                  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub1` AS SELECT _twstart+0s as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub2` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub2` AS SELECT _twstart + 10a as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub3` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub3` AS SELECT _twstart + 10s as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub4` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub4` AS SELECT _twstart + 10m as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub5` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub5` AS SELECT _twstart + 10h as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub6` INTERVAL(10m)  SLIDING(10m) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN)              INTO `tdasset`.`result_stream4_sub6` AS SELECT _twstart + 10d as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub7` INTERVAL(600s) SLIDING(1h)  FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub7` AS SELECT _twstart as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend AND ts >= 1752574200000",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub8` INTERVAL(1a)   SLIDING(1a)  FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub8` AS SELECT _twstart as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts <=_twend AND ts >= 1752574200000",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream4_sub9` INTERVAL(1d)   SLIDING(60s) FROM `tdasset`.`vt_em-4` STREAM_OPTIONS(IGNORE_DISORDER|LOW_LATENCY_CALC) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream4_sub9` AS SELECT _twstart as output_timestamp,COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压` , SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-4` WHERE ts >=_twstart AND ts < _twend AND ts >= 1752574200000",
            # stream5
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream5`      SESSION(ts, 10m) FROM `tdasset`.`vt_em-5` STREAM_OPTIONS(IGNORE_DISORDER)  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5`      AS SELECT _twstart+0s AS output_timestamp, COUNT(ts) AS cnt, LAST(`电流`) AS `最后电流` FROM tdasset.`vt_em-5` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream5_sub1` SESSION(ts, 10m) FROM `tdasset`.`vt_em-5`                                  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub1` AS SELECT _twstart+0s AS output_timestamp, COUNT(ts) AS cnt, LAST(`电流`) AS `最后电流` FROM tdasset.`vt_em-5` WHERE ts >= _twstart AND ts <=_twend",
            # stream6
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream6`      COUNT_WINDOW(5) FROM `tdasset`.`vt_em-6` STREAM_OPTIONS(IGNORE_DISORDER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6` AS SELECT _twstart+0s AS output_timestamp, COUNT(ts) AS cnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream6_sub1` COUNT_WINDOW(5) FROM `tdasset`.`vt_em-6`                                 NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream6_sub1` AS SELECT _twstart+0s AS output_timestamp, COUNT(ts) AS cnt, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压` FROM tdasset.`vt_em-6` WHERE ts >= _twstart AND ts <=_twend",
            # stream7
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream7` STATE_WINDOW(`电压`) TRUE_FOR(30s) FROM `tdasset`.`vt_em-7` STREAM_OPTIONS(IGNORE_DISORDER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream7` AS SELECT _twstart+0s AS output_timestamp, COUNT(ts) AS cnt, AVG(`电流`) AS `平均电流`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-7` WHERE ts >= _twstart AND ts <=_twend",
            # stream8
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream8` PERIOD(1s, 0s)                    FROM `tdasset`.`vt_em-8` STREAM_OPTIONS(IGNORE_DISORDER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8` AS SELECT now()+0s    AS output_timestamp, COUNT(ts) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM tdasset.`vt_em-8` WHERE ts >=_tprev_localtime and ts <=now()",
        ]
        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} streams successfully.")

        # vehicle
        sqls = [
            # stream_stb1
            "create stream if not exists `idmp`.`veh_stream_stb1`       interval(5m) sliding(5m) from `idmp`.`vst_车辆_652220` partition by `车辆资产模型`,`车辆ID`  stream_options(IGNORE_NODATA_TRIGGER)                    notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream_stb1`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`, sum(`里程`) as `里程和` from %%trows",
            "create stream if not exists `idmp`.`veh_stream_stb1_sub1`  interval(5m) sliding(5m) from `idmp`.`vst_车辆_652220` partition by `车辆资产模型`,`车辆ID`  stream_options(IGNORE_NODATA_TRIGGER|FILL_HISTORY_FIRST) notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream_stb1_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`, sum(`里程`) as `里程和` from %%trows",

            # stream1
            "create stream if not exists `idmp`.`veh_stream1`      event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_1` stream_options(ignore_disorder)       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream1`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from idmp.`vt_1` where ts >= _twstart and ts <_twend",
            "create stream if not exists `idmp`.`veh_stream1_sub1` event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_1` stream_options(delete_recalc)         notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream1_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from idmp.`vt_1` where ts >= _twstart and ts <_twend",
            # stream2
            "create stream if not exists `idmp`.`veh_stream2`      event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_2` stream_options(ignore_disorder)       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream2`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            "create stream if not exists `idmp`.`veh_stream2_sub1` event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_2`                                       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream2_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            # stream3
            "create stream if not exists `idmp`.`veh_stream3`      event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_3` stream_options(ignore_disorder)       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream3`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            "create stream if not exists `idmp`.`veh_stream3_sub1` event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_3`                                       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream3_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            # stream4
            "create stream if not exists `idmp`.`veh_stream4`      event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_4`                                       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream4`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            "create stream if not exists `idmp`.`veh_stream4_sub1` event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_4` stream_options(DELETE_RECALC)         notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream4_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            # stream5
            "create stream if not exists `idmp`.`veh_stream5`      interval(5m) sliding(5m) from `idmp`.`vt_5`                                       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream5`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            "create stream if not exists `idmp`.`veh_stream5_sub1` interval(5m) sliding(5m) from `idmp`.`vt_5` stream_options(IGNORE_NODATA_TRIGGER) notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream5_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            # stream6
            "create stream if not exists `idmp`.`veh_stream6`      interval(10m) sliding(5m) from `idmp`.`vt_6`                                       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream6`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            "create stream if not exists `idmp`.`veh_stream6_sub1` interval(10m) sliding(5m) from `idmp`.`vt_6` stream_options(IGNORE_NODATA_TRIGGER) notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream6_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            # stream7
            "create stream if not exists `idmp`.`veh_stream7`      interval(5m) sliding(10m) from `idmp`.`vt_7`                                       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream7`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            "create stream if not exists `idmp`.`veh_stream7_sub1` interval(5m) sliding(10m) from `idmp`.`vt_7` stream_options(IGNORE_NODATA_TRIGGER) notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream7_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            # stream8 watermark
            "create stream if not exists `idmp`.`veh_stream8`      interval(5m) sliding(5m) from `idmp`.`vt_8`  stream_options(WATERMARK(10m))        notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream8`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
        ]
        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} streams successfully.")


    # 
    # 3. wait stream ready
    #
    def checkStreamStatus(self):
        print("no wait stream ready ...")
        #tdStream.checkStreamStatus()
        #tdLog.info(f"check stream status successfully.")

    # 
    # 4. write trigger data
    #
    def startWriteJob(self):
        # meters
        nThreads = 1
        jsons = [
            "cases/41-StreamProcessing/20-UseCase/json/exist_idmp_meters.json",
            "cases/41-StreamProcessing/20-UseCase/json/exist_idmp_vehicle.json"
        ]
        for json in jsons:
            for i in range(nThreads):
                tdLog.info(f"start benchmark thread {i} with json: {json}")     
                thread = threading.Thread(target=self.benchmarkThread, args=(i, json))
                thread.start()
                self.threads.append(thread)


    # 
    # 5. write trigger data again
    #
    def startRestartJob(self):
        # restart dnode
        count = 10
        sleepMs = 2*10*1000  # 2 minutes
        tdLog.info(f"start restart thread with count: {count}, sleepMs: {sleepMs}")
        thread = threading.Thread(target=self.restartThread, args=(count, sleepMs))
        thread.start()
        self.threads.append(thread)        


    # 
    # 6. verify results
    #
    def verifyResults(self):
        tdLog.info("wait threads finished ...")
        for thread in self.threads:
            thread.join()

        tdLog.info("verify result ...")    


    # restart dnode
    def restartDnode(self):
        # restart
        tdLog.info("restart dnode ...")
        sc.dnodeRestartAll()

        # wait stream ready
        tdLog.info("wait stream ready after dnode restart ...")
        self.checkStreamStatus()

        tdLog.info("dnode restarted successfully.")


    #
    # thread for benchmark
    #
    def benchmarkThread(self, threadID, json):
        tdLog.info(f"benchmark thread {threadID} started with json: {json}")
        etool.benchmark(f"-f {json}")

    #
    # restart thread
    #
    def restartThread(self, count, sleepMs):
        # loop for restart
        for i in range(count):
            tdLog.info(f"restart {i} started, sleep {sleepMs} ms ...")
            time.sleep(sleepMs / 1000)
            self.restartDnode()
        # end
        tdLog.info(f"restart thread finished after {count} times of restart.")