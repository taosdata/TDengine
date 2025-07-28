import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, etool
from datetime import datetime
from datetime import date


class Test_IDMP_Vehicle:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_usecase_em(self):
        """Nevados

        Refer: https://taosdata.feishu.cn/wiki/Zkb2wNkHDihARVkGHYEcbNhmnxb

        Catalog:
            - Streams:UseCases

        Since: v3.3.7.0

        Labels: common,ci

        Jira: https://jira.taosdata.com:18080/browse/TD-36781

        History:
            - 2025-7-18 Alex Duan Created

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
        self.db    = "idmp_sample_vehicle"
        self.vdb   = "idmp"
        self.stb   = "vehicles"
        self.step  = 1 * 60 * 1000 # 1 minute
        self.start = 1752900000000
        self.start_current = 10
        self.start_voltage = 260        


        # import data
        etool.taosdump(f"-i cases/13-StreamProcessing/20-UseCase/vehicle_data/")

        tdLog.info(f"import data to db={self.db}. successfully.")


    # 
    # 1. create vtables
    #
    def createVtables(self):
        sqls = [
            f"create database {self.vdb};",
            f"use {self.vdb};",
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
            "CREATE VTABLE `vt_10` (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_010', '京ZD66G4', 2, 'zd', '3689589229', '车辆场景.XX物流公司.华北分公司.北京车队')",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls) - 2} vtable successfully.")
        

    # 
    # 2. create streams
    #
    def createStreams(self):

        sqls = [
            # stream1            
            "create stream if not exists `idmp`.`ana_stream1`      event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_1` stream_options(ignore_disorder)       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream1`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from idmp.`vt_1` where ts >= _twstart and ts <_twend",
            "create stream if not exists `idmp`.`ana_stream1_sub1` event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_1` stream_options(delete_recalc)         notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream1_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from idmp.`vt_1` where ts >= _twstart and ts <_twend",
            # stream2
            "create stream if not exists `idmp`.`ana_stream2`      event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_2` stream_options(ignore_disorder)       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream2`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            "create stream if not exists `idmp`.`ana_stream2_sub1` event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_2`                                       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream2_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            # stream3
            "create stream if not exists `idmp`.`ana_stream3`      event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_3` stream_options(ignore_disorder)       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream3`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            "create stream if not exists `idmp`.`ana_stream3_sub1` event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_3`                                       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream3_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            # stream4
            "create stream if not exists `idmp`.`ana_stream4`      event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_4`                                       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream4`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            "create stream if not exists `idmp`.`ana_stream4_sub1` event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_4` stream_options(DELETE_RECALC)         notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream4_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            # stream5
            "create stream if not exists `idmp`.`ana_stream5`      interval(5m) sliding(5m) from `idmp`.`vt_5`                                       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream5`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            "create stream if not exists `idmp`.`ana_stream5_sub1` interval(5m) sliding(5m) from `idmp`.`vt_5` stream_options(IGNORE_NODATA_TRIGGER) notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream5_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            # stream6
            "create stream if not exists `idmp`.`ana_stream6`      interval(10m) sliding(5m) from `idmp`.`vt_6`                                       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream6`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            "create stream if not exists `idmp`.`ana_stream6_sub1` interval(10m) sliding(5m) from `idmp`.`vt_6` stream_options(IGNORE_NODATA_TRIGGER) notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream6_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            # stream7
            "create stream if not exists `idmp`.`ana_stream7`      interval(5m) sliding(10m) from `idmp`.`vt_7`                                       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream7`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            "create stream if not exists `idmp`.`ana_stream7_sub1` interval(5m) sliding(10m) from `idmp`.`vt_7` stream_options(IGNORE_NODATA_TRIGGER) notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream7_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            # stream8
            "create stream if not exists `idmp`.`ana_stream8`      interval(5m) sliding(5m) from `idmp`.`vst_车辆_652220`  partition by `车辆资产模型`,`车辆ID`  stream_options(IGNORE_NODATA_TRIGGER)      notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream8`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`, sum(`里程`) as `里程和` from %%trows",
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
        # stream1
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



    # 
    # 5. verify results
    #
    def verifyResults(self):
        self.verify_stream1()
        self.verify_stream2()
        self.verify_stream3()
        self.verify_stream3_sub1()

        self.verify_stream4()
        self.verify_stream5()
        self.verify_stream6()
        self.verify_stream7()
        self.verify_stream8()


    # 
    # 6. write trigger data again
    #
    def writeTriggerDataAgain(self):
        # stream3
        self.trigger_stream3_again()


    # 
    # 7. verify results again
    #
    def verifyResultsAgain(self):
        pass
        # stream3
        self.verify_stream3_again()
        self.verify_stream3_sub1_again()

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
        ts    = self.start
        table = f"{self.db}.`vehicle_110100_001`"
        cols  = "ts,speed"

        # win1 1~5
        vals  = "120"
        count = 5
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # null 
        count = 2
        vals  = "null"
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # end
        vals  = "60"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)


        # win3 50 ~ 51 end-windows
        ts += 50 * self.step
        vals  = "10"
        count = 2
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)


        # delete win1 2 rows
        tdSql.deleteRows(table, f"ts >= {self.start + 1 * self.step} and ts <= {self.start + 2 * self.step}")

        # disorder
        ts    = self.start + (5 + 2 + 1) * self.step
        vals  = "130"
        count = 3
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)
        # null 
        count = 10
        vals  = "null"
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # null changed 65
        ts    = self.start + (5 + 2 + 1 + 3) * self.step
        count = 1
        vals  = "65"
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)        
        # null changed 140
        count = 5
        vals  = "140"
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # 130 change to null
        ts    = self.start
        vals  = "null"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # trigger disorder event
        ts   += 50 * self.step
        vals  = "9"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)


    #
    #  stream2 trigger 
    #
    def trigger_stream2(self):
        ts    = self.start
        table = f"{self.db}.`vehicle_110100_002`"
        cols  = "ts,speed"

        # win1 1~5
        vals  = "120"
        count = 5
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)
        vals  = "60"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)


        # win2 10~5
        ts += 10 * self.step
        vals  = "130"
        count = 5
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)


        # win3 50 ~ 51 end-windows
        ts += 50 * self.step
        vals  = "65"
        count = 2
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)


        # delete win1 3 rows
        tdSql.deleteRows(table, f"ts >= {self.start } and ts <= {self.start + 2 * self.step}") 

    #
    #  stream3 trigger 
    #
    def trigger_stream3(self):
        table = f"{self.db}.`vehicle_110100_003`"
        cols  = "ts,speed"

        # write order data

        # win1 order 1 ~   no -> no
        ts    = self.start
        vals  = "120"
        count = 3
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)
        ts   += 1 * self.step
        vals  = "60"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)


        # win2 order 10 ~   no -> trigger 
        ts = self.start + 10 * self.step
        ts   += 1 * self.step
        vals  = "130"
        count = 4
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)
        vals  = "65"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # win3 order 20 ~  trigger -> no
        ts = self.start + 20 * self.step
        vals  = "140"
        count = 6
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)
        vals  = "70"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # win4 order 30 ~  trigger -> trigger
        ts = self.start + 30 * self.step
        vals  = "150"
        count = 8
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)
        vals  = "75"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)


    #
    #  stream3 trigger 
    #
    def trigger_stream3_again(self):

        table = f"{self.db}.`vehicle_110100_003`"
        cols  = "ts,speed"

        # write update data

        # win1
        ts    = self.start + 3 * self.step
        vals  = "121"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # win2
        ts    = self.start + 10 * self.step
        vals  = "131"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # win3
        ts    = self.start + 20 * self.step
        vals  = "71"
        count = 2
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # win4
        ts    = self.start + 30 * self.step
        vals  = "76"
        count = 3
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)


    #
    #  stream4 trigger 
    #
    def trigger_stream4(self):
        ts    = self.start
        table = f"{self.db}.`vehicle_110100_004`"
        cols  = "ts,speed"

        # win1 1~6
        vals  = "120"
        count = 5
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)
        vals  = "60"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)


        # win2 7~13
        vals  = "130"
        count = 5
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)
        vals  = "65"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)


        # 20
        ts    = self.start + 20 * self.step
        vals  = "140"
        count = 10
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)        
        vals  = "70"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)


        # delete 1~3
        tdSql.deleteRows(table, f"ts >= {self.start } and ts <= {self.start + 3 * self.step}") 
        
        # delete 20 ~ 23
        tdSql.deleteRows(table, f"ts >= {self.start + 20 * self.step } and ts <= {self.start + 23 * self.step}") 


    #
    #  stream4 trigger again
    #
    def trigger_stream4_again(self):
        pass


    #
    #  stream5 trigger 
    #
    def trigger_stream5(self):
        table = f"{self.db}.`vehicle_110100_005`"
        cols  = "ts,speed"

        # order write

        # data1
        ts    = self.start
        vals  = "120"
        count = 5
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # blank 20

        # data2
        ts   += 20 * self.step
        vals  = "130"
        count = 5
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # close prev windows
        endTs = self.start + 60 * self.step
        vals  = "10"
        count = 1
        endTs = tdSql.insertFixedVal(table, endTs, self.step, count, cols, vals)

        # disorder

        # continue write disorder
        ts   += 10 * self.step
        vals  = "140"
        count = 5
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # blank 20

        # data2
        ts   += 20 * self.step
        vals  = "150"
        count = 5
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)      


    #
    #  stream6 trigger 
    #
    def trigger_stream6(self):
        table = f"{self.db}.`vehicle_110100_006`"
        cols  = "ts,speed"

        # order write

        # data1
        ts    = self.start
        vals  = "100"
        count = 10
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # blank 20

        # data2
        ts   += 20 * self.step
        vals  = "110"
        count = 10
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # close prev windows
        endTs = self.start + 100 * self.step
        vals  = "10"
        count = 1
        endTs = tdSql.insertFixedVal(table, endTs, self.step, count, cols, vals)        

        # data2
        vals  = "120"
        count = 10
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        endTs = self.start + 100 * self.step
        vals  = "11"
        count = 1
        endTs = tdSql.insertFixedVal(table, endTs, self.step, count, cols, vals)        


    #
    #  again stream6 trigger
    #
    def trigger_stream6_again(self):
        pass

    #
    #  stream7 trigger
    #
    def trigger_stream7(self):
        table = f"{self.db}.`vehicle_110100_007`"
        cols  = "ts,speed"

        # order write

        # data1
        ts    = self.start
        vals  = "100"
        count = 10
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # blank 20

        # data2
        ts   += 20 * self.step
        vals  = "110"
        count = 10
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # close prev windows
        endTs = self.start + 100 * self.step
        vals  = "10"
        count = 1
        endTs = tdSql.insertFixedVal(table, endTs, self.step, count, cols, vals)        

        # data2
        vals  = "120"
        count = 10
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        endTs = self.start + 100 * self.step
        vals  = "11"
        count = 1
        endTs = tdSql.insertFixedVal(table, endTs, self.step, count, cols, vals)        
        

    #
    #  stream8 trigger
    #
    def trigger_stream8(self):
        table = f"{self.db}.`vehicle_110100_008`"
        cols  = "ts,speed,mileage"

        # data1
        ts    = self.start
        vals  = "150,300"
        count = 11
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)


    #
    # ---------------------   verify    ----------------------
    #

    #
    # verify stream1
    #
    def verify_stream1(self):
        # check
        result_sql = f"select * from {self.vdb}.`result_stream1` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, self.start) # ts
            and tdSql.compareData(0, 1, 5)          # cnt
            and tdSql.compareData(0, 2, 120)        # avg(speed)
        )

        # sub
        self.verify_stream1_sub1()
        tdLog.info("verify stream1 .................................. successfully.")

    # stream1 sub1
    def verify_stream1_sub1(self):
        # check
        result_sql = f"select * from {self.vdb}.`result_stream1_sub1` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.checkRows(1, show=True)
            and tdSql.compareData(0, 0, self.start + (5 + 2 + 1 + 3 + 1) * self.step) # ts
            and tdSql.compareData(0, 1, 9)          # cnt
            and tdSql.compareData(0, 2, 140)        # avg(speed)
        )

        tdLog.info("verify stream1 sub1 ............................. successfully.")


    #
    # verify stream2
    #
    def verify_stream2(self):
        # check
        result_sql = f"select * from {self.vdb}.`result_stream2` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, self.start) # ts
            and tdSql.compareData(0, 1, 6)          # cnt
        )

        # sub
        # ***** bug3 *****
        # self.verify_stream2_sub1()

        tdLog.info("verify stream2 .................................. successfully.")


    # verify stream2 sub1
    def verify_stream2_sub1(self):
        # check
        result_sql = f"select * from {self.vdb}.`result_stream2_sub1` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, self.start + 10 * self.step) # ts
            and tdSql.compareData(0, 1, 6)                     # cnt
        )        
        tdLog.info("verify stream2 sub1 ............................. successfully.")


    #
    # verify stream3
    #
    def verify_stream3(self):
        # check
        result_sql = f"select * from {self.vdb}.`result_stream3` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 2
            # row1
            and tdSql.compareData(0, 0, self.start + 20 * self.step) # ts
            and tdSql.compareData(0, 1, 6 + 1)          # cnt
            # row2
            and tdSql.compareData(1, 0, self.start + 30 * self.step) # ts
            and tdSql.compareData(1, 1, 8 + 1)          # cnt
        )

        tdLog.info("verify stream3 .................................. successfully.")
        
    def verify_stream3_sub1(self, tables=None):
        # check
        result_sql = f"select * from {self.vdb}.`result_stream3_sub1` "
        # same with stream3
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 2
            # row1
            and tdSql.compareData(0, 0, self.start + 20 * self.step) # ts
            and tdSql.compareData(0, 1, 6 + 1)          # cnt
            # row2
            and tdSql.compareData(1, 0, self.start + 30 * self.step) # ts
            and tdSql.compareData(1, 1, 8 + 1)          # cnt
        )

        tdLog.info(f"verify stream3 sub1 ............................. successfully.")


    #
    # verify stream3 again
    #
    def verify_stream3_again(self):
        # check
        self.verify_stream3()
        tdLog.info("verify stream3 again ............................ successfully.")


    def verify_stream3_sub1_again(self, tables=None):
        # check
        result_sql = f"select * from {self.vdb}.`result_stream3_sub1` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 4
            #
            # old reserved
            #
            # row2
            and tdSql.compareData(1, 0, self.start + 20 * self.step) # ts
            and tdSql.compareData(1, 1, 6 + 1)                       # cnt
            # row3
            and tdSql.compareData(2, 0, self.start + 30 * self.step) # ts
            and tdSql.compareData(2, 1, 8 + 1)                       # cnt

            #
            # new generate append
            #

            # row1
            and tdSql.compareData(0, 0, 1752900600000) # ts
            and tdSql.compareData(0, 1, 5 + 1)         # cnt
            # row4
            and tdSql.compareData(3, 0, 1752901980000) # ts
            and tdSql.compareData(3, 1, 5 + 1)         # cnt
        )

        tdLog.info(f"verify stream3 sub1 again ...................... successfully.")


    #
    # verify stream4
    #
    def verify_stream4(self, tables=None):
        # check
        result_sql = f"select * from {self.vdb}.`result_stream4` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 3
            # row1
            and tdSql.compareData(0, 0, self.start) # ts
            and tdSql.compareData(0, 1, 6)          # cnt
            # row2
            and tdSql.compareData(1, 0, self.start + 6 * self.step) # ts
            and tdSql.compareData(1, 1, 6)          # cnt
            # row3
            and tdSql.compareData(2, 0, self.start + 20 * self.step) # ts
            and tdSql.compareData(2, 1, 11)          # cnt
        )

        # sub
        self.verify_stream4_sub1()

        tdLog.info(f"verify stream4 ................................. successfully.")

    def verify_stream4_sub1(self, tables=None):
        # check
        result_sql = f"select * from {self.vdb}.`result_stream4_sub1` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 2
            # row1
            and tdSql.compareData(0, 0, self.start + 6 * self.step) # ts
            and tdSql.compareData(0, 1, 6)                          # cnt
            # row2
            and tdSql.compareData(1, 0, self.start + 24 * self.step) # ts
            and tdSql.compareData(1, 1, 11 - 4)                      # cnt
        )

        tdLog.info(f"verify stream4 sub1 ............................. successfully.")



    def getSlidingWindow(self, start, step, cnt):
        wins = []
        x = int(start/step)
        i = 0

        while len(wins) < cnt:
            win = (x + i) * self.step
            if win >= start:
                wins.append(win)
            # move next    
            i += 1        

        return wins


    #
    # verify stream4 again
    #
    def verify_stream4_again(self):
        tdLog.info("verify stream4 again ............................ successfully.")

    
    #
    # verify stream5
    #

    def verify_stream5(self):
        # check data
        result_sql = f"select * from {self.vdb}.`result_stream5` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 13
            # row1
            and tdSql.compareData(0, 0, self.start) # ts
            and tdSql.compareData(0, 1, 5)          # cnt
            and tdSql.compareData(0, 2, 120)          # avg
            # row6
            and tdSql.compareData(5, 0, 1752901500000) # ts
            and tdSql.compareData(5, 1, 5)             # cnt
            and tdSql.compareData(5, 2, 130)           # avg
            # row9
            and tdSql.compareData(8, 0, 1752902400000) # ts
            and tdSql.compareData(8, 1, 5)             # cnt
            and tdSql.compareData(8, 2, 140)           # avg
        )

        # ts diff is 30000
        tdSql.checkResultsByFunc (
            sql = f"select * from (select diff(_c0) as dif from {self.vdb}.`result_stream5`) where dif = 300000", 
            func = lambda: tdSql.getRows() == 12
        )
        # cnt is zero
        tdSql.checkResultsByFunc (
            sql = f"select * from {self.vdb}.`result_stream5` where cnt = 0", 
            func = lambda: tdSql.getRows() == 13 - 4
        )

        # sub1
        tdSql.checkResultsBySql (
            sql     = f"select * from {self.vdb}.`result_stream5_sub1` ",
            exp_sql = f"select * from {self.vdb}.`result_stream5`      where cnt > 0",
        )
        
        tdLog.info(f"verify stream5 ................................. successfully.")

    #
    # verify stream6
    #

    def verify_stream6(self):
        # check data
        sql = f"select * from {self.vdb}.`result_stream6_sub1` "
        data = [
            [1752899700000,   5,100],
            [1752900000000,  10,100],
            [1752900300000,   5,100],
            [1752901500000,   5,110],
            [1752901800000,  10,110],
            [1752902100000,  10,115],
            [1752902400000,  10,120],
            [1752902700000,   5,120],
        ]

        # mem
        tdSql.checkDataMem(sql, data)

        # not no data
        tdSql.checkResultsBySql (
            sql     = sql,
            exp_sql = f"select * from {self.vdb}.`result_stream6` where cnt > 0"
        )

        tdLog.info(f"verify stream6 ................................. successfully.")

    def verify_stream6_again(self):
        tdLog.info(f"verify stream6 ................................. successfully.")
        

    #
    # verify stream7
    #
    def verify_stream7(self):
        # check data
        sql = f"select * from {self.vdb}.`result_stream7_sub1` "
        data = [
            [1752900000000,   5,100],
            [1752901800000,   5,110],
            [1752902400000,   5,120]
        ]

        # mem
        tdSql.checkDataMem(sql, data)

        # not no data
        tdSql.checkResultsBySql (
            sql     = sql,
            exp_sql = f"select * from {self.vdb}.`result_stream7` where cnt > 0"
        )        
        tdLog.info(f"verify stream7 ................................. successfully.")


    #
    # verify stream8
    #
    def verify_stream8(self):
        # check data
        result_sql = f"select * from {self.vdb}.`result_stream8` where `车辆ID`= '110100_008'"
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 2
            # row1
            and tdSql.compareData(0, 0, self.start) # ts
            and tdSql.compareData(0, 1, 5)          # cnt
            and tdSql.compareData(0, 2, 150)        # avg(speed)
            and tdSql.compareData(0, 3, 1500)       # sum
            # row2
            and tdSql.compareData(1, 0, self.start + 5 * self.step) # ts
            and tdSql.compareData(1, 1, 5)          # cnt
            and tdSql.compareData(1, 2, 150)        # avg(speed)
            and tdSql.compareData(1, 3, 1500)       # sum
        )

        tdLog.info(f"verify stream8 ................................. successfully.")
