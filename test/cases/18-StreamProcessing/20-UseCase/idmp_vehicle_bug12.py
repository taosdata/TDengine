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

        Labels: common,ci,skip

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
        etool.taosdump(f"-i cases/41-StreamProcessing/20-UseCase/vehicle_data/")

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
            "CREATE VTABLE `vt_501` (`经度` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.呼和浩特车队', '110100_011', '蒙Z0C3N7', 2, 'zd', '3689589230', '车辆场景.XX物流公司.华北分公司.呼和浩特车队')",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls) - 2} vtable successfully.")
        

    # 
    # 2. create streams
    #
    def createStreams(self):

        sqls = [
            # stream9 watermark 5m
            "create stream if not exists `idmp`.`veh_stream9`      interval(10m) sliding(10m) from `idmp`.`vt_9`  stream_options(WATERMARK(5m)|IGNORE_DISORDER) notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream9`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`,sum(`里程`) as `里程和` from %%trows",
            "create stream if not exists `idmp`.`veh_stream9_sub1` interval(10m) sliding(10m) from `idmp`.`vt_9`  stream_options(WATERMARK(5m))                 notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream9_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`,sum(`里程`) as `里程和` from %%trows",
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
        # stream9
        self.trigger_stream9()

    # 
    # 5. verify results
    #
    def verifyResults(self):
        self.verify_stream9()






    # ---------------------   stream trigger    ----------------------

    #
    #  stream9 trigger
    #
    def trigger_stream9(self):
        
        table = f"{self.db}.`vehicle_110100_009`"
        cols  = "ts,speed,mileage"

        # init
        ts    = self.start
        vals  = "120,100"

        # win1 1
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)
        # blank delay 2 skip
        ts_blank = ts
        ts += 2 * self.step
        # go to win1 end
        ts += 6 * self.step
        # win1 10
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)

        # win1 2~3 disorder
        count = 8
        vals  = "120,100"
        ts_blank = tdSql.insertFixedVal(table, ts_blank, self.step, count, cols, vals)

        # expect win1 not close
        sql = "select * from idmp.result_stream9" # expect table not exist
        tdSql.waitError(sql)

        # win2 two trigger win1 close
        vals  = "150,200"
        count = 6
        ts    = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals)
    #
    # ---------------------   verify    ----------------------
    #

   

    #
    # verify stream9
    #
    def verify_stream9(self):
        # check data
        result_sql = f"select * from {self.vdb}.`result_stream9` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.checkRows(1)
            # row1
            and tdSql.compareData(0, 0, self.start) # ts
            and tdSql.compareData(0, 1, 7)          # cnt
            and tdSql.compareData(0, 2, 120)        # avg
            and tdSql.compareData(0, 3, 700)        # sum
        )

        tdLog.info(f"verify stream9 ................................. successfully.")

