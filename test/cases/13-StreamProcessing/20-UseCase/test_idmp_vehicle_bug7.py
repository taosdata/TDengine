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
            # stream3
            "create stream if not exists `idmp`.`ana_stream3`      event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_3` stream_options(ignore_disorder)       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream3`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
            "create stream if not exists `idmp`.`ana_stream3_sub1` event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_3`                                       notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream3_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from %%trows",
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
        # stream3
        self.trigger_stream3()  


    # 
    # 5. verify results
    #
    def verifyResults(self):
        self.verify_stream3()
        self.verify_stream3_sub1()


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
    # ---------------------   verify    ----------------------
    #

 
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
            func = lambda: tdSql.getRows() == 2
            # row1
            and tdSql.compareData(0, 0, 1752900600000) # ts
            and tdSql.compareData(0, 1, 5 + 1)          # cnt
            # row2
            and tdSql.compareData(1, 0, 1752901980000) # ts
            and tdSql.compareData(1, 1, 5 + 1)          # cnt
        )

        tdLog.info(f"verify stream3 sub1 again ...................... successfully.")