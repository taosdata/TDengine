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
            "CREATE VTABLE `vt_1`   (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_001', '京Z1NW34', 2, 'zd', '2551765954', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_2`   (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_002`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_002', '京Z1NW84', 2, 'zd', '1819625826', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_3`   (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_003`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_003', '京Z2NW48', 2, 'zd', '5206002832', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_4`   (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_004`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_004', '京Z7A0Q7', 2, 'zd', '1663944041', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_5`   (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_005`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_005', '京Z7A2Q5', 2, 'zd', '7942624528', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_6`   (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_006`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_006', '京ZB86G7', 2, 'zd', '1960758157', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_7`   (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_007`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_007', '京ZCR392', 2, 'zd', '6560472044', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_8`   (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_008`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_008', '京ZD43R1', 2, 'zd', '3491377379', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_9`   (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_009`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_009', '京ZD62R2', 2, 'zd', '8265223624', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_10`  (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_010`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_010', '京ZD6010', 2, 'zd', '8265200010', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_11`  (`经度` FROM `idmp_sample_vehicle`.`vehicle_120100_001`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_120100_001`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_120100_001`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_120100_001`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_120100_001`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_120100_001`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_120100_001`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '120100_001', '京ZD0011', 2, 'zd', '8265200011', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_12`  (`经度` FROM `idmp_sample_vehicle`.`vehicle_120100_002`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_120100_002`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_120100_002`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_120100_002`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_120100_002`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_120100_002`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_120100_002`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '120100_002', '京ZD0012', 2, 'zd', '8265200012', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_13`  (`经度` FROM `idmp_sample_vehicle`.`vehicle_120100_003`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_120100_003`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_120100_003`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_120100_003`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_120100_003`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_120100_003`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_120100_003`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '120100_003', '京ZD0013', 2, 'zd', '8265200013', '车辆场景.XX物流公司.华北分公司.北京车队')",
            "CREATE VTABLE `vt_501` (`经度` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_150100_001`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.呼和浩特车队', '110100_011', '蒙Z0C3N7', 2, 'zd', '3689589230', '车辆场景.XX物流公司.华北分公司.呼和浩特车队')",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls) - 2} vtable successfully.")
        

    # 
    # 2. create streams
    #
    def createStreams(self):

        sqls = [
             # stream10 expired_time 1h
            "create stream if not exists `idmp`.`veh_stream10`      interval(10m) sliding(10m) from `idmp`.`vt_10`  stream_options(EXPIRED_TIME(1h)|IGNORE_DISORDER) notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream10`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`,sum(`里程`) as `里程和` from %%trows",
            "create stream if not exists `idmp`.`veh_stream10_sub1` interval(10m) sliding(10m) from `idmp`.`vt_10`  stream_options(EXPIRED_TIME(1h))                 notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream10_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`,sum(`里程`) as `里程和` from %%trows",
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
        print("write data ...")
        # stream10
        self.trigger_stream10()

    # 
    # 5. verify results
    #
    def verifyResults(self):
        print("verify results ...")
        self.verify_stream10()


    # 
    # 6. write trigger data again
    #
    def writeTriggerDataAgain(self):
        pass


    # 
    # 7. verify results again
    #
    def verifyResultsAgain(self):
        pass

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


    def printSql(self, label, sql):
        print(label + sql)
        rows = tdSql.getResult(sql)
        i = 0
        for row in rows:
            print(f"i={i} {row}")
            i += 1


    # ---------------------   stream trigger    ----------------------

      #
    #  stream10 trigger
    #
    def trigger_stream10(self):
        
        table = f"{self.db}.`vehicle_110100_010`"
        cols  = "ts,speed,mileage"

        # init
        vals1 = "120,100"
        vals2 = "150,200"
        hour  = 1 * 60 * self.step

        # win1 dirorder write 10
        ts = self.start
        count = 10
        tdSql.insertFixedVal(table, ts, self.step * -1, count, cols, vals1)

        # move 10 step
        ts += 10 * self.step
        hour_ts = ts + hour

        # win4 write end line 
        count = 1
        tdSql.insertFixedVal(table, hour_ts, self.step, count, cols, vals2)

        # win2 disorder write expired 10
        count = 10
        tdSql.insertFixedVal(table, ts - self.step, self.step * -1, count, cols, vals1)

        # win3 write no expired 10
        count = 10
        ts = tdSql.insertFixedVal(table, ts, self.step, count, cols, vals1)

    #
    # ---------------------   verify    ----------------------
    #

   

 #
    # verify stream10
    #
    def verify_stream10(self):
        # check data
        time.sleep(2)
        result_sql = f"select * from {self.vdb}.`result_stream10` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.checkRows(7)
            # row1
            and tdSql.compareData(0, 0, self.start) # ts
            and tdSql.compareData(0, 1, 1)          # cnt
            and tdSql.compareData(0, 2, 120)        # avg
            and tdSql.compareData(0, 3, 100)        # sum
            # row2
            and tdSql.compareData(1, 1, 0)
            and tdSql.compareData(1, 0, self.start + 1 * self.step)
            and tdSql.compareData(2, 0, self.start + 2 * self.step)
            and tdSql.compareData(3, 0, self.start + 3 * self.step)
            and tdSql.compareData(4, 0, self.start + 4 * self.step)
            and tdSql.compareData(5, 0, self.start + 5 * self.step)
            and tdSql.compareData(6, 0, self.start + 6 * self.step)
        )

        # sub
        self.verify_stream10_sub1()

        tdLog.info(f"verify stream10 ................................ successfully.")


    # verify stream10_sub1
    def verify_stream10_sub1(self):
        # check data
        result_sql = f"select * from {self.vdb}.`result_stream10_sub1` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.checkRows(8)
            # row1
            and tdSql.compareData(0, 0, self.start - 1 * self.step) # ts
            and tdSql.compareData(0, 1, 9)          # cnt
            and tdSql.compareData(0, 2, 120)        # avg
            and tdSql.compareData(0, 3, 900)        # sum
            # row2 
            and tdSql.compareData(1, 0, self.start) # ts
            and tdSql.compareData(1, 1, 1)          # cnt
            # row3
            and tdSql.compareData(2, 0, self.start + 1 * self.step) # ts
            # ***** bug13 *****
            and tdSql.compareData(2, 1, 0)          # cnt
            # row4
            and tdSql.compareData(3, 0, self.start + 2 * self.step) # ts
            and tdSql.compareData(3, 1, 10)         # cnt
            # row5 ~ 8
            and tdSql.compareData(4, 0, self.start + 3 * self.step)
            and tdSql.compareData(5, 0, self.start + 4 * self.step)
            and tdSql.compareData(6, 0, self.start + 5 * self.step)
            and tdSql.compareData(7, 0, self.start + 6 * self.step)
        )

        tdLog.info(f"verify stream10 sub1 ........................... successfully.")
