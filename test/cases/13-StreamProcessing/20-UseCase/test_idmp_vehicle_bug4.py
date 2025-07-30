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
            "CREATE VTABLE `vt_京Z1NW34_624364` (`经度` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`longitude`, `纬度` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`latitude`, `高程` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`elevation`, `速度` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`speed`, `方向` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`direction`, `报警标志` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`alarm`, `里程` FROM `idmp_sample_vehicle`.`vehicle_110100_001`.`mileage`) USING `vst_车辆_652220` (`_ignore_path`, `车辆资产模型`, `车辆ID`, `车牌号`, `车牌颜色`, `终端制造商`, `终端ID`, `path2`) TAGS (NULL, 'XX物流公司.华北分公司.北京车队', '110100_001', '京Z1NW34', 2, 'zd', '2551765954', '车辆场景.XX物流公司.华北分公司.北京车队')",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls) - 2} vtable successfully.")
        

    # 
    # 2. create streams
    #
    def createStreams(self):

        sqls = [
          "create stream if not exists `idmp`.`ana_stream1`      event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_京Z1NW34_624364` stream_options(ignore_disorder)  notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream1`      as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from idmp.`vt_京Z1NW34_624364` where ts >= _twstart and ts <_twend",
          "create stream if not exists `idmp`.`ana_stream1_sub1` event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_京Z1NW34_624364`                                  notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream1_sub1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from idmp.`vt_京Z1NW34_624364` where ts >= _twstart and ts <_twend",
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



    # 
    # 5. verify results
    #
    def verifyResults(self):
        self.verify_stream1()


    # ---------------------   stream trigger    ----------------------

    #
    #  stream1 trigger 
    #
    def trigger_stream1(self):
        ts    = self.start
        table = f"{self.db}.`vehicle_110100_001`"
        step  = 1 * 60 * 1000 # 1 minute
        cols  = "ts,speed"

        # win1 1~5
        vals  = "120"
        count = 5
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # null 
        count = 2
        vals  = "null"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # end
        vals  = "60"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)


        # win3 50 ~ 51 end-windows
        ts += 50 * step
        vals  = "10"
        count = 2
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)


        ''' ***** bug1 *****
        # disorder win2 10~15
        win2  = self.start + 10 * step
        vals  = "60"
        count = 2
        ts    = tdSql.insertFixedVal(table, win2, step, count, cols, vals)
        '''

        '''
        win2  = self.start + 10 * step
        vals  = "60"
        count = 1
        ts    = tdSql.insertFixedVal(table, win2, step, count, cols, vals)


        # disorder win2 20~26
        win2  = self.start + 20 * step
        vals  = "150"
        count = 6
        ts    = tdSql.insertFixedVal(table, win2, step, count, cols, vals)        
        '''

        # delete win1 2 rows
        tdSql.deleteRows(table, f"ts >= {self.start + 1 * step} and ts <= {self.start + 2 * step}")

        # disorder
        ts    = self.start + (5 + 2 + 1) * step
        vals  = "130"
        count = 3
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)
        # null 
        count = 10
        vals  = "null"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # null changed 65
        ts    = self.start + (5 + 2 + 1 + 3) * step
        count = 1
        vals  = "65"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)        
        # null changed 140
        count = 5
        vals  = "140"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # 130 change to null
        ts    = self.start
        vals  = "null"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # trigger disorder event
        ts   += 50 * step
        vals  = "9"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)


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
            func = lambda: tdSql.getRows() == 1
            and tdSql.compareData(1, 0, self.start + (5 + 2 + 1) * self.step) # ts
            and tdSql.compareData(1, 1, 9)          # cnt
            and tdSql.compareData(1, 2, 140)        # avg(speed)
        )

        tdLog.info("verify stream1 sub1 ............................. successfully.")
