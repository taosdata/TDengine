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
            "create stream if not exists `idmp`.`ana_stream1` event_window( start with `速度` > 100 end with `速度` <= 100 ) true_for(5m) from `idmp`.`vt_京Z1NW34_624364` stream_options(ignore_disorder)  notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `idmp`.`result_stream1` as select _twstart+0s as output_timestamp, count(*) as cnt, avg(`速度`) as `平均速度`  from idmp.`vt_京Z1NW34_624364` where ts >= _twstart and ts <_twend",
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

        '''
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
        '''


    # 
    # 5. verify results
    #
    def verifyResults(self):
        self.verify_stream1()
        '''
        self.verify_stream2()
        self.verify_stream3()
        self.verify_stream4()
        self.verify_stream5()
        self.verify_stream6()
        self.verify_stream7()
        # ***** bug9 *****
        #self.verify_stream8()
        '''


    # 
    # 6. write trigger data again
    #
    def writeTriggerDataAgain(self):
        pass
        '''
        # stream4
        self.trigger_stream4_again()
        # stream6
        self.trigger_stream6_again()
        '''


    # 
    # 7. verify results again
    #
    def verifyResultsAgain(self):
        pass
        '''
        # stream4
        self.verify_stream4_again()
        # stream6
        self.verify_stream6_again()
        '''

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
        ts = self.start
        table = f"{self.db}.`vehicle_110100_001`"
        step  = 1 * 60 * 1000 # 1 minute
        cols  = "ts,speed"

        # speed 120
        vals  = "120"
        count = 5
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # speed 80
        vals  = "80"
        count = 1
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

    #
    #  stream2 trigger 
    #
    def trigger_stream2(self):
        pass

    #
    #  stream3 trigger 
    #
    def trigger_stream3(self):
        pass
 


    #
    #  stream4 trigger 
    #
    def trigger_stream4(self):
        pass


    #
    #  stream4 trigger again
    #
    def trigger_stream4_again(self):
        pass


    #
    #  stream5 trigger 
    #
    def trigger_stream5(self):
        pass


    #
    #  stream6 trigger 
    #
    def trigger_stream6(self):
        pass

    #
    #  again stream6 trigger
    #
    def trigger_stream6_again(self):
        pass

    #
    #  stream7 trigger
    #
    def trigger_stream7(self):
        pass

    #
    #  stream8 trigger
    #
    def trigger_stream8(self):
        pass

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


        tdLog.info("verify stream1 .................................. successfully.")

    #
    # verify stream2
    #
    def verify_stream2(self):
        result_sql = f"select * from {self.vdb}.`result_stream2` "
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
        result_sql = f"select * from {self.vdb}.`result_stream3` "
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
            result_sql = f"select * from {self.vdb}.`{obj}` "
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
            result_sql = f"select * from {self.vdb}.`result_stream4_sub{i}` "
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

        # verify stream4_sub8
        # ***** bug5 ****
        #self.verify_stream4_sub8()

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
        result_sql = f"select * from {self.vdb}.`result_stream4_sub7` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 1
            and tdSql.checkData(0, 0, wins[0])
            and tdSql.checkData(0, 1, 10)
            and tdSql.checkData(0, 2, 400)
            and tdSql.checkData(0, 3, 10*200)
        )

    def verify_stream4_sub8(self):
        # result_stream4_sub8
        tdSql.checkResultsBySql(
            sql     = f"select * from {self.vdb}.`result_stream4_sub8` ", 
            exp_sql = f"select ts,1,voltage,power from {self.db}.`em-4` where ts >= 1752574200000 limit 119;"
        )
        tdLog.info("verify stream4_sub8 ............................. successfully.")

    def verify_stream4_sub9(self):
        # result_stream4_sub9
        ts = 1752487860000
        result_sql = f"select * from {self.vdb}.`result_stream4_sub9` "
        tdSql.checkResultsByFunc (
            sql = result_sql, 
            func = lambda: tdSql.getRows() == 119
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
        result_sql = f"select * from {self.vdb}.`result_stream4` "
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

        

        ''' ***** bug2 *****
        # restart dnode
        tdLog.info("restart dnode to verify stream4_sub1 ...")
        self.restartDnode()
        

        # result_stream4_sub1
        for i in range(10):
            # write 
            sqls = [
                "INSERT INTO {self.db}.`em-4`(ts,voltage,power) VALUES(1752574230000,2000,1000);",
                "INSERT INTO {self.db}.`em-4`(ts,voltage,power) VALUES(1752574230000,2001,10000);",
                "INSERT INTO {self.db}.`em-4`(ts,voltage,power) VALUES(1752581310000,2002,1001);"
            ]
            tdSql.executes(sqls)

            tdLog.info(f"loop check i={i} sleep 3s...")
            time.sleep(5)

            # verify
            self.verify_stream4(tables=["result_stream4_sub1"])
        '''    

        tdLog.info("verify stream4 again ............................ successfully.")

    
    #
    # verify stream5
    #

    def verify_stream5(self):
        # result_stream5
        result_sql = f"select * from {self.vdb}.`result_stream5` "
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
        result_sql = f"select * from {self.vdb}.`result_stream5_sub1` "
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, self.start2) # ts
            and tdSql.compareData(0, 1, 3 + 4 + 1)   # cnt
            and tdSql.compareData(0, 2, 31)          # last current
        )

        tdLog.info(f"verify stream5 sub1 ............................ successfully.")


    #
    # verify stream6
    #

    def verify_stream6(self):
        # result_stream6
        result_sql = f"select * from {self.vdb}.`result_stream6` "
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
        exp_sql = f"select * from {self.vdb}.`result_stream6_sub1` "

        tdSql.checkResultsBySql(result_sql, exp_sql)

        tdLog.info(f"verify stream6 ................................. successfully.")

    def verify_stream6_again(self):
        # no change
        self.verify_stream6()


    '''
    # verify stream6 sub1
    def verify_stream6_sub1(self):
        # result_stream6_sub1
        result_sql = f"select * from {self.vdb}.`result_stream6_sub1` "
        ts         = self.start2
        step       = 1 * 60 * 1000 # 1 minute
        cnt        = 5

        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 3
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
            # window3 disorder
            and tdSql.compareData(2, 0, ts + 10 * step) # ts
            and tdSql.compareData(2, 1, 5)       # cnt
            and tdSql.compareData(2, 2, 400)     # min(voltage)
            and tdSql.compareData(2, 3, 404)     # max(voltage)

        )

        tdLog.info(f"verify stream6 sub1 ............................ successfully.")
    '''    


    #
    # verify stream7
    #
    def verify_stream7(self):
        # result_stream7
        result_sql = f"select * from {self.vdb}.`result_stream7` "
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
        result_sql = f"select * from {self.vdb}.`result_stream8` "
        allCnt = 0

        tdSql.query(result_sql)
        count = tdSql.getRows()

        for i in range(count):
            # row
            cnt = tdSql.getData(i, 1)  # cnt
            allCnt += cnt
            if cnt <=0 or cnt > 5:
                tdLog.exit(f"stream8 row {i} cnt is {cnt}, not in [1, 5]")
            tdSql.checkData(i, 2, 200) # avg(voltage)
        if allCnt != 20:
            tdLog.exit(f"stream8 all cnt is {allCnt}, not 20")

        tdLog.info(f"verify stream8 ................................. successfully.")

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