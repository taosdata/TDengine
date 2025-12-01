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
        """IDMP: database manager scenario

        1. The stream running on databbase add/alter/delete
        2. The stream running on drop/alter trigger table 
        3. The stream running on drop/alter output table 
        4. The stream running on instability event (dnode restart or network interruption)
        5. The stream running on backup/restore data env
        6. The stream running on compact operator
        7. The stream running on splite/migrate vgroups
        8. Show/start/stop/drop stream 
        9. Show/drop snodes
        10. Stream options: FILL_HISTORY_FIRST|FILL_HISTORY

        Refer: https://taosdata.feishu.cn/wiki/Zkb2wNkHDihARVkGHYEcbNhmnxb

        Labels: common,ci,skip

        Since: v3.3.7.0

        History:
            - 2025-9-1 Alex Duan Created

        """

        #
        #  main test
        #

        # env
        tdStream.createSnode()

        # prepare data
        self.prepare()

        # fill history
        self.fillHistory()

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




    #
    # ---------------------   main flow frame    ----------------------
    #

    # 
    # prepare data
    #
    def prepare(self):
        
        self.start2 = 1752574200000

        # create database and table
        sql = "create database test"
        tdSql.execute(sql)
        print(sql)

        sql = "create stable \
                    test.st(ts  timestamp, bc  bool , fc  float ,dc  double ,ti  tinyint, si  smallint, ic  int ,bi bigint,  uti  tinyint unsigned, usi  smallint unsigned, ui  int unsigned, ubi  bigint unsigned, bin  binary(32) ,nch nchar(32)  ,var  varbinary(32), geo geometry(128)) \
                       tags (gid int,  tbc bool , tfc float ,tdc double ,tti tinyint, tsi smallint, tic int ,tbi bigint, tuti tinyint unsigned, tusi smallint unsigned, tui int unsigned, tubi bigint unsigned, tbin binary(32) ,tnch nchar(32) ,tvar varbinary(32))"
        print(sql)
        tdSql.execute(sql)

        sqls = [
            "create table test.t1 using test.st(gid) tags(1)",
        ]
        tdSql.executes(sqls)

        print("prepare data successfully.")

    # 
    #  fill history
    #
    def fillHistory(self):
        print("start fill history data ...")


    # 
    #  create vtables
    #
    def createVtables(self):
        print("start create vtables ...")
        sqls = [
            "use test",
            "CREATE STABLE vst_1 (ts TIMESTAMP , `电流` FLOAT, `电压` INT, `功率` BIGINT , `相位` SMALLINT ) TAGS (`地址` VARCHAR(50), `单元` TINYINT, `楼层` TINYINT, `设备ID` VARCHAR(20)) SMA(ts,`电流`) VIRTUAL 1;",
            "CREATE VTABLE vt_1  (`电流` FROM test.t1.fc,  `电压` FROM test.t1.ic,  `功率` FROM test.t1.bi,  `相位` FROM test.t1.si)  USING vst_1 ( `地址`, `单元`, `楼层`, `设备ID`) TAGS ('北京.海淀.上地街道', 1, 1, '10001');",
            #CREATE VTABLE vt_2 by dynamic 


        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} vtable successfully.")



    # 
    #  create streams
    #
    def createStreams(self):
        print("start create streams ...")

        sqls = [
              # stream1
              "CREATE STREAM test.stream1      INTERVAL(5s) SLIDING(5s) FROM test.st    PARTITION BY tbname STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|DELETE_RECALC)  INTO test.result_stream1      AS SELECT _twstart AS ts, _twrownum as wrownum, sum(bi)    as sum_power FROM %%trows",
              "CREATE STREAM test.stream1_sub1 INTERVAL(5s) SLIDING(5s) FROM test.st    PARTITION BY gid    STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|DELETE_RECALC)  INTO test.result_stream1_sub1 AS SELECT _twstart AS ts, _twrownum as wrownum, sum(bi)    as sum_power FROM %%trows",
              "CREATE STREAM test.stream1_sub2 INTERVAL(5s) SLIDING(5s) FROM test.vst_1 PARTITION BY tbname STREAM_OPTIONS(IGNORE_NODATA_TRIGGER)                INTO test.result_stream1_sub2 AS SELECT _twstart AS ts, _twrownum as wrownum, sum(`功率`) as `总功率`   FROM %%trows",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} streams successfully.")

    #
    #  check errors
    #
    def checkErrors(self):

        sqls = [
            # stream5
            "CREATE STREAM tdasset.err_stream5_sub1 SESSION(ts, 10m) FROM tdasset.vst_1 NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO tdasset.result_stream5_err (ts, col2 PRIMARY KEY, col3, col4, col5) TAGS ( mytbname varchar(100) as tbname) AS SELECT ts,tbname,`电压`,`电流`,地址 FROM tdasset.vst_1 WHERE ts >=_twstart AND ts <_twend",
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
        self.trigger_stream1()
        self.trigger_stream2()


    # 
    # 5. verify results
    #
    def verifyResults(self):
        print("wait 10s ...")
        time.sleep(10)
        print("verifyResults ...")
        self.verify_stream1()

    # 
    # 6. write trigger data again
    #
    def writeTriggerDataAgain(self):
        print("writeTriggerDataAgain ...")
        self.trigger_stream2_again()



    # 
    # 7. verify results again
    #
    def verifyResultsAgain(self):
        # wait for stream processing
        time.sleep(3)
        print("verifyResultsAgain ...")
        self.verify_stream1_again()
    

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

    #
    # ---------------------  trigger    ----------------------
    #

    #
    #  trigger stream1
    #
    def trigger_stream1(self):
        ts = self.start2
        table = "test.t1"
        step  =  1000 # 1s
        cols = "ts,fc,ic,bi,si,bin"

        # 
        count = 6
        vals = "10,10,10,10,'abcde'"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)


    #
    #  trigger stream2
    #
    def trigger_stream2(self):
        ts = self.start2
        table = "test.t2"
        step  =  1000 # 1s
        cols = "ts,fc,ic,bi,si,bin"

        # create table
        sql = f"create table {table} using test.st(gid) tags(1)"
        tdSql.execute(sql)

        # create vtable
        sql = "CREATE VTABLE vt_2  (`电流` FROM test.t2.fc,  `电压` FROM test.t2.ic,  `功率` FROM test.t2.bi,  `相位` FROM test.t2.si)  USING vst_1 ( `地址`, `单元`, `楼层`, `设备ID`) TAGS ('北京.海淀.上地街道', 1, 2, '10002')"
        tdSql.execute(sql)

        # insert
        count = 6
        vals = "20,20,20,20,'abcde'"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)


    #
    #  trigger stream2 again
    #
    def trigger_stream2_again(self):
        # delete table
        sql = "drop table test.t2"
        tdSql.execute(sql)
        print(sql)


    #
    # ---------------------  verify     ----------------------
    #


    #
    #  verify stream1
    #
    def verify_stream1(self):
        # check
        result_sql = f"select * from test.result_stream1 order by tag_tbname"
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 2
        )

        # check data
        data = [
            # ts           cnt  power
            [1752574200000, 5,   50, "t1"],
            [1752574200000, 5,  100, "t2"]
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 ................................. successfully.")

        # sub
        self.verify_stream1_sub1()
        self.verify_stream1_sub2()

    def verify_stream1_sub1(self):
        # check
        result_sql = f"select * from test.result_stream1_sub1 "
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 1
        )

        # check data
        data = [
            # ts           cnt  power  gid
            [1752574200000, 10,  150,   1]
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 sub1 ............................ successfully.")

    def verify_stream1_sub2(self):
        # check
        result_sql = f"select * from test.result_stream1_sub2 "
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 1
        )

        # check data
        data = [
            # ts           cnt  power tbname
            [1752574200000, 5,   50,  "vt_1"]
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 sub2 ............................ successfully.")

    #
    #  verify stream1 again
    #
    def verify_stream1_again(self):
        # check
        result_sql = f"select * from test.result_stream1 order by tag_tbname"
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 1
        )

        # check data
        data = [
            # ts           cnt  power
            [1752574200000, 5,   50, "t1"]
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 again ........................... successfully.")

        # sub 
        self.verify_stream1_sub1_again()
        
        
    def verify_stream1_sub1_again(self):
        # check
        result_sql = f"select * from test.result_stream1_sub1 "
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 1
        )

        # check data
        data = [
            # ts           cnt  power  gid
            [1752574200000, 5,   50,   1]
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 sub1 again ...................... successfully.")
