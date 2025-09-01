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

        # fill history
        self.fillHistory()

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
    #  create streams
    #
    def createStreams(self):
        print("start create streams ...")

        sqls = [
              "CREATE STREAM test.stream1      INTERVAL(5s) SLIDING(5s) FROM test.st PARTITION BY tbname STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO test.result_stream1      AS SELECT _twstart AS ts, _twrownum as wrownum, sum(ti) as sum_ti FROM %%trows",
              "CREATE STREAM test.stream1_sub1 INTERVAL(5s) SLIDING(5s) FROM test.st PARTITION BY gid    STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) INTO test.result_stream1_sub1 AS SELECT _twstart AS ts, _twrownum as wrownum, sum(ti) as sum_ti FROM %%trows",
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
        cols = "ts,ti,si,bin"

        # 
        count = 6
        vals = "10,10,'abcde'"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)


    #
    #  trigger stream2
    #
    def trigger_stream2(self):
        ts = self.start2
        table = "test.t2"
        step  =  1000 # 1s
        cols = "ts,ti,si,bin"

        # create table
        sql = f"create table {table} using test.st(gid) tags(1)"
        tdSql.execute(sql)

        # insert
        count = 6
        vals = "20,20,'abcde'"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)


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
            # ts           cnt   sum_ti
            [1752574200000, 5,   50, "t1"],
            [1752574200000, 5,  100, "t2"]
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 ................................. successfully.")

        # sub
        self.verify_stream1_sub1()

    def verify_stream1_sub1(self):
        # check
        result_sql = f"select * from test.result_stream1_sub1 "
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 1
        )

        # check data
        data = [
            # ts           cnt  sum_ti gid
            [1752574200000, 10,  150,   1]
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 sub1 ............................ successfully.")