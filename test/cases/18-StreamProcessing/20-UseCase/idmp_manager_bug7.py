import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, etool, sc, eutil, eos
from datetime import datetime
from datetime import date


class Test_IDMP_Meters:
    #
    #  taos.cfg config 
    #
    updatecfgDict = {
        "numOfMnodeStreamMgmtThreads"  : "4",
        "numOfStreamMgmtThreads"       : "5",
        "numOfVnodeStreamReaderThreads": "6",
        "numOfStreamTriggerThreads"    : "7",
        "numOfStreamRunnerThreads"     : "8"
    }

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
        11. Embed stream, create stream on the table that created by other stream

        Refer: https://taosdata.feishu.cn/wiki/Zkb2wNkHDihARVkGHYEcbNhmnxb

        Labels: common,ci,skip

        Since: v3.3.7.0

        Labels: common,ci

        JIRA: none

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

        # execute operation
        self.executeOperation()

        # verify results
        self.verifyResults()

        # check again
        self.checkStreamStatus()

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

        # start for history time
        self.start1 = 1752570000000
        # start for real time
        self.start2 = 1752574200000

        # create database and table
        sql = "create database test vgroups 2"
        tdSql.execute(sql)
        print(sql)

        sql = "create stable \
                    test.st(ts  timestamp, bc  bool , fc  float ,dc  double ,ti  tinyint, si  smallint, ic  int ,bi bigint,  uti  tinyint unsigned, usi  smallint unsigned, ui  int unsigned, ubi  bigint unsigned, bin  binary(32) ,nch nchar(32)  ,var  varbinary(32), geo geometry(128)) \
                       tags (gid int,  tbc bool , tfc float ,tdc double ,tti tinyint, tsi smallint, tic int ,tbi bigint, tuti tinyint unsigned, tusi smallint unsigned, tui int unsigned, tubi bigint unsigned, tbin binary(32) ,tnch nchar(32) ,tvar varbinary(32))"
        print(sql)
        tdSql.execute(sql)

        sqls = [
            "create table test.t1 using test.st(gid) tags(1)",
            # test.t2 create dynamic
            "create table test.t3 using test.st(gid) tags(3)",
            "create table test.t4 using test.st(gid) tags(3)",
            "create table test.t5 using test.st(gid) tags(5)",
            "create table test.t6 using test.st(gid) tags(5)",
        ]
        tdSql.executes(sqls)

        print("prepare data successfully.")

    #
    #  fill history
    #
    def fillHistory(self):
        print("start fill history data ...")
        self.history_stream6()
        print("fill history data successfully.")

    #
    #  create vtables
    #
    def createVtables(self):
        print("start create vtables ...")
        sqls = [
            "use test",
            "CREATE STABLE vst_1 (ts TIMESTAMP , `电流` FLOAT, `电压` INT, `功率` BIGINT , `相位` SMALLINT ) TAGS (`地址` VARCHAR(50), `单元` TINYINT, `楼层` TINYINT, `设备ID` VARCHAR(20)) SMA(ts,`电流`) VIRTUAL 1",
            "CREATE VTABLE vt_1  (`电流` FROM test.t1.fc,  `电压` FROM test.t1.ic,  `功率` FROM test.t1.bi,  `相位` FROM test.t1.si)  USING vst_1 ( `地址`, `单元`, `楼层`, `设备ID`) TAGS ('北京.海淀.上地街道', 1, 1, '10001')",
            # CREATE VTABLE vt_2 by dynamic
            "CREATE VTABLE vt_3  (`电流` FROM test.t3.fc,  `电压` FROM test.t3.ic,  `功率` FROM test.t3.bi,  `相位` FROM test.t3.si)  USING vst_1 ( `地址`, `单元`, `楼层`, `设备ID`) TAGS ('北京.海淀.上地街道', 1, 3, '10003')",
            "CREATE VTABLE vt_4  (`电流` FROM test.t4.fc,  `电压` FROM test.t4.ic,  `功率` FROM test.t4.bi,  `相位` FROM test.t4.si)  USING vst_1 ( `地址`, `单元`, `楼层`, `设备ID`) TAGS ('北京.海淀.上地街道', 1, 4, '10004')",
            "CREATE VTABLE vt_5  (`电流` FROM test.t5.fc,  `电压` FROM test.t5.ic,  `功率` FROM test.t5.bi,  `相位` FROM test.t5.si)  USING vst_1 ( `地址`, `单元`, `楼层`, `设备ID`) TAGS ('北京.海淀.上地街道', 1, 5, '10005')",
            "CREATE VTABLE vt_6  (`电流` FROM test.t6.fc,  `电压` FROM test.t6.ic,  `功率` FROM test.t6.bi,  `相位` FROM test.t6.si)  USING vst_1 ( `地址`, `单元`, `楼层`, `设备ID`) TAGS ('北京.海淀.上地街道', 1, 6, '10006')",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} vtable successfully.")

    #
    #  create streams
    #
    def createStreams(self):
        print("start create streams ...")

        sqls = [
              "create database out",

              # stream1
              "CREATE STREAM test.stream1      INTERVAL(5s) SLIDING(5s) FROM test.st    PARTITION BY tbname STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|DELETE_RECALC)  INTO test.result_stream1      AS SELECT _twstart AS ts, _twrownum as wrownum, sum(bi)    as sum_power FROM %%trows",
              "CREATE STREAM test.stream1_sub1 INTERVAL(5s) SLIDING(5s) FROM test.st    PARTITION BY gid    STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|DELETE_RECALC)  INTO test.result_stream1_sub1 AS SELECT _twstart AS ts, _twrownum as wrownum, sum(bi)    as sum_power FROM %%trows",
              "CREATE STREAM test.stream1_sub2 INTERVAL(5s) SLIDING(5s) FROM test.vst_1 PARTITION BY tbname STREAM_OPTIONS(IGNORE_NODATA_TRIGGER)                INTO test.result_stream1_sub2 AS SELECT _twstart AS ts, _twrownum as wrownum, sum(`功率`) as `总功率`   FROM %%trows",
              "CREATE STREAM test.stream1_sub3 INTERVAL(5s) SLIDING(5s) FROM test.vt_1                      STREAM_OPTIONS(IGNORE_NODATA_TRIGGER)                INTO test.result_stream1_sub3 AS SELECT _twstart AS ts, _twrownum as wrownum, sum(`功率`) as `总功率`   FROM %%trows",
              "CREATE STREAM test.stream1_sub4 INTERVAL(5s) SLIDING(5s) FROM test.st PARTITION BY gid       STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|DELETE_RECALC)  INTO test.result_stream1_sub4  AS SELECT _twstart AS ts, _twrownum as wrownum, sum(bi)   as sum_power FROM test.st where ts >= _twstart and ts < _twend partition by tbname",

              # stream3
              "CREATE STREAM test.stream3  INTERVAL(5s) SLIDING(5s) FROM test.t3  STREAM_OPTIONS(IGNORE_NODATA_TRIGGER|DELETE_RECALC)  INTO out.result_stream3  AS SELECT _twstart AS ts, _twrownum as wrownum, sum(bi)  as sum_power FROM %%trows",

              # stream4
              "CREATE TABLE test.o4 (ts TIMESTAMP , sum_cnt BIGINT, sum_power BIGINT)",
              "CREATE STREAM test.stream4      INTERVAL(5s)  SLIDING(5s)  FROM test.t4  INTO test.o4                  AS SELECT _twstart AS ts, _twrownum as sum_cnt,                          sum(bi)        as sum_power     FROM %%trows",
              "CREATE STREAM test.stream4_sub1 INTERVAL(10s) SLIDING(10s) FROM test.o4  INTO test.result_stream4_sub1 AS SELECT _twstart AS ts, _twrownum as cnt,     sum(sum_cnt) as cnt_all, sum(sum_power) as sum_power_all FROM %%trows",

              # stream5
              "CREATE STREAM test.stream5      EVENT_WINDOW( START WITH `电压` > 250 and `电流` > 50 END WITH `电压` <= 250 and `电流` <= 50 ) TRUE_FOR(5s) FROM test.vt_5  STREAM_OPTIONS(FILL_HISTORY) NOTIFY('ws://idmp:6042/recv/?key=man_stream5')       ON(WINDOW_OPEN|WINDOW_CLOSE)         INTO test.result_stream5      AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电流`) AS `最小电流`, MAX(`电流`) AS `最大电流`, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `总功率` FROM %%trows",
              "CREATE STREAM test.stream5_sub1 EVENT_WINDOW( START WITH `电压` > 250 and `电流` > 50 END WITH `电压` <= 250 and `电流` <= 50 ) TRUE_FOR(5s) FROM test.vt_5                               NOTIFY('ws://idmp:6042/recv/?key=man_stream5_sub1')  ON(WINDOW_CLOSE) WHERE `最小电流`<=48 INTO test.result_stream5_sub1 AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电流`) AS `最小电流`, MAX(`电流`) AS `最大电流`, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `总功率` FROM %%trows",


              # stream6
              "CREATE STREAM test.stream6      EVENT_WINDOW( START WITH `电压` > 250 and `电流` > 50 END WITH `电压` <= 250 and `电流` <= 50 ) TRUE_FOR(5s) FROM test.vt_6  STREAM_OPTIONS(FILL_HISTORY) NOTIFY('ws://idmp:6042/recv/?key=man_stream6')        ON(WINDOW_OPEN|WINDOW_CLOSE)        INTO test.result_stream6      AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电流`) AS `最小电流`, MAX(`电流`) AS `最大电流`, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `总功率` FROM %%trows",
        ]

        tdSql.executes(sqls)
        print(f"create {len(sqls)} streams successfully.")

    #
    #  check errors
    #
    def checkErrors(self):
        # check error operator
        sqls = [
            # operator
            # DB error: Rename column only available for normal table [0x80002649]
            "alter table test.st rename column ic renameic;",
            # DB error: Col/Tag referenced by stream [0x80002691]
            "alter table test.t1 set tag gid=10;",
            # DB error: Virtual table stream exists, use FORCE when ensure no impact [0x8000700F]
            "split vgroup 2",
            "split vgroup 4",
            # DB error: stream Out table cols count mismatch [0x80004110]
            "CREATE STREAM test.err_stream1 INTERVAL(5s) SLIDING(5s) FROM test.t3 INTO test.result_stream1 AS SELECT _twstart AS ts, _twrownum as wrownum, sum(bi), sum(ic) as sum_power FROM %%trows",
            # %%trows can not be used with WHERE clause.
            "CREATE STREAM test.err_stream2 INTERVAL(5s) SLIDING(5s) FROM test.st PARTITION BY gid INTO test.result_stream1_sub5  AS SELECT _twstart AS ts, _twrownum as wrownum, sum(bi)   as sum_power FROM %%trows where ts >= _twstart and ts < _twend partition by tbname",
            # DB error: only tag and tbname can be used in partition [0x8000410E]
            "CREATE STREAM test.err_stream3 INTERVAL(5s) SLIDING(5s) FROM test.st PARTITION BY ic  INTO test.result_stream1_sub5  AS SELECT _twstart AS ts, _twrownum as wrownum, sum(bi)   as sum_power FROM %%trows",
            # DB error: Snode still in use with streams [0x80007007]
            "drop snode on dnode 1;",
            # DB error: Only one snode can be created in each dnode [0x800003A4]
            "create snode on dnode 1;",
        ]

        tdSql.errors(sqls)
        print(f"check {len(sqls)} errors sql successfully.")

    #
    # 3. wait stream ready
    #
    def checkStreamStatus(self):
        print("wait stream ready ...")
        tdStream.checkStreamStatus()
        # verify config
        self.verify_config()
        tdLog.info(f"check stream status successfully.")

    #
    # 4. write trigger data
    #
    def writeTriggerData(self):
        print("writeTriggerData ...")
        self.trigger_stream1()
        self.trigger_stream2()
        self.trigger_stream3()
        self.trigger_stream4()
        self.trigger_stream5()

    #
    # 5. verify results
    #
    def verifyResults(self):
        print("wait 10s ...")
        time.sleep(10)
        print("verifyResults ...")
        self.verify_stream1()
        self.verify_stream3()
        self.verify_stream4()
        # ***** bug5 *****
        #self.verify_stream5()
        # ***** bug6 *****
        #self.verify_stream6()

    #
    # execute operation
    #
    def executeOperation(self):
        print("execute Operation ...")
        self.execManager()

    #
    # 6. write trigger data again
    #
    def writeTriggerDataAgain(self):
        print("writeTriggerDataAgain ...")
        self.trigger_stream1_again()
        self.trigger_stream2_again()
        self.trigger_stream3_again()
        self.trigger_stream4_again()

    #
    # 7. verify results again
    #
    def verifyResultsAgain(self):
        # wait for stream processing
        time.sleep(3)
        print("verifyResultsAgain ...")
        # ***** bug1 *****
        # self.verify_stream1_again()
        self.verify_stream3_again()
        self.verify_stream4_again()

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

    #
    # check found count rule: 0 equal, 1 greater, 2 less
    #
    def checkTaosdLog(self, key, expect = -1, rule = 0):
        cnt = eutil.findTaosdLog(key)
        if expect == -1:
            if cnt <= 0:
                tdLog.exit(f"check taosd log failed, key={key} not found.")
            else:
                print(f"check taosd log success, key:{key} found cnt:{cnt}.")
        else:
            if rule == 0 and cnt != expect:
                tdLog.exit(f"check taosd log failed, key={key} expect:{expect} != actual:{cnt}.")
            elif rule == 1 and cnt < expect:
                tdLog.exit(f"check taosd log failed, key={key} expect:{expect} > actual:{cnt}.")
            elif rule == 2 and cnt > expect:
                tdLog.exit(f"check taosd log failed, key={key} expect:{expect} < actual:{cnt}.")
            else:
                print(f"check taosd log success, key:{key} expect:{expect} rule:{rule} actual:{cnt}.")

    #
    # ---------------------   find other   ----------------------
    #

    def exec(self, sql):
        print(sql)
        tdSql.execute(sql)


    def execManager(self):
        # snodes

        
        # sqls
        sqls = [
            "show test.streams",
            "show snodes"
        ]
        for sql in sqls:
            self.exec(sql)  


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

    def alter_table(self):
        # alter
        self.exec("alter table st add column newc1 int;")
        self.exec("alter table st add column newc2 binary(12);")
        self.exec("alter table st drop column newc1 ;")
        self.exec("alter table st modify column newc2 binary(24);")
        self.exec("alter table test.t3 set tag tic=100;")

    #
    # verify config 
    # 
    def verify_config(self):
        # thread count
        items = {
           "mnode-stream-mg": 4,
           "dnode-stream-mg": 5,
           # ***** bug4 *****
           #"vnode-st-reader": 6,
           "snode-stream-tr": 7,
           "snode-stream-ru": 8
        }
        for key in items:
            expect = items[key]
            actual = eutil.findTaosdThread(key)
            if actual < expect:
                tdLog.exit(f"verify taos.cfg failed, thread:{key} expect: {expect} > actual: {actual}.")
            else:
                print(f"verify taos.cfg thread:{key} expect: {expect} <= actual: {actual}.")

        print("verify taos.cfg config ......................... successfully.")


    #
    # ---------------------  trigger    ----------------------
    #

    #
    #  trigger stream1
    #
    def trigger_stream1(self):
        ts = self.start2
        table = "test.t1"
        step = 1000  # 1s
        cols = "ts,fc,ic,bi,si,bin"

        # insert
        count = 6
        vals = "10,10,10,10,'abcde'"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

    #
    #  trigger stream1 again
    #
    def trigger_stream1_again(self):
        # drop
        sql = "drop vtable test.vt_1"
        self.exec(sql)

    #
    #  trigger stream2
    #
    def trigger_stream2(self):
        ts = self.start2
        table = "test.t2"
        step = 1000  # 1s
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
        self.exec("drop table test.t2")

    #
    #  trigger stream3
    #
    def trigger_stream3(self):
        ts = self.start2
        table = "test.t3"
        step = 1000  # 1s
        cols = "ts,fc,ic,bi,si,bin"

        # insert
        count = 6
        vals = "5,5,5,5,'abcde'"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)
        self.alter_table()

    #
    #  trigger stream3 again
    #
    def trigger_stream3_again(self):
        # drop
        self.exec("drop table out.result_stream3")

        ts = self.start2
        table = "test.t3"
        step = 1000  # 1s
        cols = "ts,fc,ic,bi,si,bin"

        # blank 10
        ts += 10 * step

        # insert
        count = 6
        vals = "10,10,10,10,'abcde'"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

    #
    #  trigger stream4
    #
    def trigger_stream4(self):
        ts    = self.start2
        table = "test.t4"
        step  =  1000 # 1s
        cols  = "ts,fc,ic,bi,si,bin"

        # insert
        count    = 21
        vals     = "5,5,5,5,'abcde'"
        self.ts4 = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

    #
    #  trigger stream4 again
    #
    def trigger_stream4_again(self):
        ts    = self.ts4
        table = "test.t4"
        step  =  1000 # 1s
        cols  = "ts,fc,ic,bi,si,bin"

        # insert
        count = 20
        vals  = "5,5,5,10,'aaaaa'"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

    #
    #  trigger stream5
    #
    def trigger_stream5(self):
        ts    = self.start1
        table = "test.t5"
        step  = 1000 # 1s
        cols  = "ts,fc,ic,bi,si,bin"

        # trigger
        count = 6
        vals  = "51,251,10,1,'abcde'"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # close trigger
        count = 4
        vals  = "49,249,10,2,'abcde'"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # trigger
        count = 6
        vals  = "52,252,10,3,'abcde'"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # close trigger
        count = 4
        vals  = "48,248,10,4,'abcde'"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)
        

    #
    #  history stream6
    #
    def history_stream6(self):
        ts    = self.start1
        table = "test.t6"
        step  = 1000 # 1s
        cols  = "ts,fc,ic,bi,si,bin"

        # trigger
        count = 6
        vals  = "51,251,10,1,'abcde'"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # close trigger
        count = 4
        vals  = "49,249,10,2,'abcde'"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # trigger
        count = 6
        vals  = "52,252,10,3,'abcde'"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # close trigger
        count = 4
        vals  = "48,248,10,4,'abcde'"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)



    #
    # ---------------------  verify     ----------------------
    #

    #
    #  verify stream1
    #
    def verify_stream1(self):
        # check
        result_sql = "select * from test.result_stream1 where tag_tbname in('t1','t2') order by tag_tbname"
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 2)

        # check data
        data = [
            # ts           cnt  power
            [1752574200000, 5, 50, "t1"],
            [1752574200000, 5, 100, "t2"],
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 ................................. successfully.")

        # sub
        self.verify_stream1_sub1()
        self.verify_stream1_sub2()

    def verify_stream1_sub1(self):
        # check
        result_sql = f"select * from test.result_stream1_sub1 where gid=1 "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 1)

        # check data
        data = [
            # ts           cnt  power  gid
            [1752574200000, 10, 150, 1]
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 sub1 ............................ successfully.")

    def verify_stream1_sub2(self):
        # check
        result_sql = f"select * from test.result_stream1_sub2 where tag_tbname in('vt_1','vt_2','vt_3') order by tag_tbname"
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 3)

        # check data
        data = [
            # ts           cnt  power tbname
            [1752574200000, 5, 50,  "vt_1"],
            [1752574200000, 5, 100, "vt_2"],
            [1752574200000, 5, 25,  "vt_3"],
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 sub2 ............................ successfully.")

    #
    #  verify stream1 again
    #
    def verify_stream1_again(self):
        # check
        result_sql = f"select * from test.result_stream1 order by tag_tbname"
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 1)

        # check data
        data = [
            # ts           cnt  power
            [1752574200000, 5, 50, "t1"]
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 again ........................... successfully.")

        # sub
        self.verify_stream1_sub1_again()
        self.verify_stream1_sub3_again()

    def verify_stream1_sub1_again(self):
        # check
        result_sql = f"select * from test.result_stream1_sub1 "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 1)

        # check data
        data = [
            # ts           cnt  power  gid
            [1752574200000, 5, 50, 1]
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 sub1 again ...................... successfully.")

    def verify_stream1_sub3_again(self):
        # check
        result_sql = f"select * from out.result_stream1_sub3 "
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 1)

        # check data
        data = [
            # ts           cnt  power  gid
            [1752574200000, 5, 50, 1]
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 sub3 again ...................... successfully.")

    #
    #  verify stream3
    #
    def verify_stream3(self):
        # check
        result_sql = f"select * from out.result_stream3"
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() == 1)

        # check data
        data = [
            # ts           cnt  power
            [1752574200000, 5, 25]
        ]
        tdSql.checkDataMem(result_sql, data)
        print("verify stream3 ................................. successfully.")

    #
    #  verify stream3 again
    #
    def verify_stream3_again(self):
        # check
        result_sql = f"select * from out.result_stream3"
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() >= 2
        )
        print("verify stream3 again ........................... successfully.")


    #
    #  verify stream4
    #
    def verify_stream4(self):
        # o4
        data = [
            # ts           cnt  power
            [1752574200000, 5,   25],
            [1752574205000, 5,   25],
            [1752574210000, 5,   25],
            [1752574215000, 5,   25]
        ]
        result_sql = f"select * from test.o4"
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == len(data)
        )
        tdSql.checkDataMem(result_sql, data)

        # result_stream4_sub1
        data = [
            # ts           cnt sum_cnt sum_power
            [1752574200000, 2,  10,  50]
        ]
        result_sql = f"select * from test.result_stream4_sub1"
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == len(data)
        )
        tdSql.checkDataMem(result_sql, data)

    #
    #  verify stream4 again
    #
    def verify_stream4_again(self):
        # o4
        data = [
            # ts           cnt  power
            [1752574200000, 5,   25],
            [1752574205000, 5,   25],
            [1752574210000, 5,   25],
            [1752574215000, 5,   25],
            [1752574220000, 5,   25],
            [1752574225000, 5,   25],
            [1752574230000, 5,   25],
            [1752574235000, 5,   25]
        ]
        result_sql = f"select * from test.o4"
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == len(data)
        )
        tdSql.checkDataMem(result_sql, data)


        # result_stream4_sub1
        data = [
            # ts           cnt sum_cnt sum_power
            [1752574200000, 2,  10,  50],
            [1752574210000, 2,  10,  50],
            [1752574220000, 2,  10,  50]
        ]
        result_sql = f"select * from test.result_stream4_sub1"
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == len(data)
        )
        tdSql.checkDataMem(result_sql, data)

        print("verify stream4 again ........................... successfully.")


    #
    #  verify stream5
    #
    def verify_stream5(self):
        # mem
        data = [
            # ts           cnt  min_cur  max_cur  min_vol  max_vol  sum_power
            [1752574200000, 7,  49,  51,  249,  251,  70],
            [1752574210000, 7,  48,  52,  248,  252,  70]
        ]
        result_sql = f"select * from test.result_stream5"
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == len(data)
        )
        tdSql.checkDataMem(result_sql, data)
        print("verify stream5 ................................. successfully.")

        # sub
        self.verify_stream5_sub1()


    def verify_stream5_sub1(self):
        # mem
        data = [
            # ts           cnt  min_cur  max_cur  min_vol  max_vol  sum_power
            [1752574200000, 7,  49,  51,  249,  251,  70],
            [1752574210000, 7,  48,  52,  248,  252,  70]
        ]
        result_sql = f"select * from test.result_stream5_sub1"
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == len(data)
        )
        tdSql.checkDataMem(result_sql, data)

        # check taosd log
        self.checkTaosdLog("?key=man_stream5_sub1", expect = 1)
        print("verify stream5 sub1 ............................ successfully.")

    #
    #  verify stream6
    #
    def verify_stream6(self):
        # mem
        data = [
            # ts           cnt  min_cur  max_cur  min_vol  max_vol  sum_power
            [1752570000000, 7,  49,  51,  249,  251,  70],
            [1752570010000, 7,  48,  52,  248,  252,  70]
        ]
        result_sql = f"select * from test.result_stream6"
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == len(data)
        )
        tdSql.checkDataMem(result_sql, data)   
        print("verify stream6 ................................. successfully.")