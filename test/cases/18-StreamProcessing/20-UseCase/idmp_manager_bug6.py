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
              # stream6
              "CREATE STREAM test.stream6 EVENT_WINDOW( START WITH `电压` > 250 and `电流` > 50 END WITH `电压` <= 250 and `电流` <= 50 ) TRUE_FOR(5s) FROM test.vt_6  STREAM_OPTIONS(FILL_HISTORY) NOTIFY('ws://idmp:6042/recv/?key=man_stream6')  ON(WINDOW_OPEN|WINDOW_CLOSE) INTO test.result_stream6 AS SELECT _twstart AS ts, COUNT(*) AS cnt, MIN(`电流`) AS `最小电流`, MAX(`电流`) AS `最大电流`, MIN(`电压`) AS `最小电压`, MAX(`电压`) AS `最大电压`, SUM(`功率`) AS `总功率` FROM %%trows",
        ]

        tdSql.executes(sqls)
        print(f"create {len(sqls)} streams successfully.")


    #
    # 3. wait stream ready
    #
    def checkStreamStatus(self):
        print("wait stream ready ...")
        tdStream.checkStreamStatus()

    #
    # 4. write trigger data
    #
    def writeTriggerData(self):
        print("writeTriggerData ...")

    #
    # 5. verify results
    #
    def verifyResults(self):
        print("wait 10s ...")
        time.sleep(10)
        print("verifyResults ...")
        self.verify_stream6()

    #
    # execute operation
    #
    def executeOperation(self):
        print("execute Operation ...")

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
