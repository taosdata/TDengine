import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, etool, sc, eutil, eos
from datetime import datetime
from datetime import date


class Test_IDMP_Meters:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_usecase_em(self):
        """IDMP: database manager scenario

        1. The stream privilege on belong to database
        2. The stream privilege on trigger   database
        3. The stream privilege on output    database
        4. The stream privilege on query     databases


        Refer: https://taosdata.feishu.cn/wiki/Zkb2wNkHDihARVkGHYEcbNhmnxb

        Labels: common,ci,skip

        Since: v3.3.7.0

        Labels: common,ci

        JIRA: none

        History:
            - 2025-9-19 Alex Duan Created
        """

        #
        #  main test
        #

        # env
        tdStream.createSnode()

        # resource like db , table
        self.createResource()

        # create users
        self.createUsers()

        # grant privilege
        self.grantPrivilege()

        # connect with user1
        self.connectCheckUser1()

        # connect with user2
        #self.connectCheckUser2()

        # connect with user3
        #self.connectCheckUser3()

        self.connectWithRoot()

        # wait stream ready
        self.checkStreamStatus()

        # insert trigger data
        self.writeTriggerData()

        # verify results
        self.verifyResults()
    #
    # ---------------------   util   ----------------------
    #

    def exec(self, sql):
        if sql is None or sql.strip() == "":
            return
        print(sql)
        tdSql.execute(sql)

    def execs(self, sqls):
        for sql in sqls:
            self.exec(sql)

    #
    # wait stream ready
    #
    def checkStreamStatus(self):
        print("wait stream ready ...")
        tdStream.checkStreamStatus()


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
    # ---------------------   main flow frame    ----------------------
    #

    #
    # createResource
    #
    def createResource(self):
        # start for history time
        self.start1 = 1752570000000
        # start for real time
        self.start2 = 1752574200000

        dbCnt      = 3
        childCnt   = 5
        for i in range(dbCnt):
            # create db
            db = f"db{i+1}"
            tdSql.execute(f"create database {db}")
            tdSql.execute(f"create database {db}_out")
            tdSql.execute(f"use {db}")
            # create stb
            tdSql.execute(f"create table meters(ts timestamp, current float, voltage int, phase float, power int) tags(groupid int)")
            # create child table
            for j in range(childCnt):
                table = f"d{j}"
                tdSql.execute(f"create table {table} using meters tags({j}) ")

        print("create resource successfully.")

    #
    #  create users
    #
    def createUsers(self):

        sqls = [
            "create user user1 pass 'taosdata' sysinfo 1 createdb 1",
            "create user user2 pass 'taosdata' sysinfo 0 createdb 0"
        ]

        self.execs(sqls)
        print("create users successfully.")

    #
    #  grant privilege
    #
    def grantPrivilege(self):
        sqls = [
            #
            # user1
            #

            # db1 all
            "grant all   on db1        to user1",
            "grant all   on db1_out    to user1",
            # db2 read
            "grant read  on db2        to user1",
            "grant read  on db2_out    to user1",
            # db3 write
            "grant write on db3        to user1",
            "grant write on db3_out    to user1",            
        ]

        self.execs(sqls)
        tdLog.info(f"grant privilege successfully.")
        

    #
    #  connect with root
    #
    def connectWithRoot(self):
        print("connect with root ...")
        tdSql.connect(user="root", password="taosdata")

    def createStream(self, streamName, trigger, into, calc, ok):
        # select
        select = "SELECT _twstart AS ts, _twrownum as wrownum, sum(power) as sum_power FROM "
        if calc is None:
            select += "%%trows"
        else:
            select += f"{calc} WHERE ts >= _twstart AND ts < _twend"
        # exec
        sql = f"CREATE STREAM {streamName} INTERVAL(5s) SLIDING(5s) FROM {trigger} PARTITION BY tbname STREAM_OPTIONS(FILL_HISTORY) INTO {into}  AS {select}"
        if ok:
            tdSql.execute(sql)
            print(f"create stream {streamName} successfully.")
        else:
            tdSql.error(sql)
            print(f"create stream {streamName} expect error .")

    def createStreamOK(self, streamName, trigger, into, calc):
        self.createStream(streamName, trigger, into, calc, True)

    def createStreamError(self, streamName, trigger, into, calc):
        self.createStream(streamName, trigger, into, calc, False)

    #
    #  connect check user1
    #
    def connectCheckUser1(self):
        print("connect with user1 ...")
        tdSql.connect(user="user1", password="taosdata")

        #
        #     -------------- ok --------------
        #

        #                      write          read           write                    read
        #                      stream         trigger        into                     calc 
        # db1
        self.createStreamOK("db1.stream11", "db1.meters", "db1_out.result_stream11", "db1.meters")
        self.createStreamOK("db1.stream12", "db1.meters", "db1_out.result_stream12",  None)
        self.createStreamOK("db1.stream13", "db1.meters", "db1_out.result_stream13", "db2.meters")
        self.createStreamOK("db1.stream14", "db1.meters", "db3_out.result_stream14", "db1.meters")
        self.createStreamOK("db1.stream15", "db2.meters", "db1_out.result_stream15", "db1.meters")
        self.createStreamOK("db1.stream16", "db2.meters", "db3_out.result_stream16", "db2.meters")
        # db2 nothing can do
        # db3
        self.createStreamOK("db3.stream31", "db2.meters", "db3_out.result_stream31", "db2.meters")
        self.createStreamOK("db3.stream32", "db1.meters", "db3_out.result_stream32",  None)
        self.createStreamOK("db3.stream33", "db2.meters", "db1_out.result_stream33", "db1.meters")
        self.createStreamOK("db3.stream34", "db1.meters", "db1_out.result_stream34", "db1.meters")
        self.createStreamOK("db3.stream35", "db2.meters", "db1_out.result_stream35", "db2.meters")
        self.createStreamOK("db3.stream36", "db2.meters", "db3_out.result_stream36", "db1.meters")


        #
        #     -------------- error --------------
        #

        #                          write              read             write                      read
        #                          stream             trigger          into                       calc 
        # db1
        self.createStreamError("db1.stream11_err", "db1.meters", "db1_out.result_stream11_err", "db3.meters")
        self.createStreamError("db1.stream12_err", "db1.meters", "db2_out.result_stream12_err", "db1.meters")
        self.createStreamError("db1.stream13_err", "db3.meters", "db1_out.result_stream13_err", "db1.meters")
        # db2
        self.createStreamError("db2.stream21_err", "db2.meters", "db2_out.result_stream21_err", "db2.meters")
        self.createStreamError("db2.stream21_err", "db1.meters", "db2_out.result_stream21_err", "db2.meters")
        self.createStreamError("db2.stream21_err", "db2.meters", "db3_out.result_stream21_err", "db1.meters")
        # db3
        self.createStreamError("db3.stream31_err", "db3.meters", "db3_out.result_stream31_err", "db3.meters")
        self.createStreamError("db3.stream32_err", "db3.meters", "db3_out.result_stream32_err", "db2.meters")
        self.createStreamError("db3.stream33_err", "db3.meters", "db2_out.result_stream33_err", "db2.meters")
        self.createStreamError("db3.stream34_err", "db1.meters", "db3_out.result_stream34_err", "db3.meters")


    #
    # write trigger data
    #
    def writeTriggerData(self):
        print("writeTriggerData ...")
        self.trigger_stream1()


    #
    # verify results
    #
    def verifyResults(self):
        print("wait 10s ...")
        time.sleep(10)
        print("verifyResults ...")
        self.verify_stream1()

    #
    # ---------------------  trigger  and verify  ----------------------
    #

    #
    #  trigger stream1
    #
    def trigger_stream1(self):
        table = "db1.d0"
        step  = 1000  # 1s
        cols  = "ts,current,voltage,power"
        ts    = self.start2

        count = 6
        vals  = "5, 200, 10"
        ts    = tdSql.insertFixedVal(table, ts, step, count, cols, vals)


    #
    #  verify stream1
    #
    def verify_stream1(self):
        # check
        result_sql = "select * from db1_out.result_stream11 where tag_tbname in('d0') order by tag_tbname"
        data = [
            # ts           cnt  power
            [1752574200000, 5,  50, "d0"]
        ]
        
        # rows
        tdSql.checkResultsByFunc(
            sql  = result_sql,
            func = lambda: tdSql.getRows() == len(data)
        )
        # mem
        tdSql.checkDataMem(result_sql, data)
        print("verify stream1 ................................. successfully.")