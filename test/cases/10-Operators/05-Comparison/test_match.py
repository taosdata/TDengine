from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck,tdCom
import threading
import time
import taos

class TestRegex:
    #
    # ------------------- 1 ----------------
    #
    def do_sim_match(self):
        db = "testdb"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdLog.info(f"======================== regular expression match test")
        st_name = "st"
        ct1_name = "ct1"
        ct2_name = "ct2"

        tdSql.execute(
            f"create table {st_name} (ts timestamp, c1b binary(20)) tags(t1b binary(20));"
        )
        tdSql.execute(f"create table {ct1_name} using {st_name} tags('taosdata1')")
        tdSql.execute(f"create table {ct2_name} using {st_name} tags('taosdata2')")
        tdSql.execute(f"create table not_match using {st_name} tags('NOTMATCH')")

        tdSql.execute(f"insert into {ct1_name} values(now, 'this is engine')")
        tdSql.execute(f"insert into {ct2_name} values(now, 'this is app egnine')")
        tdSql.execute(f"insert into not_match values (now + 1s, '1234')")

        tdSql.query(f"select tbname from {st_name} where tbname match '.*'")
        tdSql.checkRows(3)

        tdSql.query(f"select tbname from {st_name} where tbname match '^ct[[:digit:]]'")
        tdSql.checkRows(2)

        tdSql.query(
            f"select tbname from {st_name} where tbname nmatch '^ct[[:digit:]]'"
        )
        tdSql.checkRows(1)

        tdSql.query(f"select tbname from {st_name} where tbname match '.*'")
        tdSql.checkRows(3)

        tdSql.query(f"select tbname from {st_name} where tbname nmatch '.*'")
        tdSql.checkRows(0)

        tdSql.query(f"select tbname from {st_name} where t1b match '[[:lower:]]+'")
        tdSql.checkRows(2)

        tdSql.query(f"select tbname from {st_name} where t1b nmatch '[[:lower:]]+'")
        tdSql.checkRows(1)

        tdSql.query(f"select c1b from {st_name} where c1b match 'engine'")
        tdSql.checkData(0, 0, "this is engine")

        tdSql.checkRows(1)

        tdSql.query(f"select c1b from {st_name} where c1b nmatch 'engine' order by ts")
        tdSql.checkData(0, 0, "this is app egnine")

        tdSql.checkRows(2)

        tdSql.error(f"select c1b from {st_name} where c1b match e;")
        tdSql.error(f"select c1b from {st_name} where c1b nmatch e;")

        tdSql.execute(
            f"create table wrong_type(ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 float, c5 double, c6 bool, c7 nchar(20)) tags(t0 tinyint, t1 smallint, t2 int, t3 bigint, t4 float, t5 double, t6 bool, t7 nchar(10))"
        )
        tdSql.execute(
            f"insert into wrong_type_1 using wrong_type tags(1, 2, 3, 4, 5, 6, true, 'notsupport') values(now, 1, 2, 3, 4, 5, 6, false, 'notsupport')"
        )
        tdSql.error(f"select * from wrong_type where ts match '.*'")
        tdSql.error(f"select * from wrong_type where ts nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c0 match '.*'")
        tdSql.error(f"select * from wrong_type where c0 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c1 match '.*'")
        tdSql.error(f"select * from wrong_type where c1 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c2 match '.*'")
        tdSql.error(f"select * from wrong_type where c2 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c3 match '.*'")
        tdSql.error(f"select * from wrong_type where c3 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c4 match '.*'")
        tdSql.error(f"select * from wrong_type where c4 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c5 match '.*'")
        tdSql.error(f"select * from wrong_type where c5 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c6 match '.*'")
        tdSql.error(f"select * from wrong_type where c6 nmatch '.*'")
        tdSql.query(f"select * from wrong_type where c7 match '.*'")
        tdSql.query(f"select * from wrong_type where c7 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where t1 match '.*'")
        tdSql.error(f"select * from wrong_type where t1 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where t2 match '.*'")
        tdSql.error(f"select * from wrong_type where t2 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where t3 match '.*'")
        tdSql.error(f"select * from wrong_type where t3 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where t4 match '.*'")
        tdSql.error(f"select * from wrong_type where t4 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where t5 match '.*'")
        tdSql.error(f"select * from wrong_type where t5 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where t6 match '.*'")
        tdSql.error(f"select * from wrong_type where t6 nmatch '.*'")
        tdSql.query(f"select * from wrong_type where t7 match '.*'")
        tdSql.query(f"select * from wrong_type where t7 nmatch '.*'")
        
        print("do sim match .......................... [passed]")

    #
    # ------------------- 2 ----------------
    #
    def initConnection(self):        
        self.records = 10000000
        self.numOfTherads = 50
        self.ts = 1537146000000
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/home/xp/git/TDengine/sim/dnode1/cfg"
        self.conn = taos.connect(
            self.host,
            self.user,
            self.password,
            self.config)
        
    def initDB(self):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db")
        
    def stopTest(self):
        tdSql.execute("drop database if exists db")
        
    def threadTest(self, threadID):
        print(f"Thread {threadID} starting...")
        tdsqln = tdCom.newTdSql()
        for i in range(2, 50):
            tdsqln.query(f"select distinct table_name from information_schema.ins_columns where table_name match 't.*{i}dx'")
            tdsqln.checkRows(0)
        for i in range(100):
            tdsqln.query(f"select distinct table_name from information_schema.ins_columns where table_name match 't.*1x'")
            tdsqln.checkRows(2)
        
            tdsqln.query("select * from db.t1x")
            tdsqln.checkRows(5)
            
            tdsqln.query("select * from db.t1x where c1 match '_c'")
            tdsqln.checkRows(2)
            
            tdsqln.query("select * from db.t1x where c1 match '%__c'")
            tdsqln.checkRows(0)
            
            tdsqln.error("select * from db.t1x where c1 match '*d'")
        
        print(f"Thread {threadID} finished.")

    def match_test(self):
        tdSql.execute("create table db.t1x (ts timestamp, c1 varchar(100))")
        tdSql.execute("create table db.t_1x (ts timestamp, c1 varchar(100))")

        tdSql.query(f"select distinct table_name from information_schema.ins_columns where table_name match 't.*1x'")
        tdSql.checkRows(2)
        for i in range(2, 50):
            tdSql.query(f"select distinct table_name from information_schema.ins_columns where table_name match 't.*{i}x'")
            tdSql.checkRows(0)

        tdSql.error("select * from db.t1x where c1 match '*d'")
        tdSql.query("insert into db.t1x values(now, 'abc'), (now+1s, 'a%c'),(now+2s, 'a_c'),(now+3s, '_c'),(now+4s, '%c')")
        
        tdSql.query("select * from db.t1x")
        tdSql.checkRows(5)
        
        tdSql.query("select * from db.t1x where c1 match '_c'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t1x where c1 match '%__c'")
        tdSql.checkRows(0)
        tdSql.error("select * from db.t1x where c1 match '*d'")
        threads = []
        for i in range(10):
            t = threading.Thread(target=self.threadTest, args=(i,))
            threads.append(t)
            t.start()
            
        time.sleep(31)
        
        tdSql.query(f"select distinct table_name from information_schema.ins_columns where table_name match 't.*1x'")
        tdSql.checkRows(2)
        for i in range(2, 50):
            tdSql.query(f"select distinct table_name from information_schema.ins_columns where table_name match 't.*{i}x'")
            tdSql.checkRows(0)
        
        tdSql.query("select * from db.t1x")
        tdSql.checkRows(5)
        
        tdSql.query("select * from db.t1x where c1 match '_c'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t1x where c1 match '%__c'")
        tdSql.checkRows(0)

        tdSql.execute("create table db.t3x (ts timestamp, c1 varchar(100))")
        
        tdSql.execute("insert into db.t3x values(now, '我是中文'), (now+1s, '我是_中文'), (now+2s, '我是%中文'), (now+3s, '%中文'),(now+4s, '_中文')")        
        tdSql.query("select * from db.t3x where c1 match '%中文'")
        tdSql.checkRows(2)
        tdSql.query("select * from db.t3x where c1 match '中文'")
        tdSql.checkRows(5)
        tdSql.error("select * from db.t1x where c1 match '*d'")
        
        tdSql.query("select * from db.t3x where c1 regexp '%中文'")
        tdSql.checkRows(2)
        tdSql.query("select * from db.t3x where c1 regexp '中文'")
        tdSql.checkRows(5)
        tdSql.query("select * from db.t3x where c1 not regexp '%中文'")
        tdSql.checkRows(3)
        tdSql.query("select * from db.t3x where c1 not regexp '中文'")
        tdSql.checkRows(0)
        tdSql.error("select * from db.t1x where c1 regexp '*d'")

        for thread in threads:
            print(f"Thread waitting for finish...")
            thread.join()
        
        print(f"Mutithread test finished.")
   
    def do_query_match(self):
        tdSql.prepare()
        self.initConnection()
        self.initDB()
        self.match_test()
        self.stopTest()
        
        print("do query match ........................ [passed]")

    #
    # ------------------- main ----------------
    #
    def test_match(self):
        """Operator match

        Match and nmatch for regular expression matching
        1. Match wildcard
        2. Match cnc wildcard
        3. Match error wildcard
        4. Match multithread
        6. Match regexp cnc wildcard
        7. Match error regexp wildcard
        8. Match regexp not cnc wildcard        
        9. Match on super/child table

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/parser/regex.sim
            - 2025-12-19 Alex Duan Migrated from uncatalog/system-test/2-query/test_match.py

        """
        self.do_sim_match()
        self.do_query_match()