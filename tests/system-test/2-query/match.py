import taos
import sys
import datetime
import inspect
import threading
import time

from util.log import *
from util.sql import *
from util.cases import *
from util.common import tdCom

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        
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

        for thread in threads:
            print(f"Thread waitting for finish...")
            thread.join()
        
        print(f"Mutithread test finished.")
   
    def run(self):
        tdLog.printNoPrefix("==========start match_test run ...............")
        tdSql.prepare(replica = self.replicaVar)
        
        self.initConnection()

        self.initDB()
        self.match_test()
          
        self.stopTest()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
