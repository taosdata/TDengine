from wsgiref.headers import tspecials
from util.log import *
from util.cases import *
from util.sql import *
from util.common import tdCom
import numpy as np


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        
        self.dbname = "db"
        self.rowNum = 10
        self.ts = 1537146000000

    def notConditionTest(self):
        dbname = "nottest"
        stbname = "st1"
        
        tdsql = tdCom.newTdSql()
        tdsql.execute(f"create database if not exists {dbname}")
        
        stype = ["INT", "INT UNSIGNED", "BIGINT", "BIGINT UNSIGNED", "DOUBLE", "FLOAT", "SMALLINT", "SMALLINT UNSIGNED", "TINYINT", "TINYINT UNSIGNED"]
        
        for type_name in stype:
            tdsql.execute(f"drop table if exists {dbname}.{stbname}")
            tdsql.execute(f"create table if not exists {dbname}.{stbname} (ts timestamp, v1 {type_name}) tags(t1 {type_name})")
            tdsql.execute(f"insert into {dbname}.sub_1 using {dbname}.{stbname} tags(1) values({self.ts}, 10)")
            tdsql.execute(f"insert into {dbname}.sub_2 using {dbname}.{stbname} tags(2) values({self.ts + 1000}, 20)")
            tdsql.execute(f"insert into {dbname}.sub_3 using {dbname}.{stbname} tags(3) values({self.ts + 2000}, 30)")
    
            # Test case 1: NOT IN
            tdsql.query(f"select t1, * from {dbname}.{stbname} where t1 not in (1, 2) order by t1")
            tdsql.checkRows(1)
            tdsql.checkData(0, 0, 3)
    
            # Test case 2: NOT BETWEEN
            tdsql.query(f"select * from {dbname}.{stbname} where v1 not between 10 and 20 order by t1")
            tdsql.checkRows(1)
            tdsql.checkData(0, 1, 30)
            tdsql.query(f"select * from {dbname}.{stbname} where not(v1 not between 10 and 20) order by t1")
            tdsql.checkRows(2)
    
            # Test case 4: NOT EQUAL
            tdsql.query(f"select * from {dbname}.{stbname} where v1 != 20 order by t1")
            tdsql.checkRows(2)
            tdsql.checkData(0, 1, 10)
            tdsql.checkData(1, 1, 30)
    
            # Test case 8: NOT (v1 < 20 OR v1 > 30)
            tdsql.query(f"select * from {dbname}.{stbname} where not (v1 < 20 or v1 > 30) order by t1")
            tdsql.checkRows(2)
            tdsql.checkData(0, 1, 20)
            tdsql.checkData(1, 1, 30)
            
            tdsql.query(f"select * from {dbname}.{stbname} where not (v1 < 20 or v1 >= 30) order by t1")
            tdsql.checkRows(1)
    
            # Test case 9: NOT (t1 != 1)     
            tdsql.query(f"select * from {dbname}.{stbname} where not (t1 != 1) order by t1")
            tdsql.checkRows(1)
            tdsql.checkData(0, 1, 10)
            
            tdsql.query(f"select * from {dbname}.{stbname} where (t1 != 1) or not (v1 == 20) order by t1")
            tdsql.checkRows(3)
            tdsql.checkData(0, 1, 10)
            tdsql.checkData(1, 1, 20)
            tdsql.checkData(2, 1, 30)
            
            tdsql.query(f"select * from {dbname}.{stbname} where not((t1 != 1) or not (v1 == 20)) order by t1")
            tdsql.checkRows(0)
            
            tdsql.query(f"select * from {dbname}.{stbname} where not (t1 != 1) and not (v1 != 20) order by t1")
            tdsql.checkRows(0)
            
            tdsql.query(f"select * from {dbname}.{stbname} where not(not (t1 != 1) and not (v1 != 20)) order by t1")
            tdsql.checkRows(3)
    
            tdsql.query(f"select * from {dbname}.{stbname} where not (t1 != 1) and not (v1 != 10) order by t1")
            tdsql.checkRows(1)
            tdsql.checkData(0, 1, 10)
            
            tdsql.query(f"select * from {dbname}.{stbname} where not (t1 > 2) order by t1")
            tdsql.checkRows(2)
            tdsql.checkData(0, 1, 10)
            tdsql.checkData(1, 1, 20)
            
            tdsql.query(f"select * from {dbname}.{stbname} where not (t1 == 2) order by t1")
            tdsql.checkRows(2)
            tdsql.checkData(0, 1, 10)
            tdsql.checkData(1, 1, 30)
            
            tdsql.query(f"select * from {dbname}.{stbname} where not (v1 > 10 and v1 < 30) order by t1")
            tdsql.checkRows(2)
            tdsql.checkData(0, 1, 10)
            tdsql.checkData(1, 1, 30)
    
            tdsql.query(f"select * from {dbname}.{stbname} where not(not (v1 < 20 or v1 > 30)) order by t1")
            tdsql.checkRows(1)

            tdsql.query(f"select * from {dbname}.{stbname} where not(not (v1 < 20 or v1 >= 30)) order by t1")
            tdsql.checkRows(2)

            tdsql.query(f"select * from {dbname}.{stbname} where not(not (t1 != 1)) order by t1")
            tdsql.checkRows(2)

            tdsql.query(f"select * from {dbname}.{stbname} where not(not (t1 > 2)) order by t1")
            tdsql.checkRows(1)

            tdsql.query(f"select * from {dbname}.{stbname} where not(not (t1 == 2)) order by t1")
            tdsql.checkRows(1)

            tdsql.query(f"select * from {dbname}.{stbname} where not(not (v1 > 10 and v1 < 30)) order by t1")
            tdsql.checkRows(1)

    def run(self):
        dbname = "db"
        tdSql.prepare()
        
        self.notConditionTest()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())

tdCases.addLinux(__file__, TDTestCase())
