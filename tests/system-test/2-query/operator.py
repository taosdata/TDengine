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

    # test in/not in contidion with invalid value
    def ts5757(self):
        
        tdSql.execute(f"create database if not exists {self.dbname}")
        
        tdSql.execute(f"DROP STABLE IF EXISTS {self.dbname}.super_t1;")
        tdSql.execute(f"DROP TABLE IF EXISTS {self.dbname}.t1;")
        tdSql.execute(f"CREATE STABLE IF NOT EXISTS {self.dbname}.super_t1(time TIMESTAMP, c0 BIGINT UNSIGNED) TAGS (location BINARY(64))")
        tdSql.execute(f"CREATE TABLE {self.dbname}.t1 USING {self.dbname}.super_t1 TAGS ('ek')")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c0) VALUES (1641024000000, 1);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c0) VALUES (1641024005000, 2);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c0) VALUES (1641024010000, NULL);")

        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL AND c0 IN (-1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL AND c0 IN (-1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL AND c0 IN (-1, 1);")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL AND c0 IN (2, -1, 1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL AND c0 NOT IN (-1);")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL AND c0 NOT IN (-1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL AND c0 NOT IN (3);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL AND c0 NOT IN (-1, 1);")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL AND c0 NOT IN (2, -1, 1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE (c0 IS NULL AND c0 IN (-1)) or c0 in(1)")
        tdSql.checkRows(1)
        
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL OR c0 IN (-1);")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL OR c0 IN (-1);")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL OR c0 IN (-1, 1);")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL OR c0 IN (2, -1, 1);")
        tdSql.checkRows(3)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL OR c0 NOT IN (-1);")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL OR c0 NOT IN (-1);")
        tdSql.checkRows(3)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL OR c0 NOT IN (3);")
        tdSql.checkRows(3)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL OR c0 NOT IN (-1, 1);")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL OR c0 NOT IN (-1);")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL OR c0 NOT IN (2, -1, 1);")
        tdSql.checkRows(1)
        
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE ((c0 is NULL) AND  (c0 in (-1)) )")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE ((c0 in (-1)) AND (c0 is NULL) )")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE ((c0 in (-1)) AND (c0 is NULL) ) OR c0 in(1)")
        tdSql.checkRows(1)
        
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (-1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL;")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (-1) or c0 in(1);")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (1) or c0 in(-1);")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (-1) or c0 in(-1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (-1) and c0 in(1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (1) and c0 in(-1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (-1) and c0 in(-1);")
        tdSql.checkRows(0)

    def run(self):
        dbname = "db"
        tdSql.prepare()
        tdSql.execute(f"create database if not exists {self.dbname}")
        
        self.ts5757()
        



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())

tdCases.addLinux(__file__, TDTestCase())
