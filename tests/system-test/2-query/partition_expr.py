from wsgiref.headers import tspecials
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.batchNum = 5
        self.ts = 1537146000000

    def run(self):
        dbname = "db"
        tdSql.prepare()

        tdSql.execute(f'''create table sta(ts timestamp, f int, col2 bigint) tags(tg1 int, tg2 binary(20))''')
        tdSql.execute(f"create table sta1 using sta tags(1, 'a')")
        tdSql.execute(f"insert into sta1 values(1537146000001, 11, 110)")
        tdSql.execute(f"insert into sta1 values(1537146000002, 12, 120)")
        tdSql.execute(f"insert into sta1 values(1537146000003, 13, 130)")

        tdSql.query("select _wstart, f+100, count(*) from db.sta partition by f+100 session(ts, 1a) order by _wstart");
        tdSql.checkData(0, 1, 111.0)
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
