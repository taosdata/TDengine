from util.cases import *
from util.sql import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)

        tdSql.execute("drop database if exists ts_4338;")
        tdSql.execute("create database ts_4338;")
        tdSql.execute("drop table if exists ts_4338.t;")
        tdSql.execute("create database if not exists ts_4338;")
        tdSql.execute("create table ts_4338.t (ts timestamp, i8 tinyint);")
        tdSql.execute("insert into ts_4338.t (ts, i8) values (now(), 1) (now()+1s, 2);")

    def run(self):
        # TS-4348
        tdSql.query(f'select i8 from ts_4338.t;')
        tdSql.checkRows(2)

        tdSql.query(f'select i8 from ts_4338.t where 1 = 1;')
        tdSql.checkRows(2)

        tdSql.query(f'select i8 from ts_4338.t where i8 = 1;')
        tdSql.checkRows(1)

        tdSql.query(f'select * from (select * from ts_4338.t where i8 = 3);')
        tdSql.checkRows(0)

        # TD-27939
        tdSql.query(f'select * from (select * from ts_4338.t where 1 = 100);')
        tdSql.checkRows(0)

        tdSql.query(f'select * from (select * from (select * from ts_4338.t where 1 = 200));')
        tdSql.checkRows(0)

        tdSql.execute("drop database if exists ts_4338;")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")



tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
