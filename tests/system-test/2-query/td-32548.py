from util.cases import *
from util.sql import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)

        tdSql.execute("drop database if exists td_32548;")
        tdSql.execute("create database td_32548 cachemodel 'last_row' keep 3650,3650,3650;")

    def run(self):
        tdSql.execute("use td_32548;")

        tdSql.execute("create table ntb1 (ts timestamp, ival int);")
        tdSql.execute("insert into ntb1 values ('2024-07-08 17:54:49.675', 54);")

        tdSql.execute("flush database td_32548;")

        tdSql.execute("insert into ntb1 values ('2024-07-08 17:53:49.675', 53);")
        tdSql.execute("insert into ntb1 values ('2024-07-08 17:52:49.675', 52);")
        tdSql.execute("delete from ntb1 where ts = '2024-07-08 17:54:49.675';")

        tdSql.query('select last_row(ts) from ntb1;')
        tdSql.checkData(0, 0, '2024-07-08 17:53:49.675')

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
