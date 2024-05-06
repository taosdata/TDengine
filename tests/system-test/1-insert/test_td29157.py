from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    """Verify td-29157
    """
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self.conn = conn
        self.db_name = "td29157"

    def run(self):
        # self.conn = taos.self.connect(host='192.168.1.51', user='root', password='taosdata')
        self.conn.execute(f"drop database if exists {self.db_name}")
        self.conn.execute(f"CREATE DATABASE {self.db_name}")
        self.conn.execute(f"USE {self.db_name}")

        tdSql.execute("create table stb3 (ts timestamp, c0 varbinary(10)) tags(t0 varbinary(10));")
        tdSql.execute("insert into ctb3 using stb3 tags(\"0x01\") values(now,NULL);")
        tdSql.query("show tags from ctb3;")
        
        tdSql.execute("create table stb7 (ts timestamp, c0 geometry(500)) tags(t0 geometry(100));")
        tdSql.execute("insert into ctb7 using stb7 tags('LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)') values(now,'POLYGON((1.0 1.0, 2.0 2.0, 1.0 1.0))');")
        tdSql.query("show tags from ctb7;")

    def stop(self):
        tdSql.execute("drop database if exists %s" % self.db_name)
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
