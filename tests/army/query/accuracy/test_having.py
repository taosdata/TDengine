import taos
import socket
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.eos import *
from frame.server.dnodes import *


class TDTestCase(TBase):
    """Add test case to cover having key word
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), logSql)

    def prepare_data(self):
        # data for TD-32059
        tdSql.execute("create database td32059;")
        tdSql.execute("use td32059;")
        tdSql.execute("create stable stb (ts timestamp, id int, gid int) tags (t1 int);")
        tdSql.execute("insert into tb1 using stb (t1) tags (1) values ('2024-09-11 09:53:13.999', 6, 6)('2024-09-11 09:53:15.005', 6, 6)('2024-09-11 09:53:15.402', 6, 6);")
        tdSql.execute("insert into tb2 using stb (t1) tags(2) values ('2024-09-11 09:54:59.790', 9, 9)('2024-09-11 09:55:58.978', 11, 11)('2024-09-11 09:56:22.755', 12, 12)('2024-09-11 09:56:23.276', 12, 12)('2024-09-11 09:56:23.783', 12, 12)('2024-09-11 09:56:26.783', 12, 12)('2024-09-11 09:56:29.783', 12, 12);")

    def test_td32059(self):
        tdSql.execute("use td32059;")
        tdSql.query("SELECT _wstart, last_row(id) FROM stb WHERE ts BETWEEN '2024-09-11 09:50:13.999' AND '2024-09-11 09:59:13.999' INTERVAL(30s) FILL(PREV) HAVING(last_row(id) IS NOT NULL);")
        tdSql.checkRows(13)
        assert ('NULL' not in [item[1] for item in tdSql.res])

    def run(self):
        self.prepare_data()
        self.test_td32059()

    def stop(self):
        tdSql.execute("drop database td32059;")
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
