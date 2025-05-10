import random
import string

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        random.seed(1) # for reproducibility
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file
    

    def run(self):
        table_count = 100000 # 1000 is enough to reproduce the issue
        tag_length = 1026

        tdSql.prepare("db", True, vgroups=1)

        tdLog.info("create super table")
        tdSql.execute(f"create stable db.stb (ts timestamp, c1 int, c2 int) tags (tag1 varchar({tag_length}))")

        tdLog.info("create tables")
        for i in range(1, table_count):
            if i % 10000 == 0:
                tdLog.info(f"create table db.t{i}")
            tag = ''.join(random.choice(string.ascii_letters) for _ in range(tag_length))
            tdSql.execute(f"create table db.t{i} using db.stb tags ('{tag}')")

        tdLog.info("drop tables")
        for i in range(1, table_count):
            if i % 10000 == 0:
                tdLog.info(f"drop table db.t{i}")
            tdSql.execute(f"drop table db.t{i}")


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())