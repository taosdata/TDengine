
import taos

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def checksql(self, sql):
        result = os.popen(f"taos -s \"{sql}\" ")
        res = result.read()
        print(res)
        if ("Query OK" in res):
            tdLog.info(f"checkEqual success")
        else :
            tdLog.exit(f"checkEqual error")

    def td_28164(self):
        tdSql.execute("drop database if exists td_28164;")
        tdSql.execute("create database td_28164;")
        tdSql.execute("create table td_28164.test (ts timestamp, name varchar(10));")
        tdSql.execute("insert into td_28164.test values (now(), 'ac\\\\G') (now() + 1s, 'ac\\\\G') (now()+2s, 'ac\\G') (now()+3s, 'acG') (now()+4s, 'acK') ;")

        tdSql.query(f"select * from td_28164.test;")
        tdSql.checkRows(5)

        tdSql.query(f"select * from td_28164.test where name like 'ac\\\\\\G';")
        tdSql.checkRows(2)

        tdSql.query(f"select * from td_28164.test where name like 'ac\\\\G';")
        tdSql.checkRows(2)

        tdSql.query(f"select * from td_28164.test where name like 'ac\\G';")
        tdSql.checkRows(2)

        # tdSql.query(f"select * from td_28164.test where name like 'ac\\\g';")
        # tdSql.checkRows(0)
        #
        # tdSql.query(f"select * from td_28164.test where name like 'ac\\g';")
        # tdSql.checkRows(0)

        self.checksql(f'select * from td_28164.test where name like \'ac\\G\'\G;')
        # tdSql.checkRows(2)

        self.checksql(f"select * from td_28164.test where name like \'ac\\G\'   \G;")
        # tdSql.checkRows(2)

        tdSql.query(f"select * from td_28164.test where name like 'ac/\\G';")
        tdSql.checkRows(0)

        tdSql.query(f"select * from td_28164.test where name like 'ac/G';")
        tdSql.checkRows(0)

    def run(self):
        # tdSql.prepare()
        self.td_28164()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")



tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
