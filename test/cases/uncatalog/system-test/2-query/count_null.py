import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *



class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor())
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def check_results(self):
        tdSql.query(f"select count(*) from tb1")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c1) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c2) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c3) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c4) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c5) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c6) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c7) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c8) from tb1")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select count(*) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c1) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c2) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c3) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c4) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c5) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c6) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c7) from tb2")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c8) from tb2")
        tdSql.checkData(0, 0, 0)

        for i in range (3, 6):
            tdSql.query(f"select count(*) from tb{i}")
            tdSql.checkData(0, 0, 20000)
            tdSql.query(f"select count(c1) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c2) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c3) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c4) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c5) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c6) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c7) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c8) from tb{i}")
            tdSql.checkData(0, 0, 10000)


    def run(self):
        dbname = 'db'
        tbnames = ['tb1', 'tb2', 'tb3', 'tb4', 'tb5', 'tb6']
        num_rows = 20000
        num_tables = 6
        ts_base = 1685548800000

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")

        for i in range (num_tables):
            tdSql.execute(
                f'''create table if not exists {dbname}.{tbnames[i]}
                (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10))

                '''
            )


        tdLog.printNoPrefix("==========step2:insert data")

        for i in range(num_rows):
            tdSql.execute(f"insert into {dbname}.{tbnames[0]} values ({ts_base + i}, null, null, null, null, null, null, null, null, null)")

        for i in range(num_rows):
            tdSql.execute(f"insert into {dbname}.{tbnames[1]} values ({ts_base + i}, 1, 1, 1, 1, 1, 1, 1, null, null)")

        for i in range(num_rows):
            if i % 2 == 0:
                tdSql.execute(f"insert into {dbname}.{tbnames[2]} values ({ts_base + i}, null, null, null, null, null, null, null, null, null)")
            else:
                tdSql.execute(f"insert into {dbname}.{tbnames[2]} values ({ts_base + i}, 1, 1, 1, 1, 1, 1, 1, 'binary', 'nchar')")

        for i in range(num_rows):
            if i % 2 == 0:
                tdSql.execute(f"insert into {dbname}.{tbnames[3]} values ({ts_base + i}, null, null, null, null, null, null, null, 'binary', 'nchar')")
            else:
                tdSql.execute(f"insert into {dbname}.{tbnames[3]} values ({ts_base + i}, 1, 1, 1, 1, 1, 1, 1, null, null)")

        for i in range(num_rows):
            if i < num_rows / 2:
                tdSql.execute(f"insert into {dbname}.{tbnames[4]} values ({ts_base + i}, null, null, null, null, null, null, null, null, null)")
            else:
                tdSql.execute(f"insert into {dbname}.{tbnames[4]} values ({ts_base + i}, 1, 1, 1, 1, 1, 1, 1, 'binary', 'nchar')")

        for i in range(num_rows):
            if i >= num_rows / 2:
                tdSql.execute(f"insert into {dbname}.{tbnames[5]} values ({ts_base + i}, null, null, null, null, null, null, null, null, null)")
            else:
                tdSql.execute(f"insert into {dbname}.{tbnames[5]} values ({ts_base + i}, 1, 1, 1, 1, 1, 1, 1, 'binary', 'nchar')")


        tdLog.printNoPrefix("==========step3:check result in memory")
        self.check_results();

        tdLog.printNoPrefix("==========step3:check result from disk")
        tdSql.execute(f"flush database db")
        self.check_results();


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
