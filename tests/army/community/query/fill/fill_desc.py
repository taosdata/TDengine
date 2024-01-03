import taos
import sys

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *

class TDTestCase(TBase):

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor())
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def run(self):
        dbname = "db"
        stbname = "ocloud_point"
        tbname = "ocloud_point_170658_3837620225_1701134595725266945"

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")

        tdSql.execute(
            f'''create stable if not exists {dbname}.{stbname}
            (wstart timestamp, point_value float) tags (location binary(64), groupId int)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname} using {dbname}.{stbname} tags("California.SanFrancisco", 2)'''
        )

        tdSql.execute(f"insert into {dbname}.{tbname} values('2023-12-26 10:35:00.000', 5.0)")
        tdSql.execute(f"insert into {dbname}.{tbname} values('2023-12-26 10:36:00.000', 5.0)")
        tdSql.execute(f"insert into {dbname}.{tbname} values('2023-12-26 10:37:00.000', 5.0)")
        tdSql.execute(f"insert into {dbname}.{tbname} values('2023-12-26 10:38:00.000', null)")
        tdSql.execute(f"insert into {dbname}.{tbname} values('2023-12-26 10:39:00.000', 5.0)")
        tdSql.execute(f"insert into {dbname}.{tbname} values('2023-12-26 10:40:00.000', null)")

        tdLog.printNoPrefix("==========step3:fill data")

        tdSql.query(f"wstart as ts, first(point_value) as pointValu from {dbname}.{tbname} where wstart between '2823-12-2510:35:00' and '2023-12-2510:40:00' fill(prev) order by wstart desc limit 100")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 5)
        tdSql.checkData(2, 1, 5)
        tdSql.checkData(3, 1, 5)
        tdSql.checkData(4, 1, 5)
        tdSql.checkData(5, 1, 5)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
