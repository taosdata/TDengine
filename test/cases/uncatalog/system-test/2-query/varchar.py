import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *



class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        dbname = "db"
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 varchar(16),c9 nchar(32), c10 timestamp)
            tags (t1 int)
            '''
        )
        tdSql.execute(
            f'''
            create table {dbname}.t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 varchar(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')

        tdLog.printNoPrefix("==========step2:insert data")
        for i in range(9):
            tdSql.execute(

                f"insert into {dbname}.ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'varchar{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'varchar{i}', 'nchar{i}', now()+{1*i}a )"
            )

        tdSql.execute(f"insert into {dbname}.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'varchar0', 'nchar0', now()+8a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'varchar9', 'nchar9', now()+9a )")

        tdSql.execute(f"insert into {dbname}.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdSql.execute(
            f'''insert into {dbname}.t1 values
            ( '2020-04-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2020-10-21 01:01:01.000', 1, 11111, 111, 11, 1.11, 11.11, 1, "varchar1", "nchar1", now()+1a )
            ( '2020-12-31 01:01:01.000', 2, 22222, 222, 22, 2.22, 22.22, 0, "varchar2", "nchar2", now()+2a )
            ( '2021-01-01 01:01:06.000', 3, 33333, 333, 33, 3.33, 33.33, 0, "varchar3", "nchar3", now()+3a )
            ( '2021-05-07 01:01:10.000', 4, 44444, 444, 44, 4.44, 44.44, 1, "varchar4", "nchar4", now()+4a )
            ( '2021-07-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2021-09-30 01:01:16.000', 5, 55555, 555, 55, 5.55, 55.55, 0, "varchar5", "nchar5", now()+5a )
            ( '2022-02-01 01:01:20.000', 6, 66666, 666, 66, 6.66, 66.66, 1, "varchar6", "nchar6", now()+6a )
            ( '2022-10-28 01:01:26.000', 7, 00000, 000, 00, 0.00, 00.00, 1, "varchar7", "nchar7", "1970-01-01 08:00:00.000" )
            ( '2022-12-01 01:01:30.000', 8, -88888, -888, -88, -8.88, -88.88, 0, "varchar8", "nchar8", "1969-01-01 01:00:00.000" )
            ( '2022-12-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "varchar9 ", "nchar9", "1900-01-01 00:00:00.000" )
            ( '2023-02-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            '''
        )

        data_ct1_c8 = ["varchar8","varchar7","varchar6","varchar5","varchar0","varchar4","varchar3","varchar2","varchar1","varchar0","varchar9"]

        tdLog.printNoPrefix("==========step3: cast on varchar")

        tdSql.query(f"select c8 from {dbname}.ct1")
        for i in range(tdSql.queryRows):
            tdSql.checkData(i,0, data_ct1_c8[i])

        tdSql.error("create stable empty_col_stable(ts timestamp, c2 varchar(0)) tags(tg1 int)")
        tdSql.error("create stable empty_col_stable(ts timestamp, c2 varchar(10)) tags(tg1 varchar(0))")
        tdSql.error("create stable empty_col_stable(ts timestamp, c2 varchar(10)) tags(tg1 nchar(0))")
        tdSql.error("create stable empty_col_stable(ts timestamp, c2 varchar(10)) tags(tg1 binary(0))")
        tdSql.error("create stable empty_col_stable(ts timestamp, c2 varchar(10)) tags(tg1 varbinary(0))")


        # tdSql.query("select c8 from ct4")
        # data_ct4 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
        # tdSql.query("select c8 from t1")
        # data_t1 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]

        # tdLog.printNoPrefix("==========step4: cast on varchar")

        # tdSql.query("select cast(c8 as nchar(16)) as b from ct4")
        # for i in range(len(data_ct4)):
        #     tdSql.checkData( i, 0, data_ct4[i])
        # tdSql.query("select cast(c8 as nchar(16)) as b from t1")
        # for i in range(len(data_t1)):
        #     tdSql.checkData( i, 0, data_t1[i])

        # tdSql.query("select cast(c8 as nchar(2)) as b from t1")
        # for i in range(len(data_t1)):
        #     tdSql.checkData( i, 0, data_t1[i][:2])


        # tdSql.error("select cast(c1 as int) as b from ct4")
        # tdSql.error("select cast(c1 as bool) as b from ct4")
        # tdSql.error("select cast(c1 as tinyint) as b from ct4")
        # tdSql.error("select cast(c1 as smallint) as b from ct4")
        # tdSql.error("select cast(c1 as float) as b from ct4")
        # tdSql.error("select cast(c1 as double) as b from ct4")
        # tdSql.error("select cast(c1 as tinyint unsigned) as b from ct4")
        # tdSql.error("select cast(c1 as smallint unsigned) as b from ct4")
        # tdSql.error("select cast(c1 as int unsigned) as b from ct4")

        # tdSql.error("select cast(c2 as int) as b from ct4")
        # tdSql.error("select cast(c3 as bool) as b from ct4")
        # tdSql.error("select cast(c4 as tinyint) as b from ct4")
        # tdSql.error("select cast(c5 as smallint) as b from ct4")
        # tdSql.error("select cast(c6 as float) as b from ct4")
        # tdSql.error("select cast(c7 as double) as b from ct4")
        # tdSql.error("select cast(c8 as tinyint unsigned) as b from ct4")

        # tdSql.error("select cast(c8 as timestamp ) as b from ct4")

        # tdSql.error("select cast(c9 as binary(64) ) as b from ct4")
        # tdSql.error("select cast(c9 as timestamp ) as b from ct4")
        # tdSql.error("select cast(c10 as binary(64) ) as b from ct4")
        # tdSql.error("select cast(c10 as nchar(64) ) as b from ct4")


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
