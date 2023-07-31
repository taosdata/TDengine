from random import randint, random
from numpy import equal
import taos
import sys
import datetime
import inspect

from util.log import *
from util.sql import *
from util.cases import *

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def prepare_datas(self, dbname="db"):
        tdSql.execute(
            f'''create table {dbname}.stb
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t1 int)
            '''
        )

        tdSql.execute(
            f'''
            create table {dbname}.tb
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(2):
            tdSql.execute(f'create table {dbname}.ctb{i+1} using {dbname}.stb tags ( {i+1} )')

        tdSql.execute(f"insert into {dbname}.ctb1 values ('2020-12-11 00:00:00.000', 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(f"insert into {dbname}.ctb1 values ('2020-12-11 00:00:01.000', 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ctb1 values ('2020-12-11 00:00:02.000', 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ctb1 values ('2020-12-11 00:00:03.000', 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(f"insert into {dbname}.ctb2 values ('2020-12-12 00:00:00.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ctb2 values ('2020-12-12 00:00:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ctb2 values ('2020-12-12 00:00:02.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")

        tdSql.execute(
            f'''insert into {dbname}.tb values
            ( '2020-04-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2020-10-21 01:01:01.000', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now()+1a )
            ( '2020-12-31 01:01:01.000', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now()+2a )
            ( '2021-01-01 01:01:06.000', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now()+3a )
            ( '2021-05-07 01:01:10.000', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now()+4a )
            ( '2021-07-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2021-09-30 01:01:16.000', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now()+5a )
            ( '2022-02-01 01:01:20.000', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now()+6a )
            ( '2022-10-28 01:01:26.000', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )
            ( '2022-12-01 01:01:30.000', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )
            ( '2022-12-31 01:01:32.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )
            ( '2022-12-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )
            ( '2023-02-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            '''
        )

    def check_mode_function(self, dbname="db"):

        # normal table
        tdSql.query(f"select ts,mode(c1) from {dbname}.tb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2022, 12, 31, 1, 1, 36).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, 9)

        tdSql.query(f"select ts,mode(c2) from {dbname}.tb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2022, 12, 31, 1, 1, 36).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, -99999999999999999)

        tdSql.query(f"select ts,mode(c3) from {dbname}.tb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2022, 12, 31, 1, 1, 36).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, -999)

        tdSql.query(f"select ts,mode(c4) from {dbname}.tb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2022, 12, 31, 1, 1, 36).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, -99)

        tdSql.query(f"select ts,mode(c5) from {dbname}.tb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2022, 12, 31, 1, 1, 36).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, -9.99)

        tdSql.query(f"select ts,mode(c6) from {dbname}.tb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2022, 12, 31, 1, 1, 36).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, -1e+21)

        tdSql.query(f"select ts,mode(c7) from {dbname}.tb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2022, 12, 31, 1, 1, 36).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, True)

        tdSql.query(f"select ts,mode(c8) from {dbname}.tb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2022, 12, 31, 1, 1, 36).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, 'binary9')

        tdSql.query(f"select ts,mode(c9) from {dbname}.tb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2022, 12, 31, 1, 1, 36).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, 'nchar9')

        tdSql.query(f"select ts,c3,c5,c8,mode(c1) from {dbname}.tb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2022, 12, 31, 1, 1, 36).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, -999)
        tdSql.checkData(0, 2, -9.99)
        tdSql.checkData(0, 3, 'binary9')
        tdSql.checkData(0, 4, 9)

        # super table
        tdSql.query(f"select ts,mode(c1) from {dbname}.stb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2020, 12, 11, 0, 0, 3).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, 9)

        tdSql.query(f"select ts,mode(c2) from {dbname}.stb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2020, 12, 11, 0, 0, 3).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, -99999)

        tdSql.query(f"select ts,mode(c3) from {dbname}.stb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2020, 12, 11, 0, 0, 3).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, -999)

        tdSql.query(f"select ts,mode(c4) from {dbname}.stb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2020, 12, 11, 0, 0, 2).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, -99)

        tdSql.query(f"select ts,mode(c5) from {dbname}.stb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2020, 12, 11, 0, 0, 3).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, -9.99)

        tdSql.query(f"select ts,mode(c6) from {dbname}.stb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2020, 12, 11, 0, 0, 3).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, -99.99)

        tdSql.query(f"select ts,mode(c7) from {dbname}.stb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2020, 12, 11, 0, 0, 3).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, True)

        tdSql.query(f"select ts,mode(c8) from {dbname}.stb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2020, 12, 11, 0, 0, 3).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, 'binary9')

        tdSql.query(f"select ts,mode(c9) from {dbname}.stb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2020, 12, 11, 0, 0, 3).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, 'nchar9')

        tdSql.query(f"select ts,mode(c1),c3,c5,c8 from {dbname}.stb")
        tdSql.checkRows(1)
        ts = tdSql.getData(0, 0)
        tdSql.checkEqual(ts.astimezone(datetime.timezone.utc), datetime.datetime(2020, 12, 11, 0, 0, 3).astimezone(datetime.timezone.utc))
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(0, 2, -999)
        tdSql.checkData(0, 3, -9.99)
        tdSql.checkData(0, 4, 'binary9')

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas()

        tdLog.printNoPrefix("==========step2:test results  ==============")

        self.check_mode_function()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
