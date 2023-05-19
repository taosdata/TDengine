import taos
import sys
from time import sleep
from util.log import *
from util.sql import *
from util.cases import *



class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def run(self):
        rows = 10

        tdSql.execute("create database dbus PRECISION 'us' ")
        tdSql.execute("create database dbns PRECISION 'ns' ")
        tdSql.execute("create database dbms PRECISION 'ms' ")
        dball = ["dbns","dbus","dbms"]
        for i in range(len(dball)):
            dbname = dball[i]
            stb = f"{dbname}.stb1"
            if  i == 0  :
                qts = 1678883666951061471
                qts1 = 1678883666951061471

                qtime = "2023-03-15 20:34:26.951061471"
            elif  i == 1 :
                qts = 1678883666951061
                qts1 = 1678883666951061

                qtime = "2023-03-15 20:34:26.951061"
            else:
                qts = 1678883666951
                qts1 = 1678883666953

                qtime = "2023-03-15 20:34:26.951"

            tdSql.execute(f"use {dbname}")
            tdLog.printNoPrefix("==========step1:create table")
            tdSql.execute(
                f'''create table if not exists {stb}
                (ts timestamp, c1 int, c2 float, c3 bigint, c4 double, c5 smallint, c6 tinyint)
                tags(location binary(64), type int, isused bool , family nchar(64))'''
            )
            tdSql.execute(f"create table {dbname}.t1 using {stb} tags('beijing', 1, 1, 'nchar1')")
            tdSql.execute(f"create table {dbname}.t2 using {stb} tags('shanghai', 2, 0, 'nchar2')")
            tdSql.execute(f"create table {dbname}.t3 using {stb} tags('shanghai', 3, 0, 'nchar3')")

            tdLog.printNoPrefix("==========step2:insert data")
            tdSql.execute(
                f"insert into {dbname}.t3 values ({qts}, null , {-3.4*10**38}, null , {-1.7*10**308}, null , null)"
            )
        
            tdLog.printNoPrefix("==========step3:query timestamp type")
            tdSql.query(f"select ts from {dbname}.t3 limit 1")
            tdSql.checkData(0,0,qtime)
            tdSql.checkData(0,0,qts)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
