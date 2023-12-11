import sys 
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf


class TDTestCase:
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)

    #def prepare_data(self):
        

    def run(self):
        tdSql.execute("create database test_insert_timestamp;")
        tdSql.execute("use test_insert_timestamp;")
        tdSql.execute("create stable st(ts timestamp, c1 int) tags(id int);")
        tdSql.execute("create table test_t using st tags(1);")

        tdSql.error("insert into test_t values(now + today(), 1 ); ")
        tdSql.error("insert into test_t values(now - today(), 1 ); ")
        tdSql.error("insert into test_t values(today() + now(), 1 ); ")
        tdSql.error("insert into test_t values(today() - now(), 1 ); ")
        tdSql.error("insert into test_t values(2h - now(), 1 ); ")
        tdSql.error("insert into test_t values(2h - today(), 1 ); ")
        tdSql.error("insert into test_t values(2h - 1h, 1 ); ")
        tdSql.error("insert into test_t values(2h + 1h, 1 ); ")
        tdSql.error("insert into test_t values('2023-11-28 00:00:00.000' + '2023-11-28 00:00:00.000', 1 ); ")
        tdSql.error("insert into test_t values('2023-11-28 00:00:00.000' + 1701111600000, 1 ); ")
        tdSql.error("insert into test_t values(1701111500000 + 1701111600000, 1 ); ")
        tdSql.error("insert into test_insert_timestamp.test_t values(1701111600000 + 1h + 1s, 4); ")

        tdSql.execute("insert into test_insert_timestamp.test_t values(1701111600000 + 1h, 4); ")
        tdSql.execute("insert into test_insert_timestamp.test_t values(2h + 1701111600000, 5); ")
        tdSql.execute("insert into test_insert_timestamp.test_t values('2023-11-28 00:00:00.000' + 1h, 1); ")
        tdSql.execute("insert into test_insert_timestamp.test_t values(3h + '2023-11-28 00:00:00.000', 3); ")
        tdSql.execute("insert into test_insert_timestamp.test_t values(1701111600000 - 1h, 2); ")
        tdSql.execute("insert into test_insert_timestamp.test_t values(1701122400000, 6); ")
        tdSql.execute("insert into test_insert_timestamp.test_t values('2023-11-28 07:00:00.000', 7); ")

        tdSql.query(f'select ts, c1 from test_t order by ts;')
        tdSql.checkRows(7)
        tdSql.checkEqual(tdSql.queryResult[0][0], datetime.datetime(2023, 11, 28, 1, 0, 0) )
        tdSql.checkEqual(tdSql.queryResult[0][1], 1)
        tdSql.checkEqual(tdSql.queryResult[1][0], datetime.datetime(2023, 11, 28, 2, 0, 0) )
        tdSql.checkEqual(tdSql.queryResult[1][1], 2)
        tdSql.checkEqual(tdSql.queryResult[2][0], datetime.datetime(2023, 11, 28, 3, 0, 0) )
        tdSql.checkEqual(tdSql.queryResult[2][1], 3)
        tdSql.checkEqual(tdSql.queryResult[3][0], datetime.datetime(2023, 11, 28, 4, 0, 0) )
        tdSql.checkEqual(tdSql.queryResult[3][1], 4)
        tdSql.checkEqual(tdSql.queryResult[4][0], datetime.datetime(2023, 11, 28, 5, 0, 0) )
        tdSql.checkEqual(tdSql.queryResult[4][1], 5)
        tdSql.checkEqual(tdSql.queryResult[5][0], datetime.datetime(2023, 11, 28, 6, 0, 0) )
        tdSql.checkEqual(tdSql.queryResult[5][1], 6)
        tdSql.checkEqual(tdSql.queryResult[6][0], datetime.datetime(2023, 11, 28, 7, 0, 0) )
        tdSql.checkEqual(tdSql.queryResult[6][1], 7)
         
        tdSql.execute("drop table if exists test_t ;")
        tdSql.execute("drop stable if exists st;")
        tdSql.execute("drop database if exists test_insert_timestamp;")
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
