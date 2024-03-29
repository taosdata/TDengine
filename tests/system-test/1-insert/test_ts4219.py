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

    def prepare_data(self):
        tdSql.execute("create database db;")
        tdSql.execute("use db;")
        tdSql.execute("create stable st(ts timestamp, c1 int, c2 float) tags(groupname binary(32));")

    def run(self):
        tdSql.error("insert into ct1 using st tags('group name 1') values(now, 1, 1.1)(now+1s, 2, 2.2)  ct1 using st tags('group 1) values(now+2s, 3, 3.3); ")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
