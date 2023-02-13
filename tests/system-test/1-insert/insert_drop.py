# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *
import threading


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def genMultiThreadSeq(self, sql_list):
        tlist = list()
        for insert_sql in sql_list:
            t = threading.Thread(target=tdSql.execute, args=(insert_sql,))
            tlist.append(t)
        return tlist

    def multiThreadRun(self, tlist):
        for t in tlist:
            t.start()
        for t in tlist:
            t.join()

    def run(self):
        tdSql.prepare()
        tdSql.execute('create database if not exists test;')
        tdSql.execute('create table test.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int );')
        tdSql.execute('create table test.tb using test.stb TAGS (1, 1);')

        # double comma insert check error
        tdSql.error("insert into test.tb(ts, c11) values(now,,100)")

        sql_list = list()
        for i in range(5):
            sql = f'insert into test.tb values (now-{i}m, {i}, {i});'
            sql_list.append(sql)
        sql_list.append(f'drop database test;')
        tlist = self.genMultiThreadSeq(sql_list)
        self.multiThreadRun(tlist)
        tdSql.query(f'select * from information_schema.ins_databases')


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
