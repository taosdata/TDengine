# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *
class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        tdSql.execute('drop database if exists slmfvojuxt;')
        tdSql.execute('create database if not exists slmfvojuxt vgroups 1;')
        tdSql.execute('use slmfvojuxt;')
        tdSql.execute('create table if not exists downsampling_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
        tdSql.execute('create table ownsampling_ct1 using downsampling_stb tags(10, 10.1, "beijing", True);')
        tdSql.execute('create table if not exists scalar_stb (ts timestamp, c1 int, c2 double, c3 binary(20)) tags (t1 int);')
        tdSql.execute('create table scalar_ct1 using scalar_stb tags(10);')
        tdSql.execute('create stream downsampling_stream into output_downsampling_stb as select _wstart AS start, min(c1), max(c2), sum(c1) from downsampling_stb interval(10m);')
        tdSql.execute('create stream scalar_stream into output_scalar_stb as select ts, abs(c1) a1 , abs(c2) a2 from scalar_stb;')
        tdSql.execute('insert into scalar_ct1 values (1653471881952, 100, 100.1, "beijing");')
        tdSql.execute('insert into scalar_ct1 values (1653471881952+1s, -50, -50.1, "tianjin");')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
