# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *
from util.common import tdCom
class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        #for i in range(100):
            tdSql.prepare()
            dbname = tdCom.getLongName(10, "letters")
            tdSql.execute('create database if not exists djnhawvlgq vgroups 1')
            tdSql.execute('use djnhawvlgq')
            tdSql.execute('create table if not exists downsampling_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
            tdSql.execute('create table downsampling_ct1 using downsampling_stb tags(10, 10.1, "Beijing", True);')
            tdSql.execute('create table if not exists scalar_stb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 nchar(20), c5 nchar(20)) tags (t1 int);')
            tdSql.execute('create table scalar_ct1 using scalar_stb tags(10);')
            tdSql.execute('create table if not exists data_filter_stb (ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 float, c6 double, c7 binary(100), c8 nchar(200), c9 bool, c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned) tags (t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 float, t6 double, t7 binary(100), t8 nchar(200), t9 bool, t10 tinyint unsigned, t11 smallint unsigned, t12 int unsigned, t13 bigint unsigned)')
            tdSql.execute('create table if not exists data_filter_ct1 using data_filter_stb tags (1, 2, 3, 4, 5.5, 6.6, "binary7", "nchar8", true, 11, 12, 13, 14)')
            tdSql.execute('create stream data_filter_stream into output_data_filter_stb as select * from data_filter_stb             where ts >= 1653648072973+1s and c1 = 1 or c2 > 1 and c3 != 4 or c4 <= 3 and c5 <> 0 or c6 is not Null or c7 is Null or             c8 between "na" and "nchar4" and c8 not between "bi" and "binary" and c8 match "nchar[19]" and c8 nmatch "nchar[25]" or             c9 in (1, 2, 3) or c10 not in (6, 7) and c8 like "nch%" and c7 not like "bina_" and c11 <= 10 or c12 is Null or c13 >= 4;')
            tdSql.execute('insert into data_filter_ct1 values (1653648072973, 1, 1, 1, 3, 1.1, 1.1, "binary1", "nchar1", true, 1, 2, 3, 4);')
            tdSql.execute('insert into data_filter_ct1 values (1653648072973+1s, 2, 2, 1, 3, 1.1, 1.1, "binary2", "nchar2", true, 2, 3, 4, 5);')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
