###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.common import tdCom
import random

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def queryGroupTbname(self):
        '''
            select a1,a2...a10 from stb where tbname in (t1,t2,...t10) and ts...
        '''
        tdCom.cleanTb()
        table_name = tdCom.getLongName(8, "letters_mixed")
        tbname_list = list(map(lambda x: f'table_name_sub{x}', range(1, 11)))
        tb_str = ""

        for tbname in tbname_list:
            globals()[tbname] = tdCom.getLongName(8, "letters_mixed")
        tdSql.execute(f'CREATE TABLE {table_name} (ts timestamp, {table_name_sub1} tinyint, \
                     {table_name_sub2} smallint, {table_name_sub3} int, {table_name_sub4} bigint, \
                    {table_name_sub5} float, {table_name_sub6} double, {table_name_sub7} binary(20),\
                    {table_name_sub8} nchar(20), {table_name_sub9} bool) tags ({table_name_sub10} binary(20))')

        for tbname in tbname_list:
            tb_str += tbname
            tdSql.execute(f'create table {globals()[tbname]} using {table_name} tags ("{globals()[tbname]}")')

        for i in range(10):
            for tbname in tbname_list:
                tdSql.execute(f'insert into {globals()[tbname]} values (now, 1, 2, 3, 4, 1.1, 2.2, "{globals()[tbname]}", "{globals()[tbname]}", True)')
        
        for i in range(100):
            tdSql.query(f'select {table_name_sub1},{table_name_sub2},{table_name_sub3},{table_name_sub4},{table_name_sub5},{table_name_sub6},{table_name_sub7},{table_name_sub8},{table_name_sub9} from {table_name} where tbname in ("{table_name_sub1}","{table_name_sub2}","{table_name_sub3}","{table_name_sub4}","{table_name_sub5}","{table_name_sub6}","{table_name_sub7}","{table_name_sub8}","{table_name_sub9}") and ts >= "1980-01-01 00:00:00.000"')
            tdSql.checkRows(90)
        
        # TS-634
        tdLog.info("test case for bug TS-634")
        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute("create table meters (ts TIMESTAMP,voltage INT) TAGS (tableid INT)")
        tdSql.execute("CREATE TABLE t1 USING meters TAGS (1)")
        tdSql.execute("CREATE TABLE t2 USING meters TAGS (2)")

        ts = 1605381041000
        for i in range(10):                
            tdSql.execute("INSERT INTO t1 values(%d, %d)" % (ts + i, random.randint(0, 100)))
            tdSql.execute("INSERT INTO t2 values(%d, %d)" % (ts + i, random.randint(0, 100)))
        
        tdSql.query("select last_row(*), tbname from meters group by tbname order by ts desc")
        tdSql.checkRows(2)

        tdSql.execute("INSERT INTO t2 values(now, 2)")
        tdSql.query("select last_row(*), tbname from meters group by tbname order by ts desc")
        tdSql.checkRows(2)

    def run(self):
        tdSql.prepare()
        self.queryGroupTbname()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())