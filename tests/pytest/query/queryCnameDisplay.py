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
import string
import random
import sys
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getLongName(self, len, mode = "mixed"):
        """
            generate long str
        """    
        chars = ''.join(random.choice(string.ascii_letters.lower()) for i in range(len))
        return chars
    
    def checkRegularTableCname(self):
        """
            check regular table cname
        """
        # len(colName) <=64, generate cname list and make first param = 63 and second param = 65
        cname_list = []
        for i in range(10):
            cname_list.append(self.getLongName(64))
        cname_list[0] = self.getLongName(63)
        cname_list[1] = self.getLongName(65)
        # create table and insert data
        tdSql.execute("CREATE TABLE regular_table_cname_check (ts timestamp, pi1 int, pi2 bigint, pf1 float, pf2 double, ps1 binary(10), pi3 smallint, pi4 tinyint, pb1 bool, ps2 nchar(20))")
        tdSql.execute('insert into regular_table_cname_check values (now, 1, 2, 1.1, 2.2, "a", 1, 1, true, "aa");')
        tdSql.execute('insert into regular_table_cname_check values (now, 2, 3, 1.2, 2.3, "b", 2, 1, false, "aa");')
        tdSql.execute('insert into regular_table_cname_check values (now, 3, 4, 1.3, 2.4, "c", 1, 3, true, "bb");')

        # select as cname with cname_list
        sql_seq = f'select count(ts) as {cname_list[0]}, sum(pi1) as {cname_list[1]}, avg(pi2) as {cname_list[2]}, count(pf1) as {cname_list[3]}, count(pf2) as {cname_list[4]}, count(ps1) as {cname_list[5]}, min(pi3) as {cname_list[6]}, max(pi4) as {cname_list[7]}, count(pb1) as {cname_list[8]}, count(ps2) as {cname_list[9]} from regular_table_cname_check'
        sql_seq_no_as = sql_seq.replace('as ', '')
        res = tdSql.getColNameList(sql_seq)
        res_no_as = tdSql.getColNameList(sql_seq_no_as)
        
        # cname[1] > 64, it is expected to be equal to 64
        cname_list_1_expected = cname_list[1][:-1]
        cname_list[1] = cname_list_1_expected
        checkColNameList = tdSql.checkColNameList(res, cname_list)
        checkColNameList = tdSql.checkColNameList(res_no_as, cname_list)

    def checkSuperTableCname(self):
        """
            check super table cname
        """
        # len(colName) <=64, generate cname list and make first param = 63 and second param = 65
        cname_list = []
        for i in range(19):
            cname_list.append(self.getLongName(64))
        cname_list[0] = self.getLongName(63)
        cname_list[1] = self.getLongName(65)

        # create table and insert data
        tdSql.execute("create table super_table_cname_check (ts timestamp, pi1 int, pi2 bigint, pf1 float, pf2 double, ps1 binary(10), pi3 smallint, pi4 tinyint, pb1 bool, ps2 nchar(20)) tags (si1 int, si2 bigint, sf1 float, sf2 double, ss1 binary(10), si3 smallint, si4 tinyint, sb1 bool, ss2 nchar(20));")
        tdSql.execute('create table st1 using super_table_cname_check tags (1, 2, 1.1, 2.2, "a", 1, 1, true, "aa");')
        tdSql.execute('insert into st1 values (now, 1, 2, 1.1, 2.2, "a", 1, 1, true, "aa");')
        tdSql.execute('insert into st1 values (now, 1, 1, 1.4, 2.3, "b", 3, 2, true, "aa");')
        tdSql.execute('insert into st1 values (now, 1, 2, 1.1, 2.2, "a", 1, 1, false, "bb");')

        # select as cname with cname_list
        sql_seq = f'select count(ts) as {cname_list[0]}, sum(pi1) as {cname_list[1]}, avg(pi2) as {cname_list[2]}, count(pf1) as {cname_list[3]}, count(pf2) as {cname_list[4]}, count(ps1) as {cname_list[5]}, min(pi3) as {cname_list[6]}, max(pi4) as {cname_list[7]}, count(pb1) as {cname_list[8]}, count(ps2) as {cname_list[9]}, count(si1) as {cname_list[10]}, count(si2) as {cname_list[11]}, count(sf1) as {cname_list[12]}, count(sf2) as {cname_list[13]}, count(ss1) as {cname_list[14]}, count(si3) as {cname_list[15]}, count(si4) as {cname_list[16]}, count(sb1) as {cname_list[17]}, count(ss2) as {cname_list[18]} from super_table_cname_check'
        sql_seq_no_as = sql_seq.replace('as ', '')
        res = tdSql.getColNameList(sql_seq)
        res_no_as = tdSql.getColNameList(sql_seq_no_as)

        # cname[1] > 64, it is expected to be equal to 64
        cname_list_1_expected = cname_list[1][:-1]
        cname_list[1] = cname_list_1_expected
        checkColNameList = tdSql.checkColNameList(res, cname_list)
        checkColNameList = tdSql.checkColNameList(res_no_as, cname_list)
        
    def run(self):
        tdSql.prepare()
        self.checkRegularTableCname()
        self.checkSuperTableCname()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

