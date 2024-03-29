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

import re
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def check_tags(self):
        tdSql.checkRows(2)
        tdSql.checkCols(6)
        tdSql.checkData(0, 0, 'ctb1')
        tdSql.checkData(0, 1, 'db')
        tdSql.checkData(0, 2, 'stb')
        tdSql.checkData(0, 3, 't0')
        tdSql.checkData(0, 4, 'INT')
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(1, 0, 'ctb1')
        tdSql.checkData(1, 1, 'db')
        tdSql.checkData(1, 2, 'stb')
        tdSql.checkData(1, 3, 't1')
        tdSql.checkData(1, 4, 'INT')
        tdSql.checkData(1, 5, 1)

    def check_table_tags(self, is_super_table):

        if is_super_table == False:
            tdSql.checkRows(1)
            tdSql.checkCols(3)
            tdSql.checkData(0, 0, 'ctb1')
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 1)
        else:
            tdSql.checkRows(2)
            tdSql.checkCols(3)
            tdSql.checkData(0, 0, 'ctb1')
            tdSql.checkData(1, 0, 'ctb2')
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 2)
            tdSql.checkData(0, 2, 1)
            tdSql.checkData(1, 2, 2)

    def check_indexes(self):
        tdSql.checkRows(2)
        for i in range(2):
            col_name = tdSql.getData(i, 5)
            if col_name == "t0":
                continue
            tdSql.checkCols(7)
            tdSql.checkData(i, 0, 'idx1')
            tdSql.checkData(i, 1, 'db')
            tdSql.checkData(i, 2, 'stb')
            tdSql.checkData(i, 3, None)
            tdSql.checkData(i, 5, 't1')
            tdSql.checkData(i, 6, 'tag_index')

    def run(self):
        tdSql.execute(f'create database db')
        tdSql.execute(f'use db')
        tdSql.execute(f'create table stb (ts timestamp, c0 int) tags (t0 int, t1 int)')
        tdSql.execute(f'create table ctb1 using stb tags (1, 1)')
        tdSql.execute(f'create table ctb2 using stb tags (2, 2)')
        tdSql.execute(f'insert into ctb1 values (now, 1)')
        tdSql.execute(f'insert into ctb2 values (now, 2)')

        # show tags
        tdSql.query(f'show tags from stb')
        tdSql.checkRows(0)
        tdSql.query(f'show tags from stb')
        tdSql.checkRows(0);
        tdSql.query(f'show tags from `stb`')
        tdSql.checkRows(0);
        tdSql.query(f'show tags from stb from db')
        tdSql.checkRows(0);
        tdSql.query(f'show tags from `stb` from `db`')
        tdSql.checkRows(0);
        tdSql.query(f'show tags from db.stb')
        tdSql.checkRows(0);
        tdSql.query(f'show tags from `db`.`stb`')
        tdSql.checkRows(0);
        tdSql.query(f'show tags from ctb1')
        self.check_tags();
        tdSql.query(f'show tags from `ctb1`')
        self.check_tags();
        tdSql.query(f'show tags from ctb1 from db')
        self.check_tags();
        tdSql.query(f'show tags from `ctb1` from `db`')
        self.check_tags();
        tdSql.query(f'show tags from db.ctb1')
        self.check_tags();
        tdSql.query(f'show tags from `db`.`ctb1`')
        self.check_tags();

        tdSql.error(f'show tags from db.stb from db')
        tdSql.error(f'show tags from `db`.`stb` from db')
        tdSql.error(f'show tags from db.ctb1 from db')
        tdSql.error(f'show tags from `db`.`ctb1` from db')

        # show table tags
        tdSql.query(f'show table tags from stb')
        self.check_table_tags(True);
        tdSql.query(f'show table tags from `stb`')
        self.check_table_tags(True);
        tdSql.query(f'show table tags from stb from db')
        self.check_table_tags(True);
        tdSql.query(f'show table tags from `stb` from `db`')
        self.check_table_tags(True);
        tdSql.query(f'show table tags from db.stb')
        self.check_table_tags(True);
        tdSql.query(f'show table tags from `db`.`stb`')
        self.check_table_tags(True);

        tdSql.query(f'show table tags from ctb1')
        self.check_table_tags(False);
        tdSql.query(f'show table tags from `ctb1`')
        self.check_table_tags(False);
        tdSql.query(f'show table tags from ctb1 from db')
        self.check_table_tags(False);
        tdSql.query(f'show table tags from `ctb1` from `db`')
        self.check_table_tags(False);
        tdSql.query(f'show table tags from db.ctb1')
        self.check_table_tags(False);
        tdSql.query(f'show table tags from `db`.`ctb1`')
        self.check_table_tags(False);

        tdSql.error(f'show table tags from db.stb from db')
        tdSql.error(f'show table tags from `db`.`stb` from db')
        tdSql.error(f'show table tags from db.ctb1 from db')
        tdSql.error(f'show table tags from `db`.`ctb1` from db')

        # show indexes
        tdSql.execute(f'create index idx1 on stb (t1)')

        tdSql.query(f'show indexes from stb')
        self.check_indexes();
        tdSql.query(f'show indexes from `stb`')
        self.check_indexes();
        tdSql.query(f'show indexes from stb from db')
        self.check_indexes();
        tdSql.query(f'show indexes from `stb` from `db`')
        self.check_indexes();
        tdSql.query(f'show indexes from db.stb')
        self.check_indexes();
        tdSql.query(f'show indexes from `db`.`stb`')
        self.check_indexes();

        tdSql.query(f'show indexes from ctb1')
        tdSql.checkRows(0)
        tdSql.query(f'show indexes from `ctb1`')
        tdSql.checkRows(0)
        tdSql.query(f'show indexes from ctb1 from db')
        tdSql.checkRows(0)
        tdSql.query(f'show indexes from `ctb1` from `db`')
        tdSql.checkRows(0)
        tdSql.query(f'show indexes from db.ctb1')
        tdSql.checkRows(0)
        tdSql.query(f'show indexes from `db`.`ctb1`')
        tdSql.checkRows(0)

        tdSql.error(f'show indexes from db.stb from db')
        tdSql.error(f'show indexes from `db`.`stb` from db')
        tdSql.error(f'show indexes from db.ctb1 from db')
        tdSql.error(f'show indexes from `db`.`ctb1` from db')

        # check error information
        tdSql.error(f'create index idx1 on db2.stb (t1);', expectErrInfo='Database not exist')
        tdSql.error(f'use db2;', expectErrInfo='Database not exist')
        tdSql.error(f' alter stable db2.stb add column c2 int;', expectErrInfo='Database not exist')

        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
