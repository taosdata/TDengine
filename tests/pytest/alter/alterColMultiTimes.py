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

import random
import string
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def genColList(self):
        '''
            generate column list
        '''
        col_list = list()
        for i in range(1, 18):
            col_list.append(f'c{i}')
        return col_list

    def genIncreaseValue(self, input_value):
        '''
            add ', 1' to end of value every loop
        '''
        value_list = list(input_value)
        value_list.insert(-1, ", 1")
        return ''.join(value_list)

    def insertAlter(self):
        '''
            after each alter and insert, when execute 'select * from {tbname};' taosd will coredump
        '''
        tbname = ''.join(random.choice(string.ascii_letters.lower()) for i in range(7))
        input_value = '(now, 1)'
        tdSql.execute(f'create table {tbname} (ts timestamp, c0 int);')
        tdSql.execute(f'insert into  {tbname} values {input_value};')
        for col in self.genColList():
            input_value = self.genIncreaseValue(input_value)
            tdSql.execute(f'alter table {tbname} add column {col} int;')
            tdSql.execute(f'insert into {tbname} values {input_value};')
        tdSql.query(f'select * from {tbname};')
        tdSql.checkRows(18)
            
    def run(self):
        tdSql.prepare()
        self.insertAlter()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
