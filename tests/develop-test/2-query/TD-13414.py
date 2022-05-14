###################################################################
#           Copyright (c) 2021 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def caseDescription(self):
        '''
        case1<ganlin zhao>: [TD-13414] taos shell coredump when scalar function arithmetic operation has bool operands
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db")
        tdSql.execute('use db')

        tdSql.execute('create stable stb(ts timestamp, c0 int) tags (t0 int)')
        tdSql.execute('create table ctb using stb tags (1)')
        tdSql.execute('create table tb (ts timestamp, c0 int)')

        tdSql.execute('insert into ctb values(now, 1)')
        tdSql.execute('insert into tb values(now, 1)')

        tdSql.error('select ceil(1.5) + true from stb')
        tdSql.error('select ceil(1.5) + true from ctb')
        tdSql.error('select ceil(1.5) + true from tb')

        tdSql.error('select round(c0) + false from stb')
        tdSql.error('select round(c0) + false from ctb')
        tdSql.error('select round(c0) + false from tb')

        tdSql.error('select cos(1) + true from stb')
        tdSql.error('select cos(1) + true from ctb')
        tdSql.error('select cos(1) + true from tb')

        tdSql.error('select true - ceil(1.5) from stb')
        tdSql.error('select true - ceil(1.5) from ctb')
        tdSql.error('select true - ceil(1.5) from tb')

        tdSql.error('select false - round(c0) from stb')
        tdSql.error('select false - round(c0) from ctb')
        tdSql.error('select false - round(c0) from tb')

        tdSql.error('select true - cos(1) from stb')
        tdSql.error('select true - cos(1) from ctb')
        tdSql.error('select true - cos(1) from tb')

        tdSql.error('select true + ceil(1.5) - false from stb')
        tdSql.error('select true + ceil(1.5) - false from ctb')
        tdSql.error('select true + ceil(1.5) - false from tb')

        tdSql.error('select false - round(c0) - false from stb')
        tdSql.error('select false - round(c0) - false from ctb')
        tdSql.error('select false - round(c0) - false from tb')

        tdSql.error('select true - cos(1) - false from stb')
        tdSql.error('select true - cos(1) - false from ctb')
        tdSql.error('select true - cos(1) - false from tb')

        tdSql.execute('drop database db')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
