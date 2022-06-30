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
        case1<Ganlin Zhao>: [TD-12945] : taos shell crash when constant comparison cause crash
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db")
        tdSql.execute('use db')

        #Prepare data
        tdSql.execute("create table tb (ts timestamp, value int);")
        tdSql.execute("insert into tb values (now, 123);")

        ##operator: =
        tdSql.query('select 1 = 1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 = 0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 = 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 = 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 = 1.0001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 = 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 = 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 = 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 = 1.001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 = 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 = true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0 = false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 = false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0 = true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 = true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0.0 = false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 = false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0.0 = true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select true = 1.001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select true = 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.error('select "abc" = "def" from tb;')
        tdSql.error('select "abc" = 1 from tb;')
        tdSql.error('select 1 = "abc" from tb;')
        tdSql.error('select "abc" = 1.0 from tb;')
        tdSql.error('select 1.0 = "abc" from tb;')
        tdSql.error('select "abc" = true from tb;')
        tdSql.error('select false = "abc" from tb;')
        tdSql.error('select \'abc\' = \'def\' from tb;')
        tdSql.error('select \'abc\' = 1 from tb;')
        tdSql.error('select 1 = \'abc\' from tb;')
        tdSql.error('select \'abc\' = 1.0 from tb;')
        tdSql.error('select 1.0 = \'abc\' from tb;')
        tdSql.error('select \'abc\' = true from tb;')
        tdSql.error('select false = \'abc\' from tb;')


        ##operator: !=
        tdSql.query('select 1 != 1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 != 0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 != 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 != 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 != 1.0001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 != 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 != 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 != 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 != 1.001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 != 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 != true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0 != false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 != false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0 != true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 != true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0.0 != false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 != false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0.0 != true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true != 1.001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true != 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.error('select "abc" != "def" from tb;')
        tdSql.error('select "abc" != 1 from tb;')
        tdSql.error('select 1 != "abc" from tb;')
        tdSql.error('select "abc" != 1.0 from tb;')
        tdSql.error('select 1.0 != "abc" from tb;')
        tdSql.error('select "abc" != true from tb;')
        tdSql.error('select false != "abc" from tb;')
        tdSql.error('select \'abc\' != \'def\' from tb;')
        tdSql.error('select \'abc\' != 1 from tb;')
        tdSql.error('select 1 != \'abc\' from tb;')
        tdSql.error('select \'abc\' != 1.0 from tb;')
        tdSql.error('select 1.0 != \'abc\' from tb;')
        tdSql.error('select \'abc\' != true from tb;')
        tdSql.error('select false != \'abc\' from tb;')

        ##operator: <>
        tdSql.query('select 1 <> 1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 <> 0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 <> 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 <> 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 <> 1.0001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 <> 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 <> 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 <> 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 <> 1.001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 <> 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 <> true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0 <> false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 <> false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0 <> true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 <> true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0.0 <> false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 <> false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0.0 <> true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true <> 1.001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true <> 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.error('select "abc" <> "def" from tb;')
        tdSql.error('select "abc" <> 1 from tb;')
        tdSql.error('select 1 <> "abc" from tb;')
        tdSql.error('select "abc" <> 1.0 from tb;')
        tdSql.error('select 1.0 <> "abc" from tb;')
        tdSql.error('select "abc" <> true from tb;')
        tdSql.error('select false <> "abc" from tb;')
        tdSql.error('select \'abc\' <> \'def\' from tb;')
        tdSql.error('select \'abc\' <> 1 from tb;')
        tdSql.error('select 1 <> \'abc\' from tb;')
        tdSql.error('select \'abc\' <> 1.0 from tb;')
        tdSql.error('select 1.0 <> \'abc\' from tb;')
        tdSql.error('select \'abc\' <> true from tb;')
        tdSql.error('select false <> \'abc\' from tb;')

        ##operator: <
        tdSql.query('select 1 < 1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0 < 1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 < 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0.0 < 1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 < 1.0001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 < 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 < 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0.0 < 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 < 1.001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 < 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0 < true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 < false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select false < true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true < false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 < true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0.0 < false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select false < 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0.0 < true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true < 1.001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true < 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.error('select "abc" < "def" from tb;')
        tdSql.error('select "abc" < 1 from tb;')
        tdSql.error('select 1 < "abc" from tb;')
        tdSql.error('select "abc" < 1.0 from tb;')
        tdSql.error('select 1.0 < "abc" from tb;')
        tdSql.error('select "abc" < true from tb;')
        tdSql.error('select false < "abc" from tb;')
        tdSql.error('select \'abc\' < \'def\' from tb;')
        tdSql.error('select \'abc\' < 1 from tb;')
        tdSql.error('select 1 < \'abc\' from tb;')
        tdSql.error('select \'abc\' < 1.0 from tb;')
        tdSql.error('select 1.0 < \'abc\' from tb;')
        tdSql.error('select \'abc\' < true from tb;')
        tdSql.error('select false < \'abc\' from tb;')

        ##operator: >
        tdSql.query('select 1 > 1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 > 0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 > 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 > 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0001 > 1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 > 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 > 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 > 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.001 > 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0000000001 > 1.0 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 > false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0 > true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select false > true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select true > false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 > true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0.0 > false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 > false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true > 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.001 > true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0000000001 > true from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.error('select "abc" > "def" from tb;')
        tdSql.error('select "abc" > 1 from tb;')
        tdSql.error('select 1 > "abc" from tb;')
        tdSql.error('select "abc" > 1.0 from tb;')
        tdSql.error('select 1.0 > "abc" from tb;')
        tdSql.error('select "abc" > true from tb;')
        tdSql.error('select false > "abc" from tb;')
        tdSql.error('select \'abc\' > \'def\' from tb;')
        tdSql.error('select \'abc\' > 1 from tb;')
        tdSql.error('select 1 > \'abc\' from tb;')
        tdSql.error('select \'abc\' > 1.0 from tb;')
        tdSql.error('select 1.0 > \'abc\' from tb;')
        tdSql.error('select \'abc\' > true from tb;')
        tdSql.error('select false > \'abc\' from tb;')

        ##operator: <=
        tdSql.query('select 1 <= 2 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 <= 1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 <= 0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 <= 2.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 <= 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 <= 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 <= 1.0001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 <= 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 <= 2.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 <= 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 <= 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 <= 1.001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 <= 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0 <= true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 <= false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select false <= true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true <= false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select true <= true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select false <= false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 <= true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0.0 <= false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select false <= 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0.0 <= true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true <= 1.001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true <= 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.error('select "abc" <= "def" from tb;')
        tdSql.error('select "abc" <= 1 from tb;')
        tdSql.error('select 1 <= "abc" from tb;')
        tdSql.error('select "abc" <= 1.0 from tb;')
        tdSql.error('select 1.0 <= "abc" from tb;')
        tdSql.error('select "abc" <= true from tb;')
        tdSql.error('select false <= "abc" from tb;')
        tdSql.error('select \'abc\' <= \'def\' from tb;')
        tdSql.error('select \'abc\' <= 1 from tb;')
        tdSql.error('select 1 <= \'abc\' from tb;')
        tdSql.error('select \'abc\' <= 1.0 from tb;')
        tdSql.error('select 1.0 <= \'abc\' from tb;')
        tdSql.error('select \'abc\' <= true from tb;')
        tdSql.error('select false <= \'abc\' from tb;')

        ##operator: >=
        tdSql.query('select 1 >= 2 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 >= 1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 >= 0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 >= 2.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 >= 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 >= 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0001 >= 1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 >= 1.0000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 >= 2.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 >= 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 >= 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.001 >= 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0000000001 >= 1.0 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 >= false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0 >= true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select false >= true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select true >= false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select false >= false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true >= true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 >= true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0.0 >= false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 >= false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true >= 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.001 >= true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0000000001 >= true from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.error('select "abc" >= "def" from tb;')
        tdSql.error('select "abc" >= 1 from tb;')
        tdSql.error('select 1 >= "abc" from tb;')
        tdSql.error('select "abc" >= 1.0 from tb;')
        tdSql.error('select 1.0 >= "abc" from tb;')
        tdSql.error('select "abc" >= true from tb;')
        tdSql.error('select false >= "abc" from tb;')
        tdSql.error('select \'abc\' >= \'def\' from tb;')
        tdSql.error('select \'abc\' >= 1 from tb;')
        tdSql.error('select 1 >= \'abc\' from tb;')
        tdSql.error('select \'abc\' >= 1.0 from tb;')
        tdSql.error('select 1.0 >= \'abc\' from tb;')
        tdSql.error('select \'abc\' >= true from tb;')
        tdSql.error('select false >= \'abc\' from tb;')

        ##operator: between and
        tdSql.query('select 1 between 2 and 4 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 3 between 2 and 4 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 2 between 2 and 4 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 4 between 2 and 4 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 between 2.0 and 4.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1.0 between 2 and 4 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 3 between 2.0 and 4.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 3.0 between 2 and 4 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 2 between 2.0 and 4.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 4 between 2.0 and 4.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 2.0 between 2 and 4 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 4.0 between 2 and 4 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 2.0001 between 2 and 4 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 2.0000000001 between 2 and 4 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 2 between 2.0001 and 4 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 2 between 2.000000001 and 4 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)


        tdSql.query('select 4 between 2 and 4.0001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 4 between 2 and 4.000000001 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 4.0001 between 2 and 4 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 4.000000001 between 2 and 4 from tb;') ##DBL_EPSILON is used in floating number comparison
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select false between 0 and 1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select false between 1 and 2 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select true between 0 and 1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true between -1 and 0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select false between 0.0 and 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select false between 1.0 and 2.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select true between 0.0 and 1.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true between -1.0 and 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0 between false and true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 between false and true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0 between false and 10 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 between true and 10 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0.0 between false and true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1.0 between false and true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0 between false and 10.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 between true and 10.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.error('select "abc" between "def" and "ghi" from tb;')
        tdSql.error('select "abc" between 1 and 2 from tb;')
        tdSql.error('select "abc" between 1.0 and 2.0 from tb;')
        tdSql.error('select "abc" between true and false from tb;')
        tdSql.error('select 1 between 1.0 and "cde" from tb;')
        tdSql.error('select 1.0 between true and "cde" from tb;')
        tdSql.error('select true between 1 and "cde" from tb;')
        tdSql.error('select 1 between "abc" and 1.0 from tb;')
        tdSql.error('select 1.0 between "abc" and true from tb;')
        tdSql.error('select true between "abc" and 1 from tb;')

        tdSql.error('select \'abc\' between \'def\' and \'ghi\' from tb;')
        tdSql.error('select \'abc\' between 1 and 2 from tb;')
        tdSql.error('select \'abc\' between 1.0 and 2.0 from tb;')
        tdSql.error('select \'abc\' between true and false from tb;')
        tdSql.error('select 1 between 1.0 and \'cde\' from tb;')
        tdSql.error('select 1.0 between true and \'cde\' from tb;')
        tdSql.error('select true between 1 and \'cde\' from tb;')
        tdSql.error('select 1 between \'abc\' and 1.0 from tb;')
        tdSql.error('select 1.0 between \'abc\' and true from tb;')
        tdSql.error('select true between \'abc\' and 1 from tb;')

        ##operator: and
        tdSql.query('select 10 and 10 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 10.0 and 10 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 10.0 and 10.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 10 and 0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 10.0 and 0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 10.0 and 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 10.0 and 0.001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 10.0 and 0.000000000000000001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true and 10 and false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select true and 10.0 and false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 and 2 and 3 and 10.1 and -20.02 and 22.03 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 and 2 and 3 and 0 and 20.02 and 22.03 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 and 2 and 3 and 0.0 and 20.02 and 22.03 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.error('select "abc" and "def" from tb;')
        tdSql.error('select "abc" and 1 from tb;')
        tdSql.error('select 1 and "abc" from tb;')
        tdSql.error('select "abc" and 1.0 from tb;')
        tdSql.error('select 1.0 and abc from tb;')
        tdSql.error('select "abc" and true from tb;')
        tdSql.error('select false and "abc" from tb;')
        tdSql.error('select 1 and "abc" and 1.0 and true and false and 0 from tb;')
        tdSql.error('select 1 and "abc" and 1.0 and "cde" and false and 0 from tb;')
        tdSql.error('select 1 and "abc" and 1.0 and "cde" and false and "fhi" from tb;')

        tdSql.error('select \'abc\' and \'def\' from tb;')
        tdSql.error('select \'abc\' and 1 from tb;')
        tdSql.error('select 1 and \'abc\' from tb;')
        tdSql.error('select \'abc\' and 1.0 from tb;')
        tdSql.error('select 1.0 and abc from tb;')
        tdSql.error('select \'abc\' and true from tb;')
        tdSql.error('select false and \'abc\' from tb;')
        tdSql.error('select 1 and \'abc\' and 1.0 and true and false and 0 from tb;')
        tdSql.error('select 1 and \'abc\' and 1.0 and \'cde\' and false and "fhi" from tb;')


        ##operator: or
        tdSql.query('select 10 or 10 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 10.0 or 0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 10 or 0.0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0.0 or 0 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0.0 or 0.001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0.0 or 0.000000000000000001 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 or 2 or 3 or 0.0 or -20.02 or 22.03 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0 or 0.0 or 0.00 or 0.000 or 0.0000 or 0.00000 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select true or 10 or false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select true or 10.0 or false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.error('select "abc" or "def" from tb;')
        tdSql.error('select "abc" or 1 from tb;')
        tdSql.error('select 1 or "abc" from tb;')
        tdSql.error('select "abc" or 1.0 from tb;')
        tdSql.error('select 1.0 or abc from tb;')
        tdSql.error('select "abc" or true from tb;')
        tdSql.error('select false or "abc" from tb;')
        tdSql.error('select 1 or "abc" or 1.0 or true or false or 0 from tb;')
        tdSql.error('select 1 or "abc" or 1.0 or "cde" or false or 0 from tb;')
        tdSql.error('select 1 or "abc" or 1.0 or "cde" or false or "fhi" from tb;')

        tdSql.error('select \'abc\' or \'def\' from tb;')
        tdSql.error('select \'abc\' or 1 from tb;')
        tdSql.error('select 1 or \'abc\' from tb;')
        tdSql.error('select \'abc\' or 1.0 from tb;')
        tdSql.error('select 1.0 or abc from tb;')
        tdSql.error('select \'abc\' or true from tb;')
        tdSql.error('select false or \'abc\' from tb;')
        tdSql.error('select 1 or \'abc\' or 1.0 or true or false or 0 from tb;')
        tdSql.error('select 1 or \'abc\' or 1.0 or \'cde\' or false or "fhi" from tb;')

        ##operator: multiple operations
        tdSql.query('select 1 and 1 != 2 and 1 < 2 and 2 between 1 and 3 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0 or 1 = 2 or 1 >= 2 or 2 between 3 and 5 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0 and 1 != 2 and 1 < 2 and 2 between 1 and 3 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 or 1 = 2 or 1 >= 2 or 2 between 3 and 5 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 != 2 and 1 < 2 and 1 >= 2 and 2 between 1 and 3 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 = 2 or 1 >= 2 or 1<>3 or 2 between 3 and 5 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 0 or 1 != 2 and 1 <= 2 and 2 between 3 and 4 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 0 or 1 = 2 and 1 <= 2 and 2 between 3 and 5 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select 1 != 2 and 1 < 2 or 1 >= 2 or 2 between 4 and 5 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 != 2 and 1 < 2 or 1 >= 2 or 2 between 4 and 5 or true from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 != 2 and 1 < 2 or 1 >= 2 or 2 between 4 and 5 and false from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 != 2 and 1 < 2 or 1 >= 2 or 2 between 4 and 5 and true and 10.1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select 1 != 2 and 1 < 2 or 1 >= 2 or 2 between 4 and 5 and false or 10.1 from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.error('select 1 != 2 and "abc" or 1 >= 2 or "cde" between 4 and 5 and \'ghi\' or 10.1 from tb;')
        tdSql.error('select 1 != 2 and 1 < 2 or \'abc123\' or 2 between \'abc123\' and 5 and false or "abc123" from tb;')
        tdSql.error('select \'1234\' or 1 < 2 or \'aace\' and "cde" between 4 and "def" and "ckas" or 10.1 from tb;')

        #operator: is NULL
        tdSql.error('select 1 is NULL from tb;')
        tdSql.error('select 1.0 is NULL from tb;')
        tdSql.error('select true is NULL from tb;')
        tdSql.error('select \'a\' is NULL from tb;')
        tdSql.error('select "abc" is NULL from tb;')
        tdSql.error('select 1 is NULL and 1.0 is NULL or "abc" is NULL from tb;')

        #operator: is not NULL
        tdSql.error('select 1 is not NULL from tb;')
        tdSql.error('select 1.0 is not NULL from tb;')
        tdSql.error('select true is not NULL from tb;')
        tdSql.error('select \'a\' is not NULL from tb;')
        tdSql.error('select "abc" is not NULL from tb;')
        tdSql.error('select 1 is not NULL and 1.0 is not NULL or "abc" is not NULL from tb;')

        #operator: like
        tdSql.error('select 1 like 1 from tb;')
        tdSql.error('select 1.0 like 1.0 from tb;')
        tdSql.error('select true like true from tb;')
        tdSql.error('select \'abc\' like \'a_\' from tb;')
        tdSql.error('select "abc" like "ab__%" from tb;')
        tdSql.error('select 1 like 1 and 1.0 like 1.0 or "abc" like "a%" from tb;')

        #operator: match
        tdSql.error('select 1 match 1 from tb;')
        tdSql.error('select 1.0 match 1.0 from tb;')
        tdSql.error('select true match true from tb;')
        tdSql.error('select \'abc\' match \'a_\' from tb;')
        tdSql.error('select "abc" match "ab__%" from tb;')
        tdSql.error('select 1 match 1 and 1.0 match 1.0 or "abc" match "a%" from tb;')

        #operator: nmatch
        tdSql.error('select 1 nmatch 1 from tb;')
        tdSql.error('select 1.0 nmatch 1.0 from tb;')
        tdSql.error('select true nmatch true from tb;')
        tdSql.error('select \'abc\' nmatch \'a_\' from tb;')
        tdSql.error('select "abc" nmatch "ab__%" from tb;')
        tdSql.error('select 1 nmatch 1 and 1.0 nmatch 1.0 or "abc" nmatch "a%" from tb;')

        #operator: in
        tdSql.error('select 1 in 1 from tb;')
        tdSql.error('select 1 in (1, 2, 3) from tb;')
        tdSql.error('select 1.0 in 1.0 from tb;')
        tdSql.error('select 1.0 in (1.0, 2.0, 3.0) from tb;')
        tdSql.error('select true in (true, false, true) from tb;')
        tdSql.error('select \'abc\' in (\'acd\', \'bce\') from tb;')
        tdSql.error('select "abc" in ("acd", "bce") from tb;')
        tdSql.error('select 1 in (1,2,3) and 1.0 in (1.0,2.0,3.0) or "abc" in ("abc","cde") from tb;')

        tdSql.execute('drop database db')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
