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
        # test case for https://jira.taosdata.com:18080/browse/TD-4584

        #1
        tdLog.info('=============== step1,create stable')
        tdLog.info('create table stb1 (ts timestamp, value double) tags (bin binary(128))') 
        tdSql.execute('create table stb1 (ts timestamp, value double) tags (bin binary(128))')

        tdLog.info('=============== step2,create table增加了转义字符')
        tdLog.info('create table tb1 using stb1 tags("abc\\"def")')
        #增加了转义字符\
        tdSql.execute('create table tb1 using stb1 tags("abc\\"def")')

        tdLog.info('=============== step3,insert data') 
        tdLog.info('insert into  tb1 values(now,1.0)')
        tdSql.execute('insert into  tb1 values(now,1.0)')

        tdLog.info('=============== step4,select table') 
        tdLog.info('select * from stb1 ')
        tdSql.query('select * from stb1 ')

        tdLog.info('=============== step5,check data') 
        tdSql.checkData(0,2,'abc"def')

        
  

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
