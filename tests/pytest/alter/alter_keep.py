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
        tdSql.execute('create table tb (ts timestamp, speed int)')

        tdSql.query('show databases')
        tdSql.checkData(0,7,'3650,3650,3650')
        tdSql.execute('alter database db keep 10')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'10,10,10')
        tdSql.execute('alter database db keep 50')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'50,50,50')
        tdSql.error('alter database db keep !)') 

        tdSql.error('alter database db keep 1')

        ##  the following sql will not raise error, but will not cause error either
        #   based on Li Chuang's explaination, <= 0 will not cause keep>days error
        tdSql.execute('alter database db keep -10')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'50,50,50')
        tdSql.execute('alter database db keep 0')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'50,50,50')

        ##TODO: test keep keep hot alter, cannot be tested for now as test.py's output
        ##      is inconsistent with the actual output.
                    
        # tdSql.execute('insert into tb values (now, 10)')
        # tdSql.execute('insert into tb values (now + 10m, 10)')
        # tdSql.query('select * from tb')
        # tdSql.checkRows(2)
        # tdSql.execute('alter database db keep 40,40,40')
        # os.system('systemctl restart taosd')
        # tdSql.execute('insert into tb values (now-60d, 10)')
        # tdSql.execute('insert into tb values (now-30d, 10)')
        # tdSql.query('select * from tb')
        # tdSql.showQueryResult()
        # tdSql.checkRows(2)
        # tdSql.execute('alter database db keep 20,20,20')
        # tdSql.checkRows(3)
        # os.system('systemctl restart taosd')
        # tdSql.query('select * from tb')
        # tdSql.checkRows(2)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
