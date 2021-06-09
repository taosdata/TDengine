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
    
    def alterKeepCommunity(self):
        ## community accepts both 1 paramater, 2 parmaters and 3 paramaters
        ## but paramaters other than paramater 1 will be ignored 
        ## only paramater 1 will be used
        tdSql.query('show databases')
        tdSql.checkData(0,7,'3650,3650,3650')

        tdSql.execute('alter database db keep 10')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'10,10,10')

        tdSql.execute('alter database db keep 50')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'50,50,50')

        tdSql.execute('alter database db keep 20')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'20,20,20')

        tdSql.execute('alter database db keep 100, 98 ,99')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'100,100,100')

        tdSql.execute('alter database db keep 99, 100 ,101')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'99,99,99')

        tdSql.execute('alter database db keep 200, 199 ,198')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'200,200,200')

        tdSql.execute('alter database db keep 4000,4001')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'4000,4000,4000')

        tdSql.execute('alter database db keep 5000,50')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'5000,5000,5000')

        tdSql.execute('alter database db keep 50,5000')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'50,50,50')


    def alterKeepEnterprise(self):
        ## enterprise only accept three inputs
        ## does not accept 1 paramaters nor 3 paramaters
        tdSql.query('show databases')
        tdSql.checkData(0,7,'3650,3650,3650')

        tdSql.execute('alter database db keep 10')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'10,10,10')

        ## the order for altering keep is keep(D), keep0, keep1.
        ## if the order is changed, please modify the following test
        ## to make sure the the test is accurate

        tdSql.execute('alter database db keep 10, 10 ,10')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'10,10,10')

        tdSql.error('alter database db keep 100, 98 ,99')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'10,10,10')

        tdSql.execute('alter database db keep 200, 200 ,200')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'200,200,200')

        tdSql.execute('alter database db keep 198, 199 ,200')
        tdSql.query('show databases')
        tdSql.checkData(0,7,'198,199,200')

        # tdSql.execute('alter database db keep 3650,3650,3650')
        # tdSql.error('alter database db keep 4000,3640')
        # tdSql.error('alter database db keep 10,10')
        # tdSql.query('show databases')
        # tdSql.checkData(0,7,'3650,3650,3650')

    def run(self):
        tdSql.prepare()
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            tdLog.debug('running enterprise test')
            self.alterKeepEnterprise()
        else:
            tdLog.debug('running community test')
            self.alterKeepCommunity()
        

        ##TODO: need to wait for TD-4445 to implement the following
        ##      tests
        # tdSql.prepare()
        # tdSql.execute('create table tb (ts timestamp, speed int)')
        # tdSql.execute('alter database db keep 10,10,10')
        # tdSql.execute('insert into tb values (now, 10)')
        # tdSql.execute('insert into tb values (now + 10m, 10)')
        # tdSql.query('select * from tb')
        # tdSql.checkRows(2)
        # tdSql.execute('alter database db keep 40,40,40')
        # tdSql.query('show databases')
        # tdSql.checkData(0,7,'40,40,40')
        # tdSql.error('insert into tb values (now-60d, 10)')
        # tdSql.execute('insert into tb values (now-30d, 10)')
        # tdSql.query('select * from tb')
        # tdSql.checkRows(3)
        # tdSql.execute('alter database db keep 20,20,20')
        # tdSql.query('show databases')
        # tdSql.checkData(0,7,'20,20,20')
        # tdSql.query('select * from tb')
        # tdSql.checkRows(2)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
