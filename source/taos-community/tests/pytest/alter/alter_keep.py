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
import time


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
    
    def alterKeepCommunity(self):
        tdLog.notice('running Keep Test, Community Version')
        tdLog.notice('running parameter test for keep during create')
        #testing keep parameter during create
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'3650')
        tdSql.execute('drop database db')

        tdSql.execute('create database db keep 100')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'100')
        tdSql.execute('drop database db')

        tdSql.error('create database db keep ')
        tdSql.error('create database db keep 0')
        tdSql.error('create database db keep 10,20')
        tdSql.error('create database db keep 10,20,30')
        tdSql.error('create database db keep 20,30,40,50')

        #testing keep parameter during alter
        tdSql.execute('create database db')
        tdLog.notice('running parameter test for keep during alter')

        tdSql.execute('alter database db keep 100')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'100')

        tdSql.error('alter database db keep ')
        tdSql.error('alter database db keep 0')
        tdSql.error('alter database db keep 10,20')
        tdSql.error('alter database db keep 10,20,30')
        tdSql.error('alter database db keep 20,30,40,50')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'100')

    def alterKeepEnterprise(self):
        tdLog.notice('running Keep Test, Enterprise Version')
        #testing keep parameter during create
        tdLog.notice('running parameter test for keep during create')

        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'3650,3650,3650')
        tdSql.execute('drop database db')

        tdSql.execute('create database db keep 100')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'100,100,100')
        tdSql.execute('drop database db')

        tdSql.execute('create database db keep 20, 30')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'20,30,30')
        tdSql.execute('drop database db')

        tdSql.execute('create database db keep 30,40,50')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'30,40,50')
        tdSql.execute('drop database db')

        tdSql.error('create database db keep ')
        tdSql.error('create database db keep 20,30,40,50')
        tdSql.error('create database db keep 0')
        tdSql.error('create database db keep 100,50')
        tdSql.error('create database db keep 100,40,50')
        tdSql.error('create database db keep 20,100,50')
        tdSql.error('create database db keep 50,60,20')

        #testing keep parameter during alter
        tdSql.execute('create database db')
        tdLog.notice('running parameter test for keep during alter')

        tdSql.execute('alter database db keep 10')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'10,10,10')

        tdSql.execute('alter database db keep 20,30')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'20,30,30')

        tdSql.execute('alter database db keep 100,200,300')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'100,200,300')

        tdSql.error('alter database db keep ')
        tdSql.error('alter database db keep 20,30,40,50')
        tdSql.error('alter database db keep 0')
        tdSql.error('alter database db keep 100,50')
        tdSql.error('alter database db keep 100,40,50')
        tdSql.error('alter database db keep 20,100,50')
        tdSql.error('alter database db keep 50,60,20')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'100,200,300')


    def run(self):
        tdSql.prepare()
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            tdLog.debug('running enterprise test')
            self.alterKeepEnterprise()
        else:
            tdLog.debug('running community test')
            self.alterKeepCommunity()

        tdSql.prepare()


        ## preset the keep
        tdSql.prepare()

        tdLog.notice('testing if alter will cause any error')
        tdSql.execute('create table tb (ts timestamp, speed int)')
        tdSql.execute('alter database db keep 10,10,10')
        tdSql.execute('insert into tb values (now, 10)')
        tdSql.execute('insert into tb values (now + 10m, 10)')
        tdSql.query('select * from tb')
        tdSql.checkRows(2)


        #after alter from small to large, check if the alter if functioning
        #test if change through test.py is consistent with change from taos client
        #test case for TD-4459 and TD-4445
        tdLog.notice('testing keep will be altered changing from small to big')
        tdSql.execute('alter database db keep 40,40,40')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'40,40,40')
        tdSql.error('insert into tb values (now-60d, 10)')
        tdSql.execute('insert into tb values (now-30d, 10)')
        tdSql.query('select * from tb')
        tdSql.checkRows(3)

        rowNum = 3
        for i in range(30):
            rowNum += 1
            tdSql.execute('alter database db keep 20,20,20')
            tdSql.execute('alter database db keep 40,40,40')
            tdSql.query('select * from information_schema.ins_databases')
            tdSql.checkData(0,7,'40,40,40')
            tdSql.error('insert into tb values (now-60d, 10)')
            tdSql.execute('insert into tb values (now-30d, 10)')
            tdSql.query('select * from tb')
            tdSql.checkRows(rowNum)

        tdLog.notice('testing keep will be altered changing from big to small')
        tdSql.execute('alter database db keep 10,10,10')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'10,10,10')
        tdSql.error('insert into tb values (now-15d, 10)')
        tdSql.query('select * from tb')
        tdSql.checkRows(2)
        
        rowNum = 2
        tdLog.notice('testing keep will be altered if sudden change from small to big')
        for i in range(30):
            tdSql.execute('alter database db keep 14,14,14')
            tdSql.execute('alter database db keep 16,16,16')
            tdSql.execute('insert into tb values (now-15d, 10)')
            tdSql.query('select * from tb')
            rowNum += 1
            tdSql.checkRows(rowNum)

        tdLog.notice('testing keep will be altered if sudden change from big to small')
        tdSql.execute('alter database db keep 16,16,16')
        tdSql.execute('alter database db keep 14,14,14')
        tdSql.error('insert into tb values (now-15d, 10)')
        tdSql.query('select * from tb')
        tdSql.checkRows(2)

        tdLog.notice('testing data will show up again when keep is being changed to large value')
        tdSql.execute('alter database db keep 40,40,40')
        tdSql.query('select * from tb')
        tdSql.checkRows(63)



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
