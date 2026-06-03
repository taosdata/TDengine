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

from datetime import timedelta

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        ret = tdSql.query('select database()')
        tdSql.checkData(0, 0, "db")        
        
        ret = tdSql.query('select server_status()')
        tdSql.checkData(0, 0, 1)

        ret = tdSql.query('select server_status() as result')
        tdSql.checkData(0, 0, 1)

        time.sleep(1)

        ret = tdSql.query('select * from information_schema.ins_dnodes')

        dnodeId = tdSql.getData(0, 0);
        dnodeEndpoint = tdSql.getData(0, 1);

        ret = tdSql.execute('alter dnode "%s" debugFlag 135' % dnodeId)
        tdLog.info('alter dnode "%s" debugFlag 135 -> ret: %d' % (dnodeId, ret))

        time.sleep(1)

        ret = tdSql.query('select * from information_schema.ins_mnodes')
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, "master")

        role_time = tdSql.getData(0, 3)
        create_time = tdSql.getData(0, 4)
        time_delta = timedelta(milliseconds=100)

        if create_time-time_delta < role_time < create_time+time_delta:
            tdLog.info("role_time {} and create_time {} expected within range".format(role_time, create_time))
        else:
            tdLog.exit("role_time {} and create_time {} not expected within range".format(role_time, create_time))    

        ret = tdSql.query('show vgroups')
        tdSql.checkRows(0)        

        tdSql.execute('create stable st (ts timestamp, f int) tags(t int)')
        tdSql.execute('create table ct1 using st tags(1)');
        tdSql.execute('create table ct2 using st tags(2)');

        time.sleep(3)

        ret = tdSql.query('show vnodes "{}"'.format(dnodeEndpoint))
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, "master")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
