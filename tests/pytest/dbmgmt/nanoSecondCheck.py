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

#TODO: after TD-4518 and TD-4510 is resolved, add the exception test case for these situations

import sys
from util.log import *
from util.cases import *
from util.sql import *
import time
from datetime import datetime
import os


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdSql.execute('reset query cache')
        tdSql.execute('drop database if exists db')
        tdSql.execute('create database db precision "ns"')
        tdSql.query('show databases')
        tdSql.checkData(0,16,'ns')
        tdSql.execute('use db')
        tdSql.execute('create table tb (ts timestamp, speed int)')
        tdSql.execute('insert into tb values(\'2021-06-10 0:00:00.100000001\', 1)')
        tdSql.execute('insert into tb values(1623254400223456789, 1)')
        os.system('sudo timedatectl set-ntp off')
        os.system('sudo timedatectl set-time 2021-06-10')
        tdSql.execute('insert into tb values(now + 500000000b, 1)')

        ##TODO: after the connector is updated, run the following commented code
        # tdSql.query('select count(*) from tb where ts > 1623254400000000000 and ts < 1623254400150000000')
        # tdSql.checkData(0,0,1)
        # tdSql.query('select count(*) from tb where ts > \'2021-06-10 0:00:00.000000000\' and ts < \'2021-06-10 0:00:00.150000000\'')
        # tdSql.checkData(0,0,1)

        # tdSql.query('select count(*) from tb where ts > 1623254400223456788 and ts < 1623254400223456790')
        # tdSql.checkData(0,0,1)
        # tdSql.query('select count(*) from tb where ts > \'2021-06-10 00:00:00.223456788\' and ts < \'2021-06-10 00:00:00.223456790\'')
        # tdSql.checkData(0,0,1)

        # tdSql.query('select count(*) from tb where ts > 1623254400400000000')
        # tdSql.checkData(0,0,1)
        # tdSql.query('select count(*) from tb where ts > \'2021-06-10 00:00:00.400000000\'')
        # tdSql.checkData(0,0,1)

        # os.system('sudo timedatectl set-ntp off')
        # os.system('sudo timedatectl set-time 2021-06-10')
        # tdSql.query('select count(*) from tb where ts > now')
        # tdSql.checkData(0,0,3)
        # os.system('sudo timedatectl set-ntp off')
        # os.system('sudo timedatectl set-time 2021-06-10')
        # tdSql.query('select count(*) from tb where ts > now + 300000000b')
        # tdSql.checkData(0,0,1)

        # tdSql.query('select count(*) from tb where ts >= \'2021-06-10 0:00:00.100000001\'')
        # tdSql.checkData(0,0,3)

        # tdSql.query('select count(*) from tb where ts <= \'2021-06-10 0:00:00.223456789\'')
        # tdSql.checkData(0,0,2)

        # tdSql.query('select count(*) from tb where ts = \'2021-06-10 0:00:00.000000000\'')
        # tdSql.checkData(0,0,0)

        # tdSql.query('select count(*) from tb where ts <> \'2021-06-10 0:00:00.223456789\'')
        # tdSql.checkData(0,0,2)

        # tdSql.query('select count(*) from tb where ts between 1623254400000000000 and 1623254400950000000')
        # tdSql.checkData(0,0,2)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())