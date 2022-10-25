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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        #checking string input exception for alter
        tdSql.error("alter database db keep '10'")
        tdSql.error('alter database db keep "10"')
        tdSql.error("alter database db keep '\t'")
        tdSql.error("alter database db keep \'\t\'")
        tdSql.error('alter database db keep "a"')
        tdSql.error('alter database db keep "1.4"')
        tdSql.error("alter database db blocks '10'")
        tdSql.error('alter database db comp "0"')
        tdSql.execute('drop database if exists db')

        #checking string input exception for create
        tdSql.error("create database db comp '0'")
        tdSql.error('create database db comp "1"')
        tdSql.error("create database db comp '\t'")
        tdSql.error("alter database db keep \'\t\'")
        tdSql.error('create database db comp "a"')
        tdSql.error('create database db comp "1.4"')
        tdSql.error("create database db blocks '10'")
        tdSql.error('create database db keep "3650"')
        tdSql.error('create database db wal_fsync_period "3650"')
        tdSql.execute('create database db precision "us"')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,16,'us')
        tdSql.execute('drop database if exists db')

        #checking float input exception for create
        tdSql.error("create database db wal_fsync_period 7.3")
        tdSql.error("create database db wal_fsync_period 0.0")
        tdSql.error("create database db wal_fsync_period -5.32")
        tdSql.error('create database db comp 7.2')
        tdSql.error("create database db blocks 5.87")
        tdSql.error('create database db keep 15.4')

        #checking float input exception for insert
        tdSql.execute('create database db')
        tdSql.error('alter database db blocks 5.9')
        tdSql.error('alter database db blocks -4.7')
        tdSql.error('alter database db blocks 0.0')
        tdSql.error('alter database db keep 15.4')
        tdSql.error('alter database db comp 2.67')

        #checking additional exception param for alter keep
        tdSql.error('alter database db keep 365001')
        tdSql.error('alter database db keep 364999,365000,365001')
        tdSql.error('alter database db keep -10')
        tdSql.error('alter database db keep 5')
        tdSql.error('alter database db keep ')
        tdSql.error('alter database db keep 40,a,60')
        tdSql.error('alter database db keep ,,60,')
        tdSql.error('alter database db keep \t')
        tdSql.execute('alter database db keep \t50')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,7,'50,50,50')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())