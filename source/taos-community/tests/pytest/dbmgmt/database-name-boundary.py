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
import datetime
import string
import random
import subprocess

from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):

        chars = string.ascii_uppercase + string.ascii_lowercase

        getDbNameLen = "grep -w '#define TSDB_DB_NAME_LEN' ../../src/inc/taosdef.h|awk '{print $3}'"
        dbNameMaxLen = int(subprocess.check_output(getDbNameLen, shell=True))
        tdLog.info("DB name max length is %d" % dbNameMaxLen)

        tdLog.info("=============== step1")
        db_name = ''.join(random.choices(chars, k=(dbNameMaxLen + 1)))
        tdLog.info('db_name length %d' % len(db_name))
        tdLog.info('create database %s' % db_name)
        tdSql.error('create database %s' % db_name)

        tdLog.info("=============== step2")
        db_name = ''.join(random.choices(chars, k=dbNameMaxLen))
        tdLog.info('db_name length %d' % len(db_name))
        tdLog.info('create database %s' % db_name)
        tdSql.execute('create database %s' % db_name)

        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, db_name.lower())

        tdLog.info("=============== step3")
        db_name = ''.join(random.choices(chars, k=(dbNameMaxLen - 1)))
        tdLog.info('db_name length %d' % len(db_name))
        tdLog.info('create database %s' % db_name)
        tdSql.execute('create database %s' % db_name)

        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, db_name.lower())

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
