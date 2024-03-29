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

import re
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()

    def create_db(self):
        hours = 8
        # create
        keep_str = f"KEEP_TIME_OFFSET {hours}"
        tdSql.execute(f"create database db {keep_str}")

        # check result
        tdSql.query("select `keep_time_offset` from  information_schema.ins_databases where name='db'")
        tdSql.checkData(0, 0, hours)

        # alter
        hours = 4
        keep_str = f"KEEP_TIME_OFFSET {hours}"
        tdSql.execute(f"alter database db {keep_str}")

        # check result
        tdSql.query("select `keep_time_offset` from  information_schema.ins_databases where name='db'")
        tdSql.checkData(0, 0, hours)


    def check_old_syntax(self):
        # old syntax would not support again
        tdSql.error("alter dnode 1 'keeptimeoffset 10';")


    def run(self):
        # check new syntax right
        self.create_db()

        # check old syntax error
        self.check_old_syntax()
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
