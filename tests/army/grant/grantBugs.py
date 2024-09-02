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

import frame.etool

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *


class TDTestCase(TBase):
    
    def td_30642(self):
        sqls = [
            "CREATE DATABASE IF NOT EXISTS `_xTest2`",
            "CREATE USER `_xTest` PASS 'taosdata'",
            "CREATE TABLE IF NOT EXISTS `_xTest2`.`meters` (ts timestamp, v1 int) tags(t1 int)",

            "CREATE DATABASE IF NOT EXISTS `test2`",
            "CREATE USER `user1` PASS 'taosdata'",
            "CREATE TABLE IF NOT EXISTS `test2`.`meters2` (ts timestamp, v1 int) tags(t1 int)"
        ]
        tdSql.executes(sqls)

        sql1 = 'GRANT read ON `_xTest2`.`meters` WITH (t1 = 1) TO `_xTest`'
        tdSql.query(sql1)
        sql1_verify = "select * from information_schema.ins_user_privileges where user_name='_xTest' and privilege='read' and db_name='_xTest2' and table_name='meters'"
        tdSql.query(sql1_verify)
        tdSql.checkRows(1)
        tdSql.checkData(0, 4, '(`_xTest2`.`meters`.`t1` = 1)')

        sql2 = 'GRANT write ON test2.meters2 WITH (t1 = 1) TO user1'
        tdSql.query(sql2)
        sql2_verify = "select * from information_schema.ins_user_privileges where user_name='user1' and privilege='write' and db_name='test2' and table_name='meters2'"
        tdSql.query(sql2_verify)
        tdSql.checkRows(1)
        tdSql.checkData(0, 4, '(`test2`.`meters2`.`t1` = 1)')

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # TD-30642
        self.td_30642()


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
