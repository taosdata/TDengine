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

from new_test_framework.utils import tdLog, tdSql, epath, sc



class TestGrantBugs:
    
    def td_30642(self):
        tdLog.debug("===== step1")

        sqls = [
            "CREATE DATABASE IF NOT EXISTS `_xTest2` vgroups 1",
            "CREATE USER `_xTest` PASS 'taosdata'",
            "CREATE TABLE IF NOT EXISTS `_xTest2`.`meters` (ts timestamp, v1 int) tags(t1 int)",

            "CREATE DATABASE IF NOT EXISTS `test2` vgroups 1",
            "CREATE USER `user1` PASS 'taosdata'",
            "CREATE TABLE IF NOT EXISTS `test2`.`meters2` (ts timestamp, v1 int) tags(t1 int)",

            "CREATE USER `read_user` PASS 'taosdata'"
        ]
        tdSql.executes(sqls)

        tdLog.debug("===== step2")
        sql1 = 'GRANT read ON `_xTest2`.`meters` WITH (t1 = 1) TO `_xTest`'
        tdSql.query(sql1)
        sql1_verify = "select * from information_schema.ins_user_privileges where user_name='_xTest' and privilege='read' and db_name='_xTest2' and table_name='meters'"
        tdSql.query(sql1_verify)
        tdSql.checkRows(1)
        tdSql.checkData(0, 4, '(`_xTest2`.`meters`.`t1` = 1)')

        tdLog.debug("===== step3")
        sql2 = 'GRANT write ON test2.meters2 WITH (t1 = 1) TO user1'
        tdSql.query(sql2)
        sql2_verify = "select * from information_schema.ins_user_privileges where user_name='user1' and privilege='write' and db_name='test2' and table_name='meters2'"
        tdSql.query(sql2_verify)
        tdSql.checkRows(1)
        tdSql.checkData(0, 4, '(`test2`.`meters2`.`t1` = 1)')

        # TS-6276
        tdLog.debug("===== step4")
        sql3 = "GRANT read ON `_xTest2`.`meters` WITH 't1 in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)' TO `read_user`";
        tdSql.query(sql3)
        sql3_verify = "select * from information_schema.ins_user_privileges where user_name='read_user' and privilege='read' and db_name='_xTest2' and table_name='meters'"
        tdSql.query(sql3_verify)
        tdSql.checkRows(1)
        tdSql.checkData(0, 4, "'t1 in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)'")

    # run
    def test_grant_bugs(self):
        """Privileges bugs
        
        1. Verify bug TD-30642
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-22 Alex Duan Migrated from uncatalog/army/grant/test_grant_bugs.py

        """
        tdLog.debug(f"start to excute {__file__}")

        # TD-30642
        self.td_30642()

        tdLog.success(f"{__file__} successfully executed")

        

