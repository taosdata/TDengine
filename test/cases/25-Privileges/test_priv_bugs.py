# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, epath, sc



class TestGrantBugs:
    
    def td_30642(self):
        tdLog.debug("===== step1")

        sqls = [
            "CREATE DATABASE IF NOT EXISTS `_xTest2` vgroups 1",
            "CREATE USER `_xTest` PASS 'AAbb1122'",
            "CREATE TABLE IF NOT EXISTS `_xTest2`.`meters` (ts timestamp, v1 int) tags(t1 int)",

            "CREATE DATABASE IF NOT EXISTS `test2` vgroups 1",
            "CREATE USER `user1` PASS 'AAbb1122'",
            "CREATE TABLE IF NOT EXISTS `test2`.`meters2` (ts timestamp, v1 int) tags(t1 int)",

            "CREATE USER `read_user` PASS 'AAbb1122'"
        ]
        tdSql.executes(sqls)

        tdLog.debug("===== step2")
        sql1 = 'GRANT select ON `_xTest2`.`meters` WITH (t1 = 1) TO `_xTest`'
        tdSql.query(sql1)
        sql1_verify = "select * from information_schema.ins_user_privileges where user_name='_xTest' and priv_type='SELECT' and db_name='_xTest2' and table_name='meters'"
        tdSql.query(sql1_verify)
        tdSql.checkRows(1)
        tdSql.checkData(0, 5, '(`_xTest2`.`meters`.`t1` = 1)')

        tdLog.debug("===== step3")
        sql2 = 'GRANT insert ON test2.meters2 WITH (t1 = 1) TO user1'
        tdSql.query(sql2)
        sql2_verify = "select * from information_schema.ins_user_privileges where user_name='user1' and priv_type='INSERT' and db_name='test2' and table_name='meters2'"
        tdSql.query(sql2_verify)
        tdSql.checkRows(1)
        tdSql.checkData(0, 5, '(`test2`.`meters2`.`t1` = 1)')

        # TS-6276
        tdLog.debug("===== step4")
        sql3 = "GRANT select ON `_xTest2`.`meters` WITH 't1 in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)' TO `read_user`";
        tdSql.query(sql3)
        sql3_verify = "select * from information_schema.ins_user_privileges where user_name='read_user' and priv_type='SELECT' and db_name='_xTest2' and table_name='meters'"
        tdSql.query(sql3_verify)
        tdSql.checkRows(1)
        tdSql.checkData(0, 5, "'t1 in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)'")

    # run
    def test_grant_bugs(self):
        """Privileges bugs
        
        1. Verify bug TD-30642
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional,security
        Jira: None

        History:
            - 2025-10-22 Alex Duan Migrated from uncatalog/army/grant/test_grant_bugs.py

        """
        tdLog.debug(f"start to excute {__file__}")

        # TD-30642
        self.td_30642()



        

