# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

import time
from new_test_framework.utils import tdLog, tdSql


class TestShowBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_show_basic(self):
        """Show Basic

        1. build cluster with 3 dnodes
        2. execute show vgroups commands

        Catalog:
            - Show

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-12-5 dmchen init

        """

        tdSql.execute(f"create database db1 vgroups 1 replica 1")
        tdSql.execute(f"use db1")
        self.test_show_vgroups_ready()


        tdSql.execute(f"create database db2 vgroups 1 replica 2")
        tdSql.execute(f"use db2")
        self.test_show_vgroups_ready()
    

        tdSql.execute(f"create database db3 vgroups 1 replica 3")
        tdSql.execute(f"use db3")
        self.test_show_vgroups_ready()

    def test_show_vgroups_ready(self):
        """check vgroups is ready

        Check vgroups is ready after create database
        
        Since: v3.3.8.0
        """

        count = 0
        while count < 200:
            time.sleep(1)
            tdSql.query(f"show vgroups")
            tdSql.checkRows(1)
            if tdSql.getData(0, 15) == True:
                break
            count += 1
        if count >= 200:
            raise Exception("show vgroups ready timeout")
