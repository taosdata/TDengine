# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableAlter3:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_alter_3(self):
        """Alter: import old data

        1. Add column
        2. Insert out-of-order data
        3. Query data

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/stable/alter_import.sim

        """

        tdLog.info(f"======== step1")
        tdSql.execute(f"create database d1 replica 1 duration 7 keep 50")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table tb (ts timestamp, a int)")
        tdSql.execute(f"insert into tb values(now-30d, -28)")
        tdSql.execute(f"insert into tb values(now-27d, -27)")
        tdSql.execute(f"insert into tb values(now-26d, -26)")
        tdSql.query(f"select count(a) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step2")
        # sql alter table tb(ts timestamp, a int, b smallint)
        tdSql.execute(f"alter table tb add column b smallint")
        tdSql.execute(f"insert into tb values(now-25d, -25, 0)")
        tdSql.execute(f"insert into tb values(now-22d, -24, 1)")
        tdSql.execute(f"insert into tb values(now-20d, -23, 2)")
        tdSql.query(f"select count(b) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"========= step3")
        tdSql.execute(f"insert into tb values(now-23d, -23, 0)")
        tdSql.execute(f"insert into tb values(now-21d, -21, 0)")
        tdSql.query(f"select count(b) from tb")
        tdSql.checkData(0, 0, 5)

        tdSql.execute(f"insert into tb values(now-29d, -29, 0)")
        tdSql.query(f"select count(b) from tb")
        tdSql.checkData(0, 0, 6)
