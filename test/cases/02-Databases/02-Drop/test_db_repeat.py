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

import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseRepeat:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_repeat(self):
        """Drop db many repeatly

        1. Create database
        2. Create table
        3. Drop both
        4. Repeat several times

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/repeat.sim

        """

        tdLog.info(f"============================ dnode1 start")

        tdSql.execute(f"create database d1")
        tdSql.execute(f"create table d1.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d2")
        tdSql.execute(f"create table d2.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d3")
        tdSql.execute(f"create table d3.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d4")
        tdSql.execute(f"create table d4.t1(ts timestamp, i int)")

        tdSql.execute(f"drop database d1")
        tdSql.execute(f"drop database d2")
        tdSql.execute(f"drop database d3")
        tdSql.execute(f"drop database d4")

        tdSql.execute(f"create database d5")
        tdSql.execute(f"create table d5.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d6")
        tdSql.execute(f"create table d6.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d7")
        tdSql.execute(f"create table d7.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d8")
        tdSql.execute(f"create table d8.t1(ts timestamp, i int)")

        tdSql.execute(f"drop database d5")
        tdSql.execute(f"drop database d6")
        tdSql.execute(f"drop database d7")
        tdSql.execute(f"drop database d8")

        tdSql.execute(f"create database d9;")
        tdSql.execute(f"create table d9.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d10;")
        tdSql.execute(f"create table d10.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d11")
        tdSql.execute(f"create table d11.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d12")
        tdSql.execute(f"create table d12.t1(ts timestamp, i int)")

        tdSql.execute(f"drop database d9")
        tdSql.execute(f"drop database d10")
        tdSql.execute(f"drop database d11")
        tdSql.execute(f"drop database d12")
