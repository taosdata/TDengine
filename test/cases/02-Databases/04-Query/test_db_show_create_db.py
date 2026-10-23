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


class TestDatabaseShowCreateDb:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_show_create_db(self):
        """Show create db

        1. Create database
        2. Run SHOW CREATE DATABASE
        3. Verify the result

        Catalog:
            - Database:Query

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/db/show_create_db.sim

        """

        tdLog.info(f"=============== step2")
        tdSql.execute(f"create database db")
        tdSql.query(f"show create database db")

        tdSql.checkRows(1)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"use db")
        tdSql.query(f"show create database db")

        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "db")

        tdSql.execute(f"drop database db")

    def test_database_show_create_retentions(self):
        """SHOW CREATE DATABASE preserves the full RETENTIONS list

        1. Create a database with multiple RETENTIONS entries
        2. Verify SHOW CREATE DATABASE includes every entry

        Catalog:
            - Database:Query

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-10-12 Codex added RETENTIONS output regression

        """

        tdSql.execute("create database show_retention_db retentions -:7d,15s:21d,1m:365d")
        # Wait until the create-database transaction is committed so that
        # SHOW CREATE DATABASE reads the final retentions configuration.
        clusterComCheck.checkTransactions()
        tdSql.query("show create database show_retention_db")
        tdSql.checkRows(1)
        create_sql = tdSql.queryResult[0][1]
        assert "RETENTIONS -:7d,15s:21d,1m:365d" in create_sql, create_sql
        tdSql.execute("drop database show_retention_db")
