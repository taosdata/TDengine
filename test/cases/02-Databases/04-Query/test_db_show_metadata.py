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

import platform
import time
import pytest
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatabaseShowMetadata:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def setup_method(self):
        # These methods used to run in their own taosd process, so every one of
        # them started from an empty server. They now share a single deployment,
        # therefore drop every user database first to keep that isolation
        # (several cases assert on information_schema.ins_databases).
        tdSql.query(
            "select name from information_schema.ins_databases "
            "where name not in ('information_schema', 'performance_schema')"
        )
        for i in range(tdSql.queryRows):
            tdSql.execute(f"drop database if exists `{tdSql.getData(i, 0)}`")


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
            - 2026-11-20 Alex Merged into 04-Query/test_db_show_metadata.py so the methods share one taosd deployment (CI deploys once per test class)

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


    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="RETENTIONS is currently unsupported on Windows",
    )
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
            - 2026-11-20 Alex Merged into 04-Query/test_db_show_metadata.py so the methods share one taosd deployment (CI deploys once per test class)

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


    def test_database_show_create_table(self):
        """Show create table

        1. Create a normal table
        2. Create a super table
        3. Create child tables
        4. Execute SHOW CREATE TABLE and verify the output
        5. Change the showFullCreateTableColumn parameter
        6. Execute SHOW CREATE TABLE again and verify the new output

        Catalog:
            - Database:Query

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/db/show_create_table.sim
            - 2026-11-20 Alex Merged into 04-Query/test_db_show_metadata.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"===============create three type table")
        tdSql.execute(f"create database db")
        tdSql.execute(f"use db")
        tdSql.execute(
            f"create table meters(ts timestamp, f binary(8)) tags(loc int, zone binary(8))"
        )
        tdSql.execute(f"create table t0 using meters tags(1,'ch')")
        tdSql.execute(f"create table normalTbl(ts timestamp, zone binary(8))")

        tdSql.execute(f"use db")
        tdSql.query(f"show create table meters")
        tdSql.checkRows(1)

        tdLog.info(f"===============check sub table")
        tdSql.query(f"show create table t0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "t0")

        tdLog.info(f"===============check normal table")

        tdSql.query(f"show create table normalTbl")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "normaltbl")

        tdLog.info(f"===============check super table")
        tdSql.query(f"show create table meters")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "meters")

        tdLog.info(f"===============check sub table with prefix")

        tdSql.query(f"show create table db.t0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "t0")

        tdLog.info(f"===============check normal table with prefix")
        tdSql.query(f"show create table db.normalTbl")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "normaltbl")

        tdLog.info(f"===============check super table with prefix")
        tdSql.query(f"show create table db.meters")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "meters")

        tdSql.execute('alter local \'showFullCreateTableColumn\' \'1\'')

        tdSql.query(f"show create table db.meters")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "meters")
        tdSql.checkData(0, 1, "CREATE STABLE `meters` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `f` VARCHAR(8) ENCODE 'disabled' COMPRESS 'zstd' LEVEL 'medium') TAGS (`loc` INT, `zone` VARCHAR(8)) SECURITY_LEVEL 0 SECURE_DELETE 0")

        tdSql.query(f"show create table db.normalTbl")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "CREATE TABLE `normaltbl` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `zone` VARCHAR(8) ENCODE 'disabled' COMPRESS 'zstd' LEVEL 'medium')")


        tdSql.execute('alter local \'showFullCreateTableColumn\' \'0\'')
        tdSql.query(f"show create table db.meters")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "CREATE STABLE `meters` (`ts` TIMESTAMP, `f` VARCHAR(8)) TAGS (`loc` INT, `zone` VARCHAR(8)) SECURITY_LEVEL 0 SECURE_DELETE 0")

        tdSql.query(f"show create table db.normalTbl")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "CREATE TABLE `normaltbl` (`ts` TIMESTAMP, `zone` VARCHAR(8))")
        create_sql = tdSql.queryResult[0][1]
        tdSql.execute(create_sql.replace("`normaltbl`", "`normal_copy`", 1))
        tdSql.query("describe normal_copy")
        tdSql.checkRows(2)
        tdSql.execute(f"drop database db")


    def test_empty_nchar_tag(self):
        """Show create table empty nchar tag

        1. when nchar-type tag is empty, show create table should output an empty string
        2. alter table to set nchar-type tag to a non-empty string, show create table should output the new string
        3. alter table to set nchar-type tag to an empty string, show create table should output an empty string again
        4. alter table to set nchar-type tag to a non-empty string again, show create table should output the new string again
        5. drop database


        Since: v3.3.6.14

        Labels: common,ci,nchar,tag,integration,functional

        Jira: TS-7526

        History:
            - 2025-10-24 Tony Zhang created
            - 2026-11-20 Alex Merged into 04-Query/test_db_show_metadata.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"===============create table with empty nchar tag")
        tdSql.execute(f"create database if not exists db")
        tdSql.execute(f"use db")
        tdSql.execute(f"create table stb (ts timestamp, v int) tags(t1 nchar(10), gid int)", show=True)
        tdSql.execute(f"create table ctb1 using stb tags('', 1)", show=True)
        tdSql.query(f"show create table ctb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "ctb1")
        tdSql.checkData(0, 1, 'CREATE TABLE `ctb1` USING `stb` (`t1`, `gid`) TAGS ("", 1)')

        tdSql.execute("alter table ctb1 set tag t1 = '测试'", show=True)
        tdSql.query(f"show create table ctb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "ctb1")
        tdSql.checkData(0, 1, 'CREATE TABLE `ctb1` USING `stb` (`t1`, `gid`) TAGS ("测试", 1)')


    def test_show_create_table_after_tag_drop_and_add(self):
        """show create table keeps surviving tag values after a tag is replaced

        1. Create a child table with four tag values
        2. Drop two middle stable tags and add a replacement tag
        3. Verify SHOW CREATE preserves the later tag value and emits NULL for the replacement tag

        Catalog:
            - Database:Query

        Since: v3.3.6.35

        Labels: common,ci,tag

        Feishu: 7056595118

        History:
            - 2026-07-23 Codex created
            - 2026-11-20 Alex Merged into 04-Query/test_db_show_metadata.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdSql.prepare(dbname="show_create_tag", drop=True)
        tdSql.execute("create stable stb (ts timestamp, v int) tags(t1 int, t2 int, t3 int, t4 int)")
        tdSql.execute("create table ctb using stb tags(1, 2, 3, 4)")
        tdSql.execute("alter stable stb drop tag t2")
        tdSql.execute("alter stable stb drop tag t3")
        tdSql.execute("alter stable stb add tag t5 int")

        tdSql.query("show create table ctb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "ctb")
        tdSql.checkData(0, 1, "CREATE TABLE `ctb` USING `stb` (`t1`, `t4`, `t5`) TAGS (1, 4, NULL)")


    def test_database_show_vgroup(self):
        """Show vgroups

        1. Create multiple databases
        2. Repeatedly create and drop tables
        3. Run SHOW VGROUPS after each cycle to confirm the expected vgroup count

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/vgroup.sim
            - 2026-11-20 Alex Merged into 04-Query/test_db_show_metadata.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"============================ dnode1 start")

        i = 0
        dbPrefix = "ob_vg_db"
        tbPrefix = "ob_vg_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=================== step 1")
        tdSql.prepare(dbname=db, vgroups=4)
        tdSql.execute(f"use {db}")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdSql.execute(f"create table table1 (ts timestamp, speed int)")
        tdSql.execute(f"create table table2 (ts timestamp, speed int)")
        tdSql.execute(f"create table table3 (ts timestamp, speed int)")
        tdSql.execute(f"create table table4 (ts timestamp, speed int)")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdSql.execute(f"create table table5 (ts timestamp, speed int)")
        tdSql.execute(f"create table table6 (ts timestamp, speed int)")
        tdSql.execute(f"create table table7 (ts timestamp, speed int)")
        tdSql.execute(f"create table table8 (ts timestamp, speed int)")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdSql.execute(f"create table table9 (ts timestamp, speed int)")
        tdSql.execute(f"create table table10 (ts timestamp, speed int)")
        tdSql.execute(f"create table table11 (ts timestamp, speed int)")
        tdSql.execute(f"create table table12 (ts timestamp, speed int)")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdSql.execute(f"create table table13 (ts timestamp, speed int)")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdSql.execute(f"drop table table13")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdSql.execute(f"create table table13 (ts timestamp, speed int)")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdLog.info(f"=================== step 2")
        i = 1
        db = dbPrefix + str(i)

        tdSql.execute(f"create database {db} vgroups 2")
        tdSql.execute(f"use {db}")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(2)

        i = 0
        db = dbPrefix + str(i)
        tdSql.execute(f"drop database {db}")

        i = 1
        db = dbPrefix + str(i)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table table2 (ts timestamp, speed int)")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(2)

        tdSql.execute(f"drop table table2")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(2)

        tdSql.execute(f"create table table1 (ts timestamp, speed int)")
        tdSql.execute(f"create table table2 (ts timestamp, speed int)")
        tdSql.execute(f"create table table3 (ts timestamp, speed int)")
        tdSql.execute(f"create table table4 (ts timestamp, speed int)")
        tdSql.execute(f"drop table table1")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(2)

        tdLog.info(f"=================== step 3")
        i = 0
        db = dbPrefix + str(i)
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table table1 (ts timestamp, speed int)")

        i = 2
        db = dbPrefix + str(i)
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table table3 (ts timestamp, speed int)")

        i = 3
        db = dbPrefix + str(i)
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table table4 (ts timestamp, speed int)")

        i = 4
        db = dbPrefix + str(i)
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(7)

        i = 0
        while i < 5:
            db = dbPrefix + str(i)
            tdSql.execute(f"drop database {db}")
            i = i + 1

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)


