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


class TestNormalTableDropReuse:

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


    def test_drop_reuse_same_name(self):
        """Repeatedly drop and recreate the same table name

        1. Create a normal table, insert and query it
        2. Drop the table and confirm that writing to it fails
        3. Recreate the same name, insert and query again
        4. Repeat 20 times with the same name

        The old test_normaltable_delete_reuse2 claimed this coverage but its
        code only ever created new names, so the same-name path was never run.

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-20 Alex Added while merging the delete/reuse cases
        """

        base_ts = 1750046400000
        tdSql.execute("create database db_same_name replica 1")
        tdSql.execute("use db_same_name")
        for x in range(1, 21):
            tdSql.execute("create table tb (ts timestamp, i int)")
            tdSql.execute(f"insert into tb values({base_ts + x * 1000}, {x})")
            tdSql.query("select * from tb")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, x)
            tdSql.execute("drop table tb")
            tdSql.error(f"insert into tb values({base_ts + x * 1000}, {x})")
            tdLog.info(f"===> same name loop times: {x}")
        tdSql.execute("drop database db_same_name")

    def test_drop_reuse_new_name(self):
        """Repeatedly drop a table and create the next name

        1. Create a normal table (new name)
        2. Insert data 
        3. Query data
        4. Repeat 20 timeses


        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/table/delete_reuse2.sim
            - 2026-11-20 Alex Merged into 01-NormalTables/test_normaltable_drop_reuse.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"======== step1")
        tdSql.execute(f"create database db1 replica 1")
        tdSql.execute(f"create table db1.t1 (ts timestamp, i int)")
        tdSql.execute(f"insert into db1.t1 values(now, 1)")

        tdSql.query(f"select * from db1.t1")
        tdSql.checkRows(1)

        tdLog.info(f"======== step2")
        tdSql.execute(f"drop table db1.t1")
        tdSql.error(f"insert into db1.t1 values(now, 2)")

        tdLog.info(f"========= step3")
        tdSql.execute(f"create table db1.tb1 (ts timestamp, i int)")
        tdSql.execute(f"insert into db1.tb1 values(now, 2)")
        tdSql.query(f"select * from db1.tb1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 2)

        tdSql.execute(f"use db1")
        tdLog.info(f"========= step4")
        x = 1
        while x < 20:
            tb = "tb" + str(x)
            tdSql.execute(f"drop table {tb}")
            tdSql.error(f"insert into {tb} values(now, -1)")

            x = x + 1
            tb = "tb" + str(x)

            tdSql.execute(f"create table {tb} (ts timestamp, i int)")
            tdSql.execute(f"insert into {tb} values(now, {x} )")
            tdSql.query(f"select * from {tb}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, x)

            tdLog.info(f"===> loop times: {x}")


    def test_normal_table_drop(self):
        """Normal table after drop

        1. Create a table → insert one record → query that record → repeat this sequence 8 times
        2. Drop all created tables
        3. Repeat 1 times
    

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/table/table.sim
            - 2026-11-20 Alex Merged into 01-NormalTables/test_normaltable_drop_reuse.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"============================ dnode1 start")

        i = 0
        dbPrefix = "ob_tb_db"
        tbPrefix = "ob_tb_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.error(f"drop database {db}")
        tdSql.prepare(dbname=db)

        tdLog.info(f"=============== step2-3-4")
        tdSql.execute(f"use {db}")

        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val tinyint, val2 tinyint)")
        tdSql.execute(f"insert into {tb} values(now, 1, 1)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 1)

        i = 2
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val smallint, val2 smallint)")
        tdSql.execute(f"insert into {tb} values(now, 2, 2)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 2)

        i = 3
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val int, val2 int)")
        tdSql.execute(f"insert into {tb} values(now, 3, 3)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 3)

        i = 4
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val bigint, val2 bigint)")
        tdSql.execute(f"insert into {tb} values(now, 4, 4)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 4)

        i = 5
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val float, val2 float)")
        tdSql.execute(f"insert into {tb} values(now, 5, 5)")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"==> {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkData(0, 1, 5.00000)

        i = 6
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val double, val2 double)")
        tdSql.execute(f"insert into {tb} values(now, 6, 6)")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"==> {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkData(0, 1, 6.000000000)

        i = 7
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {tb} (ts timestamp, val binary(20), val2 binary(20))"
        )
        tdSql.execute(f"insert into {tb} values(now, '7', '7')")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 7)

        tdLog.info(f"=============== step5")
        tdSql.query(f"show tables")
        tdSql.checkRows(7)

        i = 1
        while i < 8:
            tb = tbPrefix + str(i)
            tdSql.execute(f"drop table {tb}")
            i = i + 1

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step6-9")
        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val tinyint, val2 tinyint)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, 1, 1)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 1)

        i = 2
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val smallint, val2 smallint)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, 2, 2)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 2)

        i = 3
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val int, val2 int)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, 3, 3)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 3)

        i = 4
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val bigint, val2 bigint)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, 4, 4)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 4)

        i = 5
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val float, val2 float)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, 5, 5)")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"==> {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkData(0, 1, 5.00000)

        i = 6
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val double, val2 double)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, 6, 6)")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"==> {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkData(0, 1, 6.000000000)

        i = 7
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {tb} (ts timestamp, val binary(20), val2 binary(20))"
        )
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, '7', '7')")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 7)

        tdLog.info(f"=============== step10")
        tb = tbPrefix + str(i)
        tdSql.error(f"create table {tb} (ts timestamp, val tinyint, val2 tinyint)")
        tdSql.query(f"show tables")
        tdSql.checkRows(7)

        tdLog.info(f"=============== step11")
        tdSql.error(f"create table {tb} (ts timestamp, val float, val2 double)")
        tdSql.query(f"show tables")
        tdSql.checkRows(7)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
