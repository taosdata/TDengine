# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from new_test_framework.utils import (tdLog, tdSql, tdCom, tdStream, StreamCheckItem, waitForRows)


class TestStreamSubQueryInVtable5:
    """Test cases for virtual tables in stream subqueries (part 5: advanced basic cases)"""
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_in_vtable5(self):
        """Stream subqueries with NOT IN, multiple virtual tables, and empty-to-match transitions

        1. Verify NOT IN filtering through a virtual table.
        2. Verify two virtual-table IN subqueries in one stream.
        3. Verify initial empty-subquery filtering before a later matching sentinel.

        Catalog:
            - Stream

        Since: v3.3.4.0

        Labels: stream, vtable

        Jira: None

        History:
            - 2026-10-14 Split from stream_subquery_in_vtable.py for CI stability

        """
        self.do_stream_subquery_in_vtable5()

    def do_stream_subquery_in_vtable5(self):
        tdCom.create_snode_if_not_exists()
        tdSql.execute(f"alter all dnodes 'debugflag 135';")
        tdSql.execute(f"alter all dnodes 'stdebugflag 135';")

        for db in ["test_not_in_vtable", "ref_db_not_in",
                   "test_in_multi_vtable", "ref_db_multi1", "ref_db_multi2",
                   "test_in_vtable_empty", "ref_db_empty"]:
            tdSql.execute(f"drop database if exists {db}")

        streams = [
            self.NotInSubqueryVirtualTable(),
            self.InSubqueryMultipleVirtualTables(),
            self.InSubqueryVirtualTableEmptyThenMatch(),
        ]
        tdStream.checkAll(streams)

    class NotInSubqueryVirtualTable(StreamCheckItem):
        """Test NOT IN subquery with virtual table"""
        def __init__(self):
            self.db = "test_not_in_vtable"
            self.refdb = "ref_db_not_in"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_not_in"
            self.stream = "s_not_in_vtable"
            self.restb = "res_not_in_vtable"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable5.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable5.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, blocked int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, blocked int from {self.refdb}.{self.reftb}.blocked)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id not in (select id from {self.vtb} where blocked = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 2, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100) ('2026-01-01 00:00:01', 2, 200) ('2026-01-01 00:00:02', 3, 300)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 180)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 100)
            tdSql.checkData(1, 1, 3)
            tdSql.checkData(1, 2, 300)

    class InSubqueryMultipleVirtualTables(StreamCheckItem):
        """Test IN subquery with multiple virtual tables"""
        def __init__(self):
            self.db = "test_in_multi_vtable"
            self.refdb1 = "ref_db_multi1"
            self.refdb2 = "ref_db_multi2"
            self.triggertb = "trigger_tb"
            self.reftb1 = "ref_tb1"
            self.reftb2 = "ref_tb2"
            self.vtb1 = "vtb_multi1"
            self.vtb2 = "vtb_multi2"
            self.stream = "s_in_multi_vtable"
            self.restb = "res_in_multi_vtable"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable5.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb1} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable5.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb2} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable5.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")
            tdSql.execute(f"create table {self.refdb1}.{self.reftb1} (ts timestamp, id int, flag1 int)")
            tdSql.execute(f"create table {self.refdb2}.{self.reftb2} (ts timestamp, id int, flag2 int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb1} (ts timestamp, id int from {self.refdb1}.{self.reftb1}.id, flag1 int from {self.refdb1}.{self.reftb1}.flag1)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb2} (ts timestamp, id int from {self.refdb2}.{self.reftb2}.id, flag2 int from {self.refdb2}.{self.reftb2}.flag2)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb1} where flag1 = 1) "
                f"and id in (select id from {self.vtb2} where flag2 = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb1}.{self.reftb1} values ('2026-01-01 00:00:00', 1, 1) ('2026-01-01 00:00:01', 2, 1) ('2026-01-01 00:00:02', 3, 0)")
            tdSql.execute(f"insert into {self.refdb2}.{self.reftb2} values ('2026-01-01 00:00:00', 1, 0) ('2026-01-01 00:00:01', 2, 1) ('2026-01-01 00:00:02', 3, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100) ('2026-01-01 00:00:01', 2, 200) ('2026-01-01 00:00:02', 3, 300)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 1, 180)
            tdSql.checkData(0, 1, 2)
            tdSql.checkData(0, 2, 200)

    class InSubqueryVirtualTableEmptyThenMatch(StreamCheckItem):
        """Test initial empty IN results followed by a matching sentinel"""
        def __init__(self):
            self.db = "test_in_vtable_empty"
            self.refdb = "ref_db_empty"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_empty"
            self.stream = "s_in_vtable_empty"
            self.restb = "res_in_vtable_empty"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable5.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable5.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, flag int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, flag int from {self.refdb}.{self.reftb}.flag)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb} where flag = 999) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1) ('2026-01-01 00:00:01', 2, 2) ('2026-01-01 00:00:02', 3, 3)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100) ('2026-01-01 00:00:01', 2, 200) ('2026-01-01 00:00:02', 3, 300)")

        def check1(self):
            tdSql.execute(
                f"insert into {self.refdb}.{self.reftb} values "
                "('2026-01-01 00:00:03', 999, 999)"
            )
            tdSql.execute(
                f"insert into {self.db}.{self.triggertb} values "
                "('2026-01-01 00:00:03', 999, 99900)"
            )
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 1, 180)
            tdSql.checkData(0, 1, 999)
            tdSql.checkData(0, 2, 99900)
