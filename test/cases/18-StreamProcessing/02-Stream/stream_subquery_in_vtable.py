# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import (tdLog, tdSql, tdCom, tdStream, StreamCheckItem, waitForRows)


class TestStreamSubQueryInVtable:
    """Test cases for virtual tables in IN subqueries for streams (part 1: basic IN)"""
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_in_vtable(self):
        """Stream with an IN subquery whose subquery scans a virtual table.

        A sliding(1s) stream filters its trigger rows with `WHERE col IN
        (SELECT ... FROM <vtable> WHERE ...)`. The subquery reads a virtual table
        that maps columns from a separate reference database, so the planner must
        set and propagate the streamVtableCalc flag for the subquery branch. Each
        case asserts the result table contains exactly the trigger rows whose id
        matches the subquery output.

        Scenarios (3):
            1. InSubqueryVirtualNormalTable    - IN, vtable over a basic table; keeps ids with status = 1.
            2. InSubqueryVirtualChildTable     - IN, vtable over a super-table child; keeps ids with status > 0.
            3. InSubqueryVirtualSuperTable     - IN, virtual super table over 2 children; keeps ids with active = 1.

        NOT IN, multiple-vtable, and empty-result scenarios live in
        stream_subquery_in_vtable5.py. Window and aggregation scenarios live in
        stream_subquery_in_vtable4.py.

        Since: v3.3.4.0

        Labels: stream, vtable

        Jira: None

        History:
            - 2026-03-18 Created
            - 2026-03-20 Split into two files for CI stability
            - 2026-06-16 Increase interval stream wait for CI load stability
            - 2026-06-24 Split window/aggregation cases out to vtable4 for CI stability
            - 2026-10-14 Split the basic cases across two files for CI stability
        """

        tdCom.create_snode_if_not_exists()
        tdSql.execute(f"alter all dnodes 'debugflag 135';")
        tdSql.execute(f"alter all dnodes 'stdebugflag 135';")

        # Clean up databases from previous runs
        for db in ["test_in_vtable_normal", "ref_db_normal", "test_in_vtable_child", "ref_db_child",
                   "test_in_vtable_super", "ref_db_super"]:
            tdSql.execute(f"drop database if exists {db}")

        streams = []

        # Basic IN cases
        streams.append(self.InSubqueryVirtualNormalTable())
        streams.append(self.InSubqueryVirtualChildTable())
        streams.append(self.InSubqueryVirtualSuperTable())

        tdStream.checkAll(streams)

    class InSubqueryVirtualNormalTable(StreamCheckItem):
        """Test IN subquery with virtual normal table"""
        def __init__(self):
            self.db = "test_in_vtable_normal"
            self.refdb = "ref_db_normal"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_normal"
            self.stream = "s_in_vtable_normal"
            self.restb = "res_in_vtable_normal"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, status int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, status int from {self.refdb}.{self.reftb}.status)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb} where status = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1) ('2026-01-01 00:00:01', 2, 0) ('2026-01-01 00:00:02', 3, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100) ('2026-01-01 00:00:01', 2, 200) ('2026-01-01 00:00:02', 3, 300)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 180)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 100)
            tdSql.checkData(1, 1, 3)
            tdSql.checkData(1, 2, 300)

    class InSubqueryVirtualChildTable(StreamCheckItem):
        """Test IN subquery with virtual child table"""
        def __init__(self):
            self.db = "test_in_vtable_child"
            self.refdb = "ref_db_child"
            self.triggertb = "trigger_tb"
            self.refstb = "ref_stb"
            self.refctb = "ref_ctb"
            self.vtb = "vtb_child"
            self.stream = "s_in_vtable_child"
            self.restb = "res_in_vtable_child"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")
            tdSql.execute(f"create stable {self.refdb}.{self.refstb} (ts timestamp, id int, status int) tags (location nchar(20))")
            tdSql.execute(f"create table {self.refdb}.{self.refctb} using {self.refdb}.{self.refstb} tags ('beijing')")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.refctb}.id, status int from {self.refdb}.{self.refctb}.status)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb} where status > 0) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.refctb} values ('2026-01-01 00:00:00', 10, 5) ('2026-01-01 00:00:01', 20, 0) ('2026-01-01 00:00:02', 30, 3)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 10, 1000) ('2026-01-01 00:00:01', 20, 2000) ('2026-01-01 00:00:02', 30, 3000)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 180)
            tdSql.checkData(0, 1, 10)
            tdSql.checkData(0, 2, 1000)
            tdSql.checkData(1, 1, 30)
            tdSql.checkData(1, 2, 3000)

    class InSubqueryVirtualSuperTable(StreamCheckItem):
        """Test IN subquery with virtual super table"""
        def __init__(self):
            self.db = "test_in_vtable_super"
            self.refdb = "ref_db_super"
            self.triggertb = "trigger_tb"
            self.refstb = "ref_stb"
            self.refctb1 = "ref_ctb1"
            self.refctb2 = "ref_ctb2"
            self.vstb = "vstb_super"
            self.stream = "s_in_vtable_super"
            self.restb = "res_in_vtable_super"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")
            tdSql.execute(f"create stable {self.refdb}.{self.refstb} (ts timestamp, id int, active int) tags (region nchar(20))")
            tdSql.execute(f"create table {self.refdb}.{self.refctb1} using {self.refdb}.{self.refstb} tags ('north')")
            tdSql.execute(f"create table {self.refdb}.{self.refctb2} using {self.refdb}.{self.refstb} tags ('south')")

            tdSql.execute(f"create stable {self.db}.{self.vstb} (ts timestamp, id int, active int) tags (region nchar(20)) virtual 1")
            tdSql.execute(f"create vtable {self.db}.vtb_super_north ({self.refdb}.{self.refctb1}.id, {self.refdb}.{self.refctb1}.active) using {self.db}.{self.vstb} tags ('north')")
            tdSql.execute(f"create vtable {self.db}.vtb_super_south ({self.refdb}.{self.refctb2}.id, {self.refdb}.{self.refctb2}.active) using {self.db}.{self.vstb} tags ('south')")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vstb} where active = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.refctb1} values ('2026-01-01 00:00:00', 100, 1) ('2026-01-01 00:00:01', 200, 0)")
            tdSql.execute(f"insert into {self.refdb}.{self.refctb2} values ('2026-01-01 00:00:02', 300, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 100, 10000) ('2026-01-01 00:00:01', 200, 20000) ('2026-01-01 00:00:02', 300, 30000)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 180)
            tdSql.checkData(0, 1, 100)
            tdSql.checkData(0, 2, 10000)
            tdSql.checkData(1, 1, 300)
            tdSql.checkData(1, 2, 30000)
