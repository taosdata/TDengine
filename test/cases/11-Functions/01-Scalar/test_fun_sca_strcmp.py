# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdLog, tdSql


class TestFunStrcmp:

    def setup_class(cls):
        cls.replicaVar = 1
        tdLog.debug(f"start to excute {__file__}")

    def test_fun_sca_strcmp(self):
        """ Fun: strcmp()

        1. Constants: equal, less, greater, prefix, empty string, case, trailing space
        2. NULL constants and NULL column values
        3. VARCHAR/NCHAR mixed comparison including multibyte characters
        4. Normal table, super table, WHERE/ORDER BY/GROUP BY usage
        5. Invalid parameter count and types

        Since: v3.5.0.0

        Labels: common,ci,functional

        Jira: None

        History:
            - 2026-11-20 Created for MySQL STRCMP compatibility
        """
        tdSql.execute("drop database if exists db_strcmp")
        tdSql.execute("create database db_strcmp")
        tdSql.execute("use db_strcmp")

        # constants
        cases = [
            ("'abc'", "'abc'", 0), ("'abc'", "'abd'", -1), ("'b'", "'a'", 1),
            ("'abc'", "'abcd'", -1), ("'abcd'", "'abc'", 1),
            ("''", "''", 0), ("''", "'a'", -1), ("'a'", "''", 1),
            ("'A'", "'a'", -1), ("'a'", "'A'", 1), ("'Ab'", "'aB'", -1),
            ("'a'", "'a '", -1), ("'a '", "'a'", 1),
            ("'你好'", "'你好'", 0), ("'你'", "'好'", -1),
        ]
        for a, b, exp in cases:
            tdSql.query(f"select strcmp({a}, {b})")
            tdSql.checkData(0, 0, exp)

        for sql in ("select strcmp(NULL, 'a')", "select strcmp('a', NULL)",
                    "select strcmp(NULL, NULL)", "select strcmp('', NULL)"):
            tdSql.query(sql)
            tdSql.checkData(0, 0, None)

        # mixed types
        tdSql.query("select strcmp(cast('abc' as varchar(10)), cast('abc' as nchar(10)))")
        tdSql.checkData(0, 0, 0)
        tdSql.query("select strcmp(cast('abc' as nchar(10)), cast('abd' as varchar(10)))")
        tdSql.checkData(0, 0, -1)
        tdSql.query("select strcmp(cast('你好' as nchar(10)), cast('你好' as varchar(10)))")
        tdSql.checkData(0, 0, 0)
        tdSql.query("select strcmp(cast('你' as varchar(10)), cast('好' as nchar(10)))")
        tdSql.checkData(0, 0, -1)
        tdSql.query("select strcmp(cast('b' as nchar(10)), cast('' as varchar(10)))")
        tdSql.checkData(0, 0, 1)

        # tables
        tdSql.execute("create stable st (ts timestamp, v varchar(20), n nchar(20), i int) "
                      "tags (t varchar(20))")
        tdSql.execute("create table ct1 using st tags ('abc')")
        tdSql.execute("create table ct2 using st tags ('xyz')")
        tdSql.execute("create table nt (ts timestamp, v varchar(20), n nchar(20))")
        tdSql.execute("insert into ct1 values (1700000000000, 'abc', 'abc', 1) "
                      "(1700000001000, 'abd', 'ABC', 2) (1700000002000, '', '', 3) "
                      "(1700000003000, NULL, 'x', 4) (1700000004000, 'x', NULL, 5)")
        tdSql.execute("insert into ct2 values (1700000000000, 'abc', '你好', 6)")
        tdSql.execute("insert into nt values (1700000000000, 'b', 'a') (1700000001000, 'a', 'a') "
                      "(1700000002000, NULL, NULL)")

        tdSql.query("select strcmp(v, 'abc') from ct1 order by ts")
        tdSql.checkRows(5)
        for r, e in enumerate([0, 1, -1, None, 1]):
            tdSql.checkData(r, 0, e)

        tdSql.query("select strcmp(v, n) from ct1 order by ts")
        for r, e in enumerate([0, 1, 0, None, None]):
            tdSql.checkData(r, 0, e)

        tdSql.query("select strcmp(v, n) from nt order by ts")
        for r, e in enumerate([1, 0, None]):
            tdSql.checkData(r, 0, e)

        tdSql.query("select strcmp(n, '你好') from ct2")
        tdSql.checkData(0, 0, 0)

        tdSql.query("select count(*) from st where strcmp(v, 'abc') = 0")
        tdSql.checkData(0, 0, 2)

        tdSql.query("select ts from ct1 where strcmp(v, 'abc') > 0 order by strcmp(v, n), ts")
        tdSql.checkRows(2)

        tdSql.query("select t, strcmp(t, 'abc') c from st group by t order by t")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)

        tdSql.query("select strcmp(t, v) from ct1 order by ts")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, -1)

        # long strings
        a = "a" * 20
        tdSql.query(f"select strcmp('{a}', '{a[:-1]}b')")
        tdSql.checkData(0, 0, -1)

        # max-length column values (varchar(20)/nchar(20) filled to the limit)
        tdSql.execute("create table nt_max (ts timestamp, v varchar(20), n nchar(20))")
        tdSql.execute(f"insert into nt_max values (1700000000000, '{a}', '{a}') "
                      f"(1700000001000, '{a[:-1]}b', '{a[:-1]}b')")
        tdSql.query("select strcmp(v, n), strcmp(v, 'aaaaaaaaaaaaaaaaaaa'), "
                    "strcmp('aaaaaaaaaaaaaaaaaaa', n) from nt_max order by ts")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, -1)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(1, 1, 1)

        # invalid UTF-8 in a VARCHAR that must be converted to NCHAR must not succeed
        tdSql.error("select strcmp(cast(x'ff' as varchar(10)), cast('a' as nchar(10)))")

        # errors
        for sql in ("select strcmp('a')", "select strcmp('a', 'b', 'c')", "select strcmp()",
                    "select strcmp(1, 2)", "select strcmp(i, 'a') from ct1",
                    "select strcmp(ts, 'a') from ct1", "select strcmp('a', true)"):
            tdSql.error(sql)
