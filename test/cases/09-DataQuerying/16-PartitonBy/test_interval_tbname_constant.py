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

from new_test_framework.utils import tdLog, tdSql, etool


class TestIntervalTbnameConstant:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def do_interval_tbname_constant(self):
        tdSql.execute("drop database if exists interval_tbname_constant_ci")
        tdSql.execute("create database interval_tbname_constant_ci keep 3650")
        tdSql.execute("use interval_tbname_constant_ci")
        tdSql.execute("create stable meters (ts timestamp, v int) tags (tag_id int)")

        long_name = "meter_" + "x" * 130
        start = 1767225600000
        for begin in range(0, 4200, 200):
            values = ",".join(f"({start + row * 1000},{row})" for row in range(begin, begin + 200))
            if begin == 0:
                tdSql.execute(f"insert into {long_name} using meters tags (1) values {values}")
            else:
                tdSql.execute(f"insert into {long_name} values {values}")

        tdSql.execute(
            f"insert into short_meter using meters tags (2) values "
            f"({start},1) ({start + 61000},2)"
        )
        tdSql.execute("create table empty_meter using meters tags (3)")

        tdSql.query("select count(*), _wstart, tbname from meters partition by tbname interval(1m)")
        tdSql.checkRows(72)
        counts = {long_name: [], "short_meter": []}
        for count, _, name in tdSql.queryResult:
            assert name in counts
            counts[name].append(count)
        assert counts[long_name] == [60] * 70
        assert counts["short_meter"] == [1, 1]

        # A filter and a direct projection must retain ordinary row columns.
        tdSql.query(f"select count(*) from meters where tbname = '{long_name}'")
        tdSql.checkData(0, 0, 4200)
        tdSql.query(
            f"select count(*), _wstart, tbname from meters where tbname = '{long_name}' "
            "partition by tbname interval(1m)"
        )
        tdSql.checkRows(70)
        assert all(row[0] == 60 and row[2] == long_name for row in tdSql.queryResult)
        tdSql.query(f"select tbname from {long_name} limit 3")
        tdSql.checkRows(3)
        for row in tdSql.queryResult:
            assert row[0] == long_name

        print("interval tbname constant .................... [passed]")

    def do_interval_tag_constant(self):
        tdSql.execute("use interval_tbname_constant_ci")
        tdSql.execute(
            "create stable tag_meters (ts timestamp, v int) "
            "tags (tag_name varchar(160), tag_text nchar(100), tag_id int)"
        )
        long_tag = "group_" + "a" * 130
        start = 1767225600000
        for begin in range(0, 4200, 200):
            values = ",".join(f"({start + row * 1000},{row})" for row in range(begin, begin + 200))
            if begin == 0:
                tdSql.execute(
                    f"insert into tag_meter_a using tag_meters tags ('{long_tag}','相同分组',7) values {values}"
                )
            else:
                tdSql.execute(f"insert into tag_meter_a values {values}")
        tdSql.execute(
            f"insert into tag_meter_b using tag_meters tags ('{long_tag}','相同分组',7) values "
            f"({start},1) ({start + 61000},2)"
        )
        tdSql.execute(
            f"insert into tag_meter_null using tag_meters tags (null,null,null) values ({start},3)"
        )
        tdSql.execute("create table tag_meter_empty using tag_meters tags ('unused','空',9)")

        for tag_col, expected in (("tag_name", long_tag), ("tag_text", "相同分组"), ("tag_id", 7)):
            tdSql.query(
                f"select count(*), _wstart, {tag_col} from tag_meters "
                f"partition by {tag_col} interval(1m)"
            )
            rows = tdSql.queryResult
            assert sum(row[0] for row in rows) == 4203
            group_rows = [row for row in rows if row[2] == expected]
            assert len(group_rows) == 70
            assert sorted(row[0] for row in group_rows) == [60] * 68 + [61, 61]
            assert [(row[0], row[2]) for row in rows if row[2] is None] == [(1, None)]

        # Predicates and ordinary projection still need dense tag columns.
        tdSql.query(f"select count(*) from tag_meters where tag_name = '{long_tag}'")
        tdSql.checkData(0, 0, 4202)
        tdSql.query(
            f"select count(*), _wstart, tag_name from tag_meters where tag_name = '{long_tag}' "
            "partition by tag_name interval(1m)"
        )
        assert len(tdSql.queryResult) == 70
        assert sum(row[0] for row in tdSql.queryResult) == 4202
        tdSql.query(
            "select count(tag_id), _wstart, tag_name from tag_meters "
            "partition by tag_name interval(1m)"
        )
        assert sum(row[0] for row in tdSql.queryResult) == 4202
        tdSql.query("select tag_name, tag_text, tag_id from tag_meter_a limit 1")
        assert tdSql.queryResult[0] == (long_tag, "相同分组", 7)

        tdSql.execute("alter table tag_meter_b set tag tag_name = 'changed_group'")
        tdSql.query(
            "select count(*), _wstart, tag_name from tag_meters "
            "partition by tag_name interval(1m)"
        )
        changed = [row for row in tdSql.queryResult if row[2] == "changed_group"]
        assert len(changed) == 2 and all(row[0] == 1 for row in changed)
        assert sum(row[0] for row in tdSql.queryResult) == 4203
        print("interval tag constant ....................... [passed]")

    def test_interval_tbname_constant(self):
        """Verify interval tbname and tag output across scan blocks and fallbacks.

        Check window counts and group keys for long tbname and tag values.
        Cover NULL, shared tags, tag changes, filters, and dense projections.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-27 Verify block constant tbname and tag values, including dense fallback.

        """
        self.do_interval_tbname_constant()
        self.do_interval_tag_constant()
