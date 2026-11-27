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

from new_test_framework.utils import tdSql


def assert_interval_hint_modes(sql, fast_interval, ex_interval):
    """Verify hint result equivalence and the planner-selected interval mode."""
    tdSql.query(sql)
    expected = list(tdSql.queryResult)

    for hint, expected_fast, expected_ex in (
        ("FAST_INTERVAL()", fast_interval, False),
        ("EX_INTERVAL()", False, ex_interval),
    ):
        hinted = sql.replace("select", f"select /*+ {hint} */", 1)
        tdSql.query(f"explain verbose true {hinted}", queryTimes=1)
        plan = "\n".join(str(row) for row in tdSql.queryResult)
        modes = {mode for mode in ("fast", "ex", "normal") if f"mode={mode}" in plan}
        if not modes:
            modes = {"normal"}
        expected_mode = "ex" if expected_ex else ("fast" if expected_fast else "normal")
        assert modes == {expected_mode}, (
            f"{hint} interval mode mismatch: expected={expected_mode}, modes={modes}, plan={plan}"
        )

        tdSql.query(hinted)
        actual = list(tdSql.queryResult)
        assert actual == expected, (
            f"{hint} result differs from the regular interval path: "
            f"expected={expected}, actual={actual}"
        )


def assert_supported_interval_hint_modes():
    """Run both interval implementations on a deterministic multi-vgroup shape."""
    db = "interval_hints"
    tdSql.execute(f"drop database if exists {db}")
    tdSql.execute(f"create database {db} vgroups 2")
    tdSql.execute(f"create stable {db}.st(ts timestamp, f float, d double, i bigint) tags(t int)")
    for table in range(3):
        tdSql.execute(f"create table {db}.ct{table} using {db}.st tags({table})")
        tdSql.execute(
            f"insert into {db}.ct{table} values "
            f"('2024-01-01 00:00:00.000', {table + 1.5}, {table + 10.0}, {table + 1}) "
            f"('2024-01-01 00:00:05.000', {table + 2.5}, {table + 20.0}, {table + 2}) "
            f"('2024-01-01 00:00:10.000', {table + 3.5}, {table + 30.0}, {table + 3})"
        )
    tdSql.execute(f"create table {db}.ct_null using {db}.st tags(99)")
    tdSql.execute(
        f"insert into {db}.ct_null values "
        "('2024-01-01 00:00:20.000', NULL, NULL, 7) "
        "('2024-01-01 00:00:25.000', 8.0, 80.0, 8)"
    )

    assert_interval_hint_modes(
        f"select _wstart, _wend, _wduration, count(*), sum(f), avg(f), sum(d), avg(d) "
        f"from {db}.st where ts >= '2024-01-01 00:00:00' "
        f"and ts < '2024-01-01 00:00:20' interval(10s)",
        True,
        True,
    )
    tdSql.execute(f"drop database {db}")
