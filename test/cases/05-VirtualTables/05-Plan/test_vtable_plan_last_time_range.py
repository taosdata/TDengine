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

from new_test_framework.utils import tdLog, tdSql, etool
import pytest


class TestVtablePlanLastTimeRange:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.source_db = "vtable_last_time_source"
        cls.virtual_db = "vtable_last_time_virtual"
        for db in (cls.virtual_db, cls.source_db):
            tdSql.execute(f"drop database if exists {db}")
            tdSql.execute(f"create database {db} vgroups 1 cachemodel 'both'")

        tdSql.query("select cast(now as bigint)")
        cls.base_ts = tdSql.getData(0, 0) - 60000
        tdSql.execute(
            f"create stable {cls.source_db}.meters "
            "(ts timestamp, current float, voltage float, power float, temp float) "
            "tags (label nchar(64))"
        )
        tdSql.execute(
            f"create table {cls.source_db}.cabinet using {cls.source_db}.meters "
            "tags ('cabinet')"
        )
        tdSql.execute(
            f"insert into {cls.source_db}.cabinet values "
            f"({cls.base_ts}, 10, 220, 1000, 30) "
            f"({cls.base_ts + 1000}, null, 230, 1348.5, 31) "
            f"({cls.base_ts + 2000}, 20, 240, null, 32)"
        )
        tdSql.execute(
            f"create table {cls.virtual_db}.analysis "
            "(ts timestamp, max_temp float)"
        )
        # The placeholder shifts virtual column IDs away from the source IDs.
        tdSql.execute(
            f"create stable {cls.virtual_db}.vstb "
            "(ts timestamp, placeholder tinyint, current_v float, voltage_v float, "
            "power_v float, temp_v float, max_temp_v float) "
            "tags (label nchar(64)) virtual 1"
        )
        tdSql.execute(
            f"create vtable {cls.virtual_db}.vctb ("
            f"current_v from {cls.source_db}.cabinet.current, "
            f"voltage_v from {cls.source_db}.cabinet.voltage, "
            f"power_v from {cls.source_db}.cabinet.power, "
            f"temp_v from {cls.source_db}.cabinet.temp, "
            f"max_temp_v from {cls.virtual_db}.analysis.max_temp) "
            f"using {cls.virtual_db}.vstb (label) "
            f"tags (from {cls.source_db}.cabinet.label)"
        )
        tdSql.execute(
            f"create vtable {cls.virtual_db}.vntb ("
            "ts timestamp, placeholder tinyint, "
            f"current_v float from {cls.source_db}.cabinet.current, "
            f"voltage_v float from {cls.source_db}.cabinet.voltage, "
            f"power_v float from {cls.source_db}.cabinet.power, "
            f"temp_v float from {cls.source_db}.cabinet.temp, "
            f"max_temp_v float from {cls.virtual_db}.analysis.max_temp)"
        )
        tdSql.execute(
            f"create vtable {cls.virtual_db}.vntb_aligned (ts timestamp, "
            f"current float from {cls.source_db}.cabinet.current, "
            f"voltage float from {cls.source_db}.cabinet.voltage, "
            f"power float from {cls.source_db}.cabinet.power, "
            f"temp float from {cls.source_db}.cabinet.temp)"
        )

    def check_last(self, table, select_ts, predicate, expected_ts, expected_power, has_virtual_scan=None):
        projection = "last(_c0), last(power_v)" if select_ts else "last(power_v)"
        sql = f"select {projection} from {self.virtual_db}.{table}{predicate}"
        tdSql.query(f"explain verbose true {sql}")
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        assert "IS NOT NULL" not in plan, plan
        if has_virtual_scan is not None:
            assert ("Virtual Table Scan" in plan) == has_virtual_scan, plan
        tdSql.query(sql)
        tdSql.checkRows(1)
        if select_ts:
            tdSql.checkData(0, 0, expected_ts)
        tdSql.checkData(0, int(select_ts), expected_power)

    def do_last_time_range(self, table, select_ts):
        predicates = (
            "",
            " where _c0 >= now-1h and _c0 <= now",
            " where ts > now-10d",
            f" where ts >= {self.base_ts} and ts <= {self.base_ts + 2000}",
        )
        for cachemodel in ("none", "both"):
            tdSql.execute(f"alter database {self.source_db} cachemodel '{cachemodel}'")
            for flush in (False, True):
                if flush:
                    tdSql.execute(f"flush database {self.source_db}")
                for predicate in predicates:
                    self.check_last(table, select_ts, predicate, self.base_ts + 2000, 1348.5)
                self.check_last(
                    table, select_ts,
                    f" where _c0 >= {self.base_ts} and _c0 < {self.base_ts + 1000}",
                    self.base_ts, 1000,
                )
                self.check_last(
                    table, select_ts,
                    f" where ts >= {self.base_ts} and power_v > 0",
                    self.base_ts + 1000, 1348.5, has_virtual_scan=True,
                )
        print(f"{table} last time range (select_ts={select_ts}) [ passed ]")

    def do_skip_prefilter(self):
        predicate = f"where ts >= {self.base_ts} and ts <= {self.base_ts + 2000}"
        queries = (
            (f"select last(power_v) from {self.virtual_db}.vctb {predicate}", False),
            (f"select last(power_v) from {self.virtual_db}.vntb {predicate}", False),
            (f"select last(power_v) from {self.virtual_db}.vstb {predicate}", False),
            (f"select last(power) from {self.virtual_db}.vntb_aligned {predicate}", False),
            (f"select last(power) from {self.source_db}.cabinet {predicate}", True),
        )
        for sql, expect_prefilter in queries:
            for explain in ("explain verbose true", "explain analyze verbose true"):
                tdSql.query(f"{explain} {sql}")
                plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
                assert ("IS NOT NULL" in plan) == expect_prefilter, plan
            tdSql.query(sql)
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1348.5)
        print("virtual tables skip LAST non-null prefilters [ passed ]")

    @pytest.mark.parametrize("table", ("vctb", "vntb"))
    @pytest.mark.parametrize("select_ts", (True, False))
    def test_last_time_range(self, table, select_ts):
        """Query: LAST on referenced virtual columns with a time range.

        1. Check virtual child and normal tables with shifted column IDs.
        2. Check LAST(timestamp) together with LAST(value), and LAST(value) alone.
        3. Check time bounds, trailing NULL values, cache modes and flushed data.
        4. Check a value predicate that keeps the virtual scan in the plan.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci,virtual

        Jira: None

        History:
            - 2026-10-09 Jing Sima Created

        """
        self.do_last_time_range(table, select_ts)

    def test_skip_prefilter(self):
        """Plan: skip LAST non-null prefilters for every virtual table query.

        1. Check virtual child, normal and super tables without hidden filters.
        2. Check aligned single-source columns after virtual scan elimination.
        3. Check EXPLAIN ANALYZE and preserve prefilters on the source table.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci,virtual

        Jira: None

        History:
            - 2026-10-10 Jing Sima Created

        """
        self.do_skip_prefilter()
