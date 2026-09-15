import re
import time

import pytest

from new_test_framework.utils import ci_scaled_retry, tdLog, tdSql
from new_test_framework.utils.log_reader import LogReader
from new_test_framework.utils.srvCtl import sc


class TestInterpMixedPartition:
    updatecfgDict = {
        "asynclog": 0,
        "qDebugFlag": 135,
    }
    DB = "interp_mixed_partition"
    RANGE = (
        "range('2020-01-01 00:00:03', "
        "'2020-01-01 00:00:09') every(3s) fill(linear)"
    )
    TIME_FILTER = (
        "ts >= '2020-01-01 00:00:01' and "
        "ts <= '2020-01-01 00:00:11'"
    )
    RUNTIME_RE = re.compile(
        r"TimeSlice runtime interpMode=(-?\d+) "
        r"scanKind=([a-z_]+) scanInterpMode=(-?\d+)"
    )
    # EInterpMode values are defined in include/libs/nodes/plannodes.h.
    # "other"/-1 means Interp's direct child is not a scan operator.
    LEGACY_ROUTE = (0, "other", -1)
    OUTER_EPOCH_RE = re.compile(
        r"(TID:0x[0-9a-f]+ QID:0x[0-9a-f]+) "
        r"TimeSlice MAIN_FIRST early-stop phase=(\d+) .*"
        r"outerEpoch=(\d+)"
    )

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---

    def prepare(self):
        self.EXPECTED_ROUTE_BY_PARTITION = {
            "profile": (1, "table_merge", 1) if self.vgroups == 1
            else (1, "other", -1),
            "outer_id, kind": (2, "other", -1),
        }
        tdSql.execute(f"drop database if exists {self.DB}")
        tdSql.execute(
            f"create database {self.DB} vgroups {self.vgroups} keep 36500"
        )
        tdSql.execute(f"use {self.DB}")
        tdSql.execute(
            "create stable readings "
            "(ts timestamp, kind int, val double) "
            "tags (outer_id int, profile varchar(16))"
        )

        candidates = []
        tables_per_vgroup = 8 if self.vgroups == 1 else 7
        for index in range(64):
            table = f"p{index:02d}"
            tdSql.execute(
                f"create table {table} using readings "
                "tags (0, 'candidate')"
            )
            candidates.append(table)
            table_vgroups = self.table_vgroups()
            by_vgroup = {}
            for candidate, vgroup in table_vgroups.items():
                by_vgroup.setdefault(vgroup, []).append(candidate)
            if len(by_vgroup) == self.vgroups and all(
                len(tables) >= tables_per_vgroup
                for tables in by_vgroup.values()
            ):
                break
        else:
            raise AssertionError(
                "could not allocate enough candidate tables per vgroup: "
                f"{table_vgroups}"
            )

        for tables in by_vgroup.values():
            tables.sort(key=candidates.index)
        vgroups = sorted(by_vgroup)
        role_specs = (
            ("next17", 17, "a_next", 10, "next"),
            ("prev42", 42, "m_prev", 50, "prev"),
            ("main42", 42, "z_main", 80, "main"),
            ("prev17", 17, "m_prev", 40, "prev"),
            ("main17", 17, "z_main", 70, "main"),
            ("next42", 42, "a_next", 20, "next"),
        )
        self.role_tables = {}
        self.role_profiles = {}
        for slot, spec in enumerate(role_specs):
            role, outer_id, profile, kind, row_profile = spec
            tables = [by_vgroup[vgroup][slot] for vgroup in vgroups]
            self.role_tables[role] = tables
            self.role_profiles[role] = (kind, row_profile)
            for table in tables:
                tdSql.execute(
                    f"alter table {table} set tag outer_id = {outer_id}"
                )
                tdSql.execute(
                    f"alter table {table} set tag profile = '{profile}'"
                )

        self.role_tables["side17"] = [by_vgroup[vgroups[0]][6]]
        side42_slot = 7 if self.vgroups == 1 else 6
        self.role_tables["side42"] = [by_vgroup[vgroups[-1]][side42_slot]]
        self.role_profiles["side17"] = (60, "side")
        self.role_profiles["side42"] = (30, "side")
        for role, outer_id in (("side17", 17), ("side42", 42)):
            table = self.role_tables[role][0]
            tdSql.execute(
                f"alter table {table} set tag outer_id = {outer_id}"
            )
            tdSql.execute(
                f"alter table {table} set tag profile = 'q_side'"
            )

        # RANGE is [3s, 9s]. Each outer group has MAIN-, NEXT-, PREV-, and
        # side-only profiles with deliberately nonascending ordinary keys.
        insertion_order = (
            "prev17",
            "next42",
            "next17",
            "side42",
            "main17",
            "main42",
            "prev42",
            "side17",
        )
        for role in insertion_order:
            kind, profile = self.role_profiles[role]
            for table in reversed(self.role_tables[role]):
                self.insert_profile(table, kind, profile)

        expected_roles = {
            17: ["main17", "next17", "prev17"],
            42: ["main42", "next42", "prev42"],
        }
        for completion_order in expected_roles.values():
            for vgroup in vgroups:
                table_order = sorted(
                    completion_order,
                    key=lambda role: candidates.index(
                        next(
                            table
                            for table in self.role_tables[role]
                            if table_vgroups[table] == vgroup
                        )
                    ),
                )
                assert table_order != completion_order
            kind_order = sorted(
                completion_order,
                key=lambda role: self.role_profiles[role][0],
            )
            assert kind_order != completion_order
            inserted = [
                role for role in insertion_order if role in completion_order
            ]
            assert inserted != completion_order

    def insert_profile(self, table, kind, profile):
        seconds = {
            "main": (3, 9),
            "next": (3, 6, 10),
            "prev": (2, 6, 9),
            "side": (2, 10),
        }[profile]
        values = " ".join(
            "('2020-01-01 00:00:"
            f"{second:02d}', {kind}, {kind * 10 + second})"
            for second in seconds
        )
        tdSql.execute(f"insert into {table} values {values}")

    def interp_sql(
        self, partition, include_side, outer_id=None, hint="", limit=""
    ):
        if partition == "profile":
            projection = "profile, _irowts, interp(val)"
        else:
            projection = "outer_id, kind, _irowts, interp(val)"
        filters = [self.TIME_FILTER]
        if outer_id is not None:
            filters.append(f"outer_id = {outer_id}")
        if not include_side:
            filters.append("profile <> 'q_side'")
        return (
            f"select {hint}{projection} from readings "
            f"where {' and '.join(filters)} "
            f"partition by {partition} {self.RANGE} {limit}"
        ).strip()

    @staticmethod
    def first_seen_keys(rows, key_columns):
        keys = []
        for row in rows:
            key = row[0] if key_columns == 1 else tuple(row[:key_columns])
            if key not in keys:
                keys.append(key)
        return keys

    def query_with_route(self, sql, expected_route):
        log_reader = LogReader([
            sc.dnodeLogPath(index) + "/taosdlog.*"
            for index in range(1, self.dnodeNum + 1)
        ])
        tdSql.query(sql)
        rows = [tuple(row) for row in tdSql.queryResult]
        deadline = time.monotonic() + ci_scaled_retry(5)
        evidence = ""
        routes = []
        while time.monotonic() < deadline:
            evidence += log_reader.read()
            routes = [
                (int(mode), kind, int(scan_mode))
                for mode, kind, scan_mode in self.RUNTIME_RE.findall(
                    evidence
                )
            ]
            if routes and all(
                route == expected_route for route in routes
            ):
                return rows, routes, evidence
            time.sleep(0.1)

        raise AssertionError(
            "missing expected interpolation route: "
            f"expected={expected_route}, observed={routes}\n"
            f"last log access error: {log_reader.last_error or 'none'}\n"
            f"log tail:\n{evidence[-4000:]}"
        )

    def assert_plan_routes(self):
        if self.vgroups == 1:
            for partition in self.EXPECTED_ROUTE_BY_PARTITION:
                tdSql.query(f"explain {self.interp_sql(partition, False)}")
                plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
                assert "Exchange" not in plan, plan
                assert "Table Merge Scan" in plan, plan
            return
        interp_sql = self.interp_sql("profile", False)
        tdSql.query(f"explain {interp_sql}")
        lines = [str(row[0]) for row in tdSql.queryResult]
        plan = "\n".join(lines)
        interp_index = next(
            (index for index, line in enumerate(lines) if "Interp" in line),
            -1,
        )
        exchange_index = next(
            (
                index
                for index, line in enumerate(lines)
                if "Exchange" in line
            ),
            -1,
        )
        assert 0 <= interp_index < exchange_index, plan
        assert "Table Merge Scan" in plan, plan

        mixed_sql = self.interp_sql("outer_id, kind", False)
        tdSql.query(f"explain {mixed_sql}")
        lines = [str(row[0]) for row in tdSql.queryResult]
        plan = "\n".join(lines)
        indices = []
        for token in ("Interp", "Partition", "-> Merge ", "Exchange"):
            indices.append(
                next(
                    (
                        index
                        for index, line in enumerate(lines)
                        if token in line
                    ),
                    -1,
                )
            )
        assert all(index >= 0 for index in indices), plan
        assert indices[0] < indices[1] < indices[2] < indices[3], plan
        assert "Table Merge Scan" in plan, plan

    def assert_vgroup_distribution(self):
        table_vgroups = self.table_vgroups()
        all_vgroups = set(table_vgroups.values())
        assert len(all_vgroups) == self.vgroups, table_vgroups
        completion_roles = (
            "main17",
            "next17",
            "prev17",
            "main42",
            "next42",
            "prev42",
        )
        for role in completion_roles:
            tables = self.role_tables[role]
            actual = {table_vgroups[table] for table in tables}
            assert actual == all_vgroups, (
                f"completion profile does not span vgroups: role={role}, "
                f"tables={tables}, mapping={table_vgroups}"
            )

    def table_vgroups(self):
        tdSql.query(
            "select table_name, vgroup_id from "
            "information_schema.ins_tables "
            f"where db_name = '{self.DB}' and stable_name = 'readings'"
        )
        return {
            str(table): int(vgroup)
            for table, vgroup in tdSql.queryResult
        }

    # --- impl ---

    def do_completion_order(self):
        cases = (
            (
                "profile",
                1,
                17,
                ["z_main", "a_next", "m_prev"],
            ),
            (
                "outer_id, kind",
                2,
                17,
                [(17, 70), (17, 10), (17, 40)],
            ),
            (
                "outer_id, kind",
                2,
                42,
                [(42, 80), (42, 20), (42, 50)],
            ),
        )
        for partition, key_columns, outer_id, expected in cases:
            all_sql = self.interp_sql(
                partition,
                False,
                outer_id=outer_id,
                limit="slimit 3",
            )
            route = self.EXPECTED_ROUTE_BY_PARTITION[partition]
            if route == self.LEGACY_ROUTE:
                rows, _, _ = self.query_with_route(all_sql, route)
                actual = self.first_seen_keys(rows, key_columns)
                assert set(actual) == set(expected), actual
                expected = actual
            for _ in range(5):
                rows, _, _ = self.query_with_route(all_sql, route)
                assert self.first_seen_keys(rows, key_columns) == expected

                for offset, expected_key in enumerate(expected):
                    sql = self.interp_sql(
                        partition,
                        False,
                        outer_id=outer_id,
                        limit=f"slimit 1 soffset {offset}",
                    )
                    selected, _, _ = self.query_with_route(sql, route)
                    actual = self.first_seen_keys(selected, key_columns)
                    assert actual == [expected_key], (
                        f"partition={partition}, offset={offset}, "
                        f"expected={expected_key}, actual={actual}"
                    )
        print("completion-order SLIMIT/SOFFSET ........ [passed]")

    def do_outer_epoch_routing(self):
        sql = self.interp_sql("outer_id, kind", False)
        rows, _, evidence = self.query_with_route(
            sql, self.EXPECTED_ROUTE_BY_PARTITION["outer_id, kind"]
        )
        keys = self.first_seen_keys(rows, 2)
        expected = {
            17: [(17, 70), (17, 10), (17, 40)],
            42: [(42, 80), (42, 20), (42, 50)],
        }
        assert set(keys) == {
            key for group_keys in expected.values() for key in group_keys
        }, keys
        for outer_id, group_keys in expected.items():
            indices = [
                index
                for index, key in enumerate(keys)
                if key[0] == outer_id
            ]
            assert indices == list(range(indices[0], indices[-1] + 1)), keys
            assert [keys[index] for index in indices] == group_keys, keys

        notifications = {}
        for task_id, phase, epoch in self.OUTER_EPOCH_RE.findall(evidence):
            notifications.setdefault(task_id, []).append(
                (int(phase), int(epoch))
            )
        candidates = [
            observed
            for observed in notifications.values()
            if len({epoch for _, epoch in observed}) == 2
        ]
        assert len(candidates) == 1, notifications
        observed = candidates[0]
        epochs = []
        for _, epoch in observed:
            if not epochs or epochs[-1] != epoch:
                epochs.append(epoch)
        assert len(epochs) == 2 and epochs == sorted(epochs), observed
        assert all(epoch != 0 for epoch in epochs), observed
        print("mixed outer-epoch boundaries ........... [passed]")

    def do_group_membership(self):
        hint = "/*+ para_tables_sort() */ "
        cases = (
            (
                "profile",
                1,
                {"z_main", "a_next", "m_prev", "q_side"},
            ),
            (
                "outer_id, kind",
                2,
                {
                    (17, 70),
                    (17, 10),
                    (17, 40),
                    (42, 80),
                    (42, 20),
                    (42, 50),
                },
            ),
        )
        for partition, key_columns, expected in cases:
            outer_id = 17 if partition == "profile" else None
            plain = self.interp_sql(
                partition, True, outer_id=outer_id
            )
            legacy = self.interp_sql(
                partition, True, outer_id=outer_id, hint=hint
            )
            legacy_route = (
                (0, "table_merge", 0)
                if self.vgroups == 1 and partition == "profile"
                else self.LEGACY_ROUTE
            )
            route_cases = (
                (plain, self.EXPECTED_ROUTE_BY_PARTITION[partition]),
                (legacy, legacy_route),
            )
            for sql, route in route_cases:
                rows, _, _ = self.query_with_route(sql, route)
                actual = set(self.first_seen_keys(rows, key_columns))
                assert actual == expected, (
                    f"partition={partition}, route={route}, "
                    f"expected={expected}, actual={actual}"
                )
                if key_columns == 2:
                    for _, kind, timestamp, value in rows:
                        assert value == kind * 10 + timestamp.second, rows
        print("D3b group membership routes ............ [passed]")

    # --- main ---

    @pytest.mark.parametrize("vgroups", [1, 2])
    def test_interp_mixed_partition(self, vgroups):
        """Verify interpolation completion order and group membership.

        1. Pin single-vgroup SLIMIT/SOFFSET to group completion order.
        2. Compare D3b membership in phased and rejected LEGACY routes.
        3. Exercise tag-only and mixed tag-plus-column partitions.
        4. Verify mixed-group outer epochs in single and multiple vgroups.

        Catalog:
            - Query:Interp

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-02 OpenAI added completion-order baselines

        """
        self.vgroups = vgroups
        self.prepare()
        self.assert_vgroup_distribution()
        self.assert_plan_routes()
        self.do_completion_order()
        self.do_outer_epoch_routing()
        self.do_group_membership()
