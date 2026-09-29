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

import time

from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestExternalWindowMultiGroup:
    DB = "ext_mg"
    SOC_DB = "ext_soc_reuse"
    RETRY = 120
    STABLE_SECONDS = 3
    SPILL_RULES = 2600

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode(1)

    def test_grouped_external_window_matches_single_group_baselines(self):
        """Five-leaf external-window JOINs preserve per-trigger-group results.

        1. Trigger one group first to cover a singleton multi-group request.
        2. Add two groups with colliding timestamps and JOIN keys.
        3. Run two window-bound and three broadcast leaves through four JOINs.
        4. Compare every grouped output subtable with its single-child stream.
        5. Prove valid-input empty-middle, duplicate, collision, and fallback cases.

        Catalog:
            - Streams:SubQuery:ExternalWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7024149350

        History:
            - 2026-08-26 OpenAI Added five-leaf multi-group JOIN regression
            - 2026-08-28 OpenAI Added singleton request protocol regression
            - 2026-09-18 OpenAI Added ordered-leaf fallback regression
        """

        tdStream.dropAllStreamsAndDbs()
        try:
            tables = self._prepare_fixture()
            self._create_normal_streams(tables)
            self._create_force_output_streams(tables)
            self._create_fallback_stream()
            self._create_ordered_leaf_fallback_stream()
            self._create_spill_streams(tables)
            tdStream.checkStreamStatus()
            self._write_trigger_rows(tables[:1])
            self._assert_singleton_grouped_result(tables[0][0])
            self._write_trigger_rows(tables[1:])
            self._assert_normal_results(tables)
            self._assert_force_output_results(tables)
            self._assert_fallback_result(tables)
            self._assert_ordered_leaf_fallback_result(tables)
            self._assert_spill_results(tables)
        finally:
            tdStream.dropAllStreamsAndDbs()

    def test_customer_soc_reloads_dimensions_between_requests(self):
        """Customer SOC JOIN reloads fixed dimensions for the next request.

        1. Use four groups with three distinct piles and an all-zero group.
        2. Join fragmented one-row brand/rule children with duplicate keys.
        3. Verify group-specific thresholds and the pre-filtered sibling.
        4. Restart the cluster to restore the saved customer Broadcast plan.
        5. Update dimensions and verify the next request reloads both relations.

        Catalog:
            - Streams:SubQuery:ExternalWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-09-09 OpenAI Added SOC request dimension reload regression
            - 2026-09-14 OpenAI Added fragmented dimensions and saved-plan restart
        """

        tdStream.dropAllStreamsAndDbs()
        try:
            self._prepare_customer_soc_fixture(group_count=4, fragmented=True)
            tdSql.execute(self._customer_soc_stream_sql())
            tdStream.checkStreamStatus("s_soc")

            self._write_customer_soc_rows(0, 3, 4, {3})
            result_sql = (
                "select trade_no,prev_soc,curr_soc,diff_soc,"
                f"percent_threshold,flag from {self.SOC_DB}.result "
                "order by trade_no,ts"
            )
            initial = [
                (f"order{group}", 20 + offset, 21 + offset, 1, 5.0 if group < 2 else 0.25, 0 if group < 2 else 1)
                for group in range(3)
                for offset in range(2)
            ]
            expected_tables = [(f"soc_m{group}",) for group in range(4)]
            self._wait_customer_soc_publications(
                result_sql, initial, expected_tables
            )

            # The exact customer query qualified before B1; restart exercises its saved plan.
            dnodes = [int(row[0]) for row in self._query_rows(
                "select id from information_schema.ins_dnodes order by id"
            )]
            stopped = []
            try:
                for dnode in dnodes:
                    sc.dnodeStop(dnode)
                    stopped.append(dnode)
                for dnode in dnodes:
                    sc.dnodeStart(dnode)
                    stopped.remove(dnode)
                clusterComCheck.checkDnodes(len(dnodes))
                tdStream.checkStreamStatus("s_soc")
            finally:
                for dnode in stopped:
                    sc.dnodeStart(dnode)

            tdSql.execute(
                f"insert into {self.SOC_DB}.r0 values "
                "('2025-01-01 00:00:00',0,0.5,20,'INSERT')"
            )
            tdSql.execute(
                f"insert into {self.SOC_DB}.r1 values "
                "('2025-01-01 00:00:00',1,9,20,'INSERT')"
            )
            self._write_customer_soc_rows(3, 2, 4, {3})
            updated = [
                (f"order{group}", 22 + offset, 23 + offset, 1, 0.5 if group < 2 else 9.0, 1 if group < 2 else 0)
                for group in range(3)
                for offset in range(2)
            ]
            expected = sorted(initial + updated, key=lambda row: (row[0], row[1]))
            self._wait_customer_soc_publications(
                result_sql, expected, expected_tables
            )
        finally:
            tdStream.dropAllStreamsAndDbs()

    def test_local_window_join_fixed_sources(self):
        """Fixed normal/child sources preserve grouped JOIN results.

        1. Build three trigger groups and independent fixed normal/child sources.
        2. Exercise aggregate, time-bound scalar fallback, and Broadcast scalar.
        3. Check exact values, all-NULL empty outcomes, and fixed-source routing.
        4. Verify output tables and %%1/%%2/_tgrpid partition identities.

        Catalog:
            - Streams:SubQuery:ExternalWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-09-23 OpenAI Added fixed-source local-window JOIN matrix
        """

        base = 1767225600000
        sources = (("fn", "fixed_normal"), ("fc", "fixed_child"))
        modes = ("aggregate", "scalar", "broadcast")
        for source_key, source in sources:
            for mode in modes:
                db = f"ext_l_{source_key}_{mode}"
                tdSql.execute(f"drop database if exists {db}")
                try:
                    tdSql.executes(
                        [
                            f"create database {db} vgroups 3",
                            f"create stable {db}.trigger_src "
                            "(ts timestamp,v int) tags(g int)",
                            f"create table {db}.g0 using {db}.trigger_src tags(0)",
                            f"create table {db}.g1 using {db}.trigger_src tags(1)",
                            f"create table {db}.g2 using {db}.trigger_src tags(2)",
                            f"create table {db}.fixed_normal (ts timestamp,v int)",
                            f"create stable {db}.fixed_stable "
                            "(ts timestamp,v int) tags(k int)",
                            f"create table {db}.fixed_child using "
                            f"{db}.fixed_stable tags(9)",
                        ]
                    )

                    selected_values = (900, 901, 902, 903, 777)
                    other_values = (1900, 1901, 1902, 1903, 1777)
                    normal_values = (
                        selected_values if source == "fixed_normal" else other_values
                    )
                    child_values = (
                        selected_values if source == "fixed_child" else other_values
                    )
                    fixed_timestamps = (
                        base,
                        base + 1000,
                        base + 2000,
                        base + 3000,
                        base - 10000,
                    )
                    for table, values in (
                        ("fixed_normal", normal_values),
                        ("fixed_child", child_values),
                    ):
                        rows = " ".join(
                            f"({timestamp},{value})"
                            for timestamp, value in zip(fixed_timestamps, values)
                        )
                        tdSql.execute(f"insert into {db}.{table} values {rows}")

                    right_queries = {
                        "aggregate": (
                            f"select _twend ts,last(v) rv from {db}.{source} "
                            "where ts>=_twstart and ts<=_twend"
                        ),
                        # This time-bound scalar keeps the pre-existing split path.
                        "scalar": (
                            f"select _twend ts,v rv from {db}.{source} "
                            "where ts>=_twstart and ts<=_twend and v%10=1"
                        ),
                        # The direct scalar relation is the qualified Broadcast path.
                        "broadcast": (
                            f"select _twend ts,v rv from {db}.{source} where v=777"
                        ),
                    }
                    stream_sql = (
                        f"create stream {db}.s_local count_window(2,1) "
                        f"from {db}.trigger_src partition by tbname,g "
                        f"into {db}.result output_subtable(concat('local_',%%1)) "
                        "(ts,lv,rv) tags(source varchar(64) as %%1,"
                        "trigger_group int as %%2,group_id bigint as _tgrpid) as "
                        "select a.ts,a.lv,b.rv from "
                        "(select _twend ts,last(v) lv from %%tbname "
                        "where ts>=_twstart and ts<=_twend) a join "
                        f"({right_queries[mode]}) b on a.ts=b.ts"
                    )
                    tdSql.execute(stream_sql)
                    tdStream.checkStreamStatus("s_local")

                    trigger_parts = []
                    for group in range(3):
                        values = (
                            (None, None, None, None)
                            if group == 2
                            else tuple(10 * (group + 1) + index for index in range(4))
                        )
                        rows = " ".join(
                            f"({base + index * 1000},"
                            f"{'NULL' if value is None else value})"
                            for index, value in enumerate(values)
                        )
                        trigger_parts.append(f"{db}.g{group} values {rows}")
                    tdSql.execute("insert into " + " ".join(trigger_parts))

                    ends = (1, 2) if mode == "scalar" else (1, 2, 3)
                    # Existing LAST semantics emit no aggregate row for an
                    # all-NULL window, so g2 is a legal empty JOIN group.
                    expected = [
                        (
                            f"local_g{group}",
                            f"g{group}",
                            group,
                            None if group == 2 else 10 * (group + 1) + end,
                            901
                            if mode == "scalar"
                            else 777
                            if mode == "broadcast"
                            else 900 + end,
                        )
                        for group in range(2)
                        for end in ends
                    ]
                    result_sql = (
                        "select tbname,source,trigger_group,lv,rv "
                        f"from {db}.result order by tbname,ts"
                    )
                    tdSql.checkResultsByFunc(
                        sql=result_sql,
                        func=lambda: tdSql.queryResult == expected,
                        retry=self.RETRY,
                    )
                    tdSql.query(result_sql)
                    tdSql.checkRows(len(expected))
                    for row, values in enumerate(expected):
                        for col, value in enumerate(values):
                            tdSql.checkData(row, col, value)

                    table_sql = (
                        "select table_name from information_schema.ins_tables "
                        f"where db_name='{db}' and stable_name='result' "
                        "order by table_name"
                    )
                    tdSql.checkResultsByFunc(
                        sql=table_sql,
                        func=lambda: tdSql.queryResult
                        == [("local_g0",), ("local_g1",), ("local_g2",)],
                        retry=self.RETRY,
                    )

                    expected_counts = [
                        (f"local_g{group}", len(ends) if group < 2 else 0)
                        for group in range(3)
                    ]
                    gid_sql = (
                        "select tbname,group_id,count(*) "
                        f"from {db}.result group by tbname,group_id order by tbname"
                    )
                    tdSql.checkResultsByFunc(
                        sql=gid_sql,
                        func=lambda: len(tdSql.queryResult) == 3
                        and [(row[0], row[2]) for row in tdSql.queryResult]
                        == expected_counts
                        and all(row[1] not in (None, 0) for row in tdSql.queryResult)
                        and len({row[1] for row in tdSql.queryResult}) == 3,
                        retry=self.RETRY,
                    )
                finally:
                    tdSql.execute(f"drop stream if exists {db}.s_local")
                    tdSql.execute(f"drop database if exists {db}")

    def test_nested_on_only_keys_preserve_null_and_duplicate_matches(self):
        """Composite JOIN keys preserve NULL exclusion and duplicate multiplicity.

        The TAG key is only referenced by the outer ON clause. Compare ordinary
        planning and hash JOIN on the same timestamp with duplicate TAG values.

        Catalog:
            - Streams:SubQuery:ExternalWindow

        Since: v3.4.2.9

        Labels: common,ci,integration,functional
        Feishu: None

        History:
            - 2026-09-17 OpenAI Added ON-only composite key result regression
        """
        db = "ext_on_only_keys"
        tdSql.execute(f"create database {db} vgroups 3")
        try:
            tdSql.execute(f"create table {db}.a (ts timestamp,v int)")
            tdSql.execute(f"create table {db}.b (ts timestamp,v int) tags(k int)")
            tdSql.execute(f"create table {db}.c (ts timestamp,k int,v int) tags(id int)")
            ts = "'2026-09-17 00:00:00'"
            tdSql.execute(f"insert into {db}.a values ({ts},7)")
            for index, key in enumerate((1, 1, 2, "NULL")):
                tdSql.execute(f"insert into {db}.b{index} using {db}.b tags({key}) values({ts},0)")
            for index, (key, value) in enumerate(((1, 10), (1, 20), (2, 30), ("NULL", 90), (3, 99))):
                tdSql.execute(
                    f"insert into {db}.c{index} using {db}.c tags({index}) values({ts},{key},{value})"
                )
            expected = [(7, 10), (7, 10), (7, 20), (7, 20), (7, 30)]
            for hint in ("", "/*+ hash_join() */"):
                tdSql.query(
                    f"select {hint} a.v,c.v from {db}.a a join {db}.b b on a.ts=b.ts "
                    f"join {db}.c c on b.ts=c.ts and b.k=c.k order by c.v"
                )
                assert tdSql.queryResult == expected, (hint, tdSql.queryResult)
        finally:
            tdSql.execute(f"drop database if exists {db}")

    def test_customer_soc_continuous_same_group_batches(self):
        """Customer SOC publishes exact results across continuous batches.

        1. Share one pile across two gun groups and add a filtered sibling.
        2. Run the exact five-branch state/count-window customer query.
        3. Send twelve four-row batches without waiting between writes.
        4. Check all SOC deltas and thresholds from both gun groups.
        5. Prove the filtered group creates no output subtable.

        Catalog:
            - Streams:SubQuery:ExternalWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-09-10 OpenAI Added continuous same-group SOC regression
        """

        tdStream.dropAllStreamsAndDbs()
        try:
            self._prepare_customer_soc_fixture()
            tdSql.execute(self._customer_soc_stream_sql())
            tdStream.checkStreamStatus("s_soc")

            for start in range(0, 48, 4):
                self._write_customer_soc_rows(start, 4)

            result_sql = (
                "select trade_no,prev_soc,curr_soc,diff_soc,"
                f"percent_threshold,flag from {self.SOC_DB}.result "
                "order by trade_no,ts"
            )
            expected = [
                (f"order{group}", 20 + offset, 21 + offset, 1, 5.0, 0)
                for group in range(2)
                for offset in range(47)
            ]
            self._wait_customer_soc_publications(
                result_sql, expected, [("soc_m0",), ("soc_m1",)]
            )
        finally:
            tdStream.dropAllStreamsAndDbs()

    def test_customer_soc_new_order_starts_with_zero_soc(self):
        """A new order must not reuse the previous order's nonzero SOC scope.

        Run the exact five-branch SQL across an order transition, then keep the
        new order zero before introducing nonzero rows. A third group stays
        legally empty. Assert literal window ends, order values and SOC deltas.

        Catalog:
            - Streams:SubQuery:ExternalWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional
        Feishu: None

        History:
            - 2026-09-15 OpenAI Added ancestor-transition sensitivity
        """
        tdStream.dropAllStreamsAndDbs()
        try:
            self._prepare_customer_soc_fixture(group_count=3)
            tdSql.execute(self._customer_soc_stream_sql())
            tdStream.checkStreamStatus("s_soc")
            result_sql = (
                "select tbname,cast(ts as bigint)-"
                "cast(cast('2025-01-01 00:00:00' as timestamp) as bigint),"
                "trade_no,prev_soc,curr_soc,diff_soc,percent_threshold,flag "
                f"from {self.SOC_DB}.result order by tbname,ts"
            )
            tables = [("soc_m0",), ("soc_m1",), ("soc_m2",)]
            self._write_customer_soc_rows(0, 4, 3, {2})
            expected = [
                (f"soc_m{group}", second * 1000, f"order{group}",
                 19 + second, 20 + second, 1, 5.0, 0)
                for group in range(2) for second in range(1, 4)
            ]
            self._wait_customer_soc_publications(result_sql, expected, tables)
            for start in (4, 8):
                parts = []
                for group in range(3):
                    rows = " ".join(
                        f"('2025-01-01 00:00:{second:02d}','next{group}',3,"
                        f"{0 if group == 2 or second < 8 else 40 + second},"
                        f"{second},10)"
                        for second in range(start, start + 4)
                    )
                    parts.append(f"{self.SOC_DB}.m{group} values {rows}")
                tdSql.execute("insert into " + " ".join(parts))
                # EXTEND(1) ends the old order at new-order ts - 1; its
                # descendants reset before the new row, so no (3,4) window.
                if start == 8:
                    # Windows ending at seconds 5..8 have a zero-only new-order
                    # prefix in t0; the earlier order must not make them eligible.
                    expected += [
                        (f"soc_m{group}", second * 1000, f"next{group}",
                         39 + second, 40 + second, 1, 5.0, 0)
                        for group in range(2) for second in range(9, 12)
                    ]
                self._wait_customer_soc_publications(result_sql, sorted(expected), tables)
        finally:
            tdStream.dropAllStreamsAndDbs()

    def test_customer_soc_many_groups_with_empty_results(self):
        """Customer SOC preserves groups and empty results across Reader sources.

        1. Create forty gun groups across three vgroups with one shared pile.
        2. Keep every seventh group's SOC zero and add a pre-filtered sibling.
        3. Execute the five-branch state/count-window customer query.
        4. Write eight batches without waiting for individual group results.
        5. Verify exact SOC values, cardinality and output table identities.

        Catalog:
            - Streams:SubQuery:ExternalWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-09-10 OpenAI Added multi-source SOC empty-group regression
        """

        groups = 40
        zero_groups = set(range(0, groups, 7))
        tdStream.dropAllStreamsAndDbs()
        try:
            self._prepare_customer_soc_fixture(group_count=groups, vgroups=3)
            tdSql.execute(self._customer_soc_stream_sql())
            tdStream.checkStreamStatus("s_soc")
            for start in range(0, 32, 4):
                self._write_customer_soc_rows(start, 4, groups, zero_groups)

            result_sql = (
                "select tbname,trade_no,prev_soc,curr_soc,diff_soc,"
                f"percent_threshold,flag from {self.SOC_DB}.result "
                "order by trade_no,ts"
            )
            expected = sorted(
                (
                    f"soc_m{group}",
                    f"order{group}",
                    20 + offset,
                    21 + offset,
                    1,
                    5.0,
                    0,
                )
                for group in range(groups)
                if group not in zero_groups
                for offset in range(31)
            )
            tables = sorted((f"soc_m{group}",) for group in range(groups))
            self._wait_customer_soc_publications(result_sql, expected, tables)
            table_counts = [
                (
                    f"soc_m{group}",
                    self._query_rows(
                        f"select count(*) from {self.SOC_DB}.soc_m{group}"
                    )[0][0],
                )
                for group in range(groups)
            ]
            assert table_counts == [
                (f"soc_m{group}", 0 if group in zero_groups else 31)
                for group in range(groups)
            ], table_counts
        finally:
            tdStream.dropAllStreamsAndDbs()

    def test_customer_soc_reader_fetch_tag_dimension(self):
        """Verify Reader fetch results with a TAG-filtered SOC dimension table.

        1. Create twelve monitor groups across three vgroups.
        2. Store pile_code as a TAG on the pile dimension stable.
        3. Keep two groups empty and write continuous four-row batches.
        4. Verify exact first-batch and continuation results and table identities.

        Catalog:
            - Streams:SubQuery:ExternalWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-09-20 OpenAI Added TAG dimension Reader fetch regression
        """

        groups = 12
        zero_groups = {0, 7}
        tdStream.dropAllStreamsAndDbs()
        try:
            self._prepare_customer_soc_fixture(
                group_count=groups, vgroups=3, pile_code_is_tag=True
            )
            tdSql.execute(self._customer_soc_stream_sql())
            tdStream.checkStreamStatus("s_soc")
            result_sql = (
                "select tbname,trade_no,prev_soc,curr_soc,diff_soc,"
                f"percent_threshold,flag from {self.SOC_DB}.result "
                "order by trade_no,ts"
            )
            tables = [(f"soc_m{group}",) for group in range(groups)]
            start = 0
            for stop in (8, 32):
                for offset in range(start, stop, 4):
                    self._write_customer_soc_rows(
                        offset, 4, groups, zero_groups
                    )
                expected = sorted(
                    (
                        f"soc_m{group}",
                        f"order{group}",
                        20 + offset,
                        21 + offset,
                        1,
                        5.0,
                        0,
                    )
                    for group in range(groups)
                    if group not in zero_groups
                    for offset in range(stop - 1)
                )
                self._wait_customer_soc_publications(
                    result_sql, expected, sorted(tables)
                )
                start = stop
        finally:
            tdStream.dropAllStreamsAndDbs()

    def _prepare_customer_soc_fixture(
        self,
        group_count=2,
        vgroups=1,
        fragmented=False,
        pile_code_is_tag=False,
    ):
        db = self.SOC_DB
        pile_stable_sql = (
            f"create stable {db}.pile (ts timestamp,pile_brand_setting_id int,"
            "station_belong_to int,type varchar(16)) "
            "tags(id int,pile_code varchar(16))"
            if pile_code_is_tag
            else f"create stable {db}.pile (ts timestamp,pile_code varchar(16),"
            "pile_brand_setting_id int,station_belong_to int,type varchar(16)) "
            "tags(id int)"
        )
        pile_insert_sql = (
            f"insert into {db}.p0 using {db}.pile tags(0,'p0') values "
            "('2025-01-01 00:00:00',0,0,'INSERT')"
            if pile_code_is_tag
            else f"insert into {db}.p0 using {db}.pile tags(0) values "
            "('2025-01-01 00:00:00','p0',0,0,'INSERT')"
        )
        tdSql.executes(
            [
                f"create database {db} vgroups {vgroups}",
                f"create stable {db}.monitor (ts timestamp,trade_no varchar(32),"
                "charge_status int,soc int,charged_time int,pile_kw double) "
                "tags(pile_code varchar(16),gun_code int)",
                "create table "
                + " ".join(
                    f"{db}.m{group} using {db}.monitor "
                    f"tags('p{max(0, group - 1) if fragmented else 0}',{group})"
                    for group in range(group_count)
                )
                + f" {db}.m_filtered using {db}.monitor tags('p0',{group_count})",
                pile_stable_sql,
                f"create stable {db}.brand (ts timestamp,pile_brand_setting_id int,"
                "pile_type int,type varchar(16)) tags(id int)",
                f"create stable {db}.rule (ts timestamp,station_id int,"
                "percent_threshold double,rule_id int,type varchar(16)) tags(id int)",
                pile_insert_sql,
                f"insert into {db}.b0 using {db}.brand tags(0) values "
                "('2025-01-01 00:00:00',0,300102,'INSERT')",
                f"insert into {db}.r0 using {db}.rule tags(0) values "
                "('2025-01-01 00:00:00',0,5,20,'INSERT')",
            ]
        )

        if fragmented:
            if pile_code_is_tag:
                tdSql.executes([
                    f"insert into {db}.p1 using {db}.pile tags(1,'p1') values "
                    "('2025-01-01 00:00:00',1,1,'INSERT')",
                    f"insert into {db}.p2 using {db}.pile tags(2,'p2') values "
                    "('2025-01-01 00:00:00',2,2,'INSERT')",
                ])
            else:
                tdSql.executes([
                    f"insert into {db}.p1 using {db}.pile tags(1) values "
                    "('2025-01-01 00:00:00','p1',1,1,'INSERT')",
                    f"insert into {db}.p2 using {db}.pile tags(2) values "
                    "('2025-01-01 00:00:00','p2',2,2,'INSERT')",
                ])
            # Every child contributes one row. Unmatched keys still pass t3/t4's fixed filter.
            for start in range(1, 2737, 100):
                end = min(start + 100, 2737)
                tdSql.execute("insert into " + " ".join(
                    f"{db}.b{key} using {db}.brand tags({key}) values "
                    f"('2025-01-01 00:00:00',{key},300102,'INSERT')"
                    for key in range(start, end)
                ))
                tdSql.execute("insert into " + " ".join(
                    f"{db}.r{key} using {db}.rule tags({key}) values "
                    f"('2025-01-01 00:00:00',{key},{0.25 if key == 1 else 7},20,'INSERT')"
                    for key in range(start, end)
                ))
            # Matching duplicates are intentional. The timestamp sink collapses them;
            # executor tests assert duplicate multiplicity before sink writes.
            tdSql.execute(
                f"insert into {db}.b_duplicate using {db}.brand tags(3000) values "
                "('2025-01-01 00:00:00',0,300102,'INSERT')"
            )

    def _customer_soc_stream_sql(self):
        db = self.SOC_DB
        return f"""
            create stream {db}.s_soc window (
                state_window(trade_no) extend(1) as w_order,count_window(2,1))
            from {db}.monitor partition by tbname,pile_code,gun_code
            stream_options(pre_filter(ifnull(trade_no,'')<>'' and
                trade_no not like '000000000%' and charge_status=3))
            into {db}.result output_subtable(concat('soc_',%%1)) as
            select t1.ts,t1.trade_no,t1.charged_time,t1.pile_kw,t4.percent_threshold,
                t1.soc curr_soc,t1.prev_soc,abs(t1.soc-t1.prev_soc) diff_soc,
                case when t4.percent_threshold is null then 0 else
                    case when abs(t1.soc-t1.prev_soc)>t4.percent_threshold
                    then 1 else 0 end end flag
            from (select _twend ts,max(case when soc!=0 then 1 else 0 end) has_nonzero_soc
                from %%tbname where ts>=w_order._twstart and ts<=_twstart) t0
            inner join (select _twend ts,last(soc) soc,last(trade_no) trade_no,
                last(charged_time) charged_time,last(pile_kw) pile_kw,last(gun_code) gun_code,
                last(pile_code) pile_code,first(soc) prev_soc,first(trade_no) prev_trade_no
                from %%tbname where ts>=_twstart and ts<=_twend and _twduration<=30000
                and ifnull(trade_no,'')<>'' and trade_no not like '000000000%'
                and charge_status=3) t1 on t0.ts=t1.ts
            inner join (select _twend ts,pile_code,pile_brand_setting_id,
                station_belong_to station_id from {db}.pile
                where pile_code=%%2 and type<>'DELETE') t2 on t1.ts=t2.ts
            inner join (select _twend ts,pile_brand_setting_id from {db}.brand
                where pile_type=300102 and type<>'DELETE') t3
                on t2.pile_brand_setting_id=t3.pile_brand_setting_id and t2.ts=t3.ts
            inner join (select _twend ts,station_id,percent_threshold from {db}.rule
                where rule_id=20 and type<>'DELETE') t4
                on t2.station_id=t4.station_id and t2.ts=t4.ts
            where t0.has_nonzero_soc>0 and t1.prev_soc is not null
        """

    def _write_customer_soc_rows(self, start_second, count, group_count=2, zero_groups=()):
        db = self.SOC_DB
        parts = []
        for group in range(group_count):
            rows = " ".join(
                "('2025-01-01 00:00:"
                f"{second:02d}','order{group}',3,"
                f"{0 if group in zero_groups else 20 + second},{second},10)"
                for second in range(start_second, start_second + count)
            )
            parts.append(f"{db}.m{group} values {rows}")
        filtered_rows = " ".join(
            "('2025-01-01 00:00:"
            f"{second:02d}','',3,{20 + second},{second},10)"
            for second in range(start_second, start_second + count)
        )
        parts.append(f"{db}.m_filtered values {filtered_rows}")
        tdSql.execute("insert into " + " ".join(parts))

    def _prepare_fixture(self):
        tdSql.execute(f"create database {self.DB} vgroups 2")
        tdSql.execute(
            f"create stable {self.DB}.realtime ("
            "ts timestamp,soc int,trade_no varchar(16)) "
            "tags(pile_code varchar(16),gun_code varchar(16))"
        )
        for index in range(12):
            tdSql.execute(
                f"create table {self.DB}.rt{index} using {self.DB}.realtime "
                f"tags('P_UNUSED_{index}','G_UNUSED_{index}')"
            )

        tdSql.query(
            "select table_name,vgroup_id from information_schema.ins_tables "
            f"where db_name='{self.DB}' and stable_name='realtime' "
            "order by vgroup_id,table_name"
        )
        by_vgroup = {}
        for row in range(tdSql.getRows()):
            table_name = tdSql.getData(row, 0)
            vgroup_id = tdSql.getData(row, 1)
            by_vgroup.setdefault(vgroup_id, []).append(table_name)
        assert len(by_vgroup) >= 2, by_vgroup

        groups = sorted(by_vgroup.items(), key=lambda item: item[0])
        shared_vgroup = next((item for item in groups if len(item[1]) >= 2), None)
        assert shared_vgroup is not None, by_vgroup
        other_vgroup = next(
            (item for item in groups if item[0] != shared_vgroup[0] and item[1]),
            None,
        )
        assert other_vgroup is not None, by_vgroup
        selected = [
            (shared_vgroup[1][0], "P_SHARED", "G_SHARED"),
            (other_vgroup[1][0], "P_ZERO", "G_ZERO"),
            (shared_vgroup[1][1], "P_SHARED", "G_SHARED"),
        ]
        for table_name, pile_code, gun_code in selected:
            tdSql.execute(
                f"alter table {self.DB}.{table_name} set tag pile_code="
                f"'{pile_code}'"
            )
            tdSql.execute(
                f"alter table {self.DB}.{table_name} set tag gun_code="
                f"'{gun_code}'"
            )

        tdSql.execute(
            f"create table {self.DB}.pile_dim ("
            "ts timestamp,pile_code varchar(16),brand_code varchar(16))"
        )
        tdSql.execute(
            f"create table {self.DB}.brand_dim ("
            "ts timestamp,brand_code varchar(16),station_code varchar(16))"
        )
        tdSql.execute(
            f"create table {self.DB}.station_rule ("
            "ts timestamp,rule_id varchar(16) primary key,"
            "station_code varchar(16),threshold int,payload varchar(4093))"
        )
        tdSql.execute(
            f"create table {self.DB}.station_rule_spill ("
            "ts timestamp,rule_id varchar(16) primary key,"
            "station_code varchar(16),threshold int,payload varchar(4093))"
        )
        tdSql.execute(
            f"insert into {self.DB}.pile_dim values "
            "('2026-08-26 00:00:00','P_SHARED','B_SHARED')"
        )
        tdSql.execute(
            f"insert into {self.DB}.brand_dim values "
            "('2026-08-26 00:00:00','B_SHARED','S_SHARED')"
        )
        tdSql.execute(
            f"insert into {self.DB}.station_rule values "
            "('2026-08-26 00:00:00','R_DUP_1','S_SHARED',5,'normal') "
            "('2026-08-26 00:00:00','R_DUP_2','S_SHARED',5,'normal') "
            "('2026-08-26 00:00:00','R_NULL','S_SHARED',null,'normal')"
        )
        self._write_spill_rules()
        return selected

    def _create_normal_streams(self, tables):
        grouped_query = self._five_leaf_query("%%tbname", pile_filter="pile_code = %%2")
        tdSql.execute(
            f"create stream {self.DB}.s_normal_grouped window("
            "state_window(trade_no) extend(1) as w_order,count_window(2,1)) "
            f"from {self.DB}.realtime partition by tbname,pile_code,gun_code "
            f"into {self.DB}.r_normal_grouped "
            "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,threshold,"
            "flag,payload_len) tags(base_table varchar(64) as %%1) as "
            f"{grouped_query}"
        )
        for table_name, _, _ in tables:
            baseline_query = self._five_leaf_query(f"{self.DB}.{table_name}")
            tdSql.execute(
                f"create stream {self.DB}.s_normal_{table_name} window("
                "state_window(trade_no) extend(1) as w_order,"
                "count_window(2,1)) "
                f"from {self.DB}.{table_name} into "
                f"{self.DB}.r_normal_{table_name} "
                "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,"
                "threshold,flag,payload_len) as "
                f"{baseline_query}"
            )

    def _create_force_output_streams(self, tables):
        grouped_query = self._five_leaf_query("%%tbname", pile_filter="pile_code = %%2")
        tdSql.execute(
            f"create stream {self.DB}.s_force_grouped window("
            "state_window(trade_no) extend(1) as w_order,count_window(2,1)) "
            f"from {self.DB}.realtime partition by tbname,pile_code,gun_code "
            "stream_options(force_output) "
            f"into {self.DB}.r_force_grouped "
            "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,threshold,"
            "flag,payload_len) tags(base_table varchar(64) as %%1) as "
            f"{grouped_query}"
        )
        for table_name, _, _ in tables:
            baseline_query = self._five_leaf_query(f"{self.DB}.{table_name}")
            tdSql.execute(
                f"create stream {self.DB}.s_force_{table_name} window("
                "state_window(trade_no) extend(1) as w_order,"
                "count_window(2,1)) "
                f"from {self.DB}.{table_name} stream_options(force_output) "
                f"into {self.DB}.r_force_{table_name} "
                "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,"
                "threshold,flag,payload_len) as "
                f"{baseline_query}"
            )

    def _create_fallback_stream(self):
        fallback_query = self._five_leaf_query("%%tbname", order_by=True)
        tdSql.execute(
            f"create stream {self.DB}.s_fallback_grouped window("
            "state_window(trade_no) extend(1) as w_order,count_window(2,1)) "
            f"from {self.DB}.realtime partition by tbname,pile_code,gun_code "
            f"into {self.DB}.r_fallback_grouped "
            "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,threshold,"
            "flag,payload_len) tags(base_table varchar(64) as %%1) as "
            f"{fallback_query}"
        )

    def _create_ordered_leaf_fallback_stream(self):
        query = (
            "select a.ts,a.v0,b.v1,c.v2 from "
            f"(select _twstart ts,max(soc) v0 from {self.DB}.realtime "
            "where ts>=_twstart and ts<_twend) a join "
            f"(select _twstart ts,max(soc) v1 from {self.DB}.realtime "
            "where ts>=_twstart and ts<_twend) b on a.ts=b.ts join "
            f"(select _twstart ts,max(soc) v2 from {self.DB}.realtime "
            "where ts>=_twstart and ts<_twend order by ts) c on b.ts=c.ts"
        )
        tdSql.execute(
            f"create stream {self.DB}.s_ordered_leaf_fallback "
            "interval(10s) sliding(10s) "
            f"from {self.DB}.realtime partition by tbname "
            f"into {self.DB}.r_ordered_leaf_fallback "
            "(ts,v0,v1,v2) tags(base_table varchar(64) as %%1) as "
            f"{query}"
        )

    def _create_spill_streams(self, tables):
        grouped_query = self._five_leaf_query(
            "%%tbname", station_rule="station_rule_spill", pile_filter="pile_code = %%2"
        )
        tdSql.execute(
            f"create stream {self.DB}.s_spill_grouped window("
            "state_window(trade_no) extend(1) as w_order,count_window(2,1)) "
            f"from {self.DB}.realtime partition by tbname,pile_code,gun_code "
            f"into {self.DB}.r_spill_grouped "
            "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,threshold,"
            "flag,payload_len) tags(base_table varchar(64) as %%1) as "
            f"{grouped_query}"
        )
        for table_name, _, _ in tables:
            baseline_query = self._five_leaf_query(
                f"{self.DB}.{table_name}", station_rule="station_rule_spill"
            )
            tdSql.execute(
                f"create stream {self.DB}.s_spill_{table_name} window("
                "state_window(trade_no) extend(1) as w_order,"
                "count_window(2,1)) "
                f"from {self.DB}.{table_name} into "
                f"{self.DB}.r_spill_{table_name} "
                "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,"
                "threshold,flag,payload_len) as "
                f"{baseline_query}"
            )

    def _five_leaf_query(
        self,
        realtime_table,
        station_rule="station_rule",
        order_by=False,
        pile_filter="pile_code is not null",
    ):
        order_clause = " order by t1.sample_ts" if order_by else ""
        dimension_where = f"where {pile_filter}"
        return (
            "select t1.sample_ts,t4.rule_id,t1.trade_no,t1.curr_soc,"
            "t1.prev_soc,t4.threshold,"
            "case when t4.threshold is null then null "
            "when abs(t1.curr_soc-t1.prev_soc)>t4.threshold then 1 else 0 end,"
            "length(t4.payload) from "
            "(select _twstart join_ts,last(pile_code) pile_code,"
            f"max(soc) has_nonzero_soc from {realtime_table} "
            "where ts>=w_order._twstart and ts<=_twstart) t0 "
            "join (select _twstart join_ts,last(ts) sample_ts,"
            "last(pile_code) pile_code,last(trade_no) trade_no,"
            "last(soc) curr_soc,first(soc) prev_soc "
            f"from {realtime_table} where ts>=_twstart and ts<=_twend "
            "and _twduration<=30000) t1 "
            "on t0.join_ts=t1.join_ts and t0.pile_code=t1.pile_code "
            "join (select _twstart join_ts,pile_code,brand_code "
            f"from {self.DB}.pile_dim {dimension_where}) t2 "
            "on t1.join_ts=t2.join_ts and t1.pile_code=t2.pile_code "
            "join (select _twstart join_ts,brand_code,station_code "
            f"from {self.DB}.brand_dim where brand_code='B_SHARED') t3 "
            "on t2.join_ts=t3.join_ts and t2.brand_code=t3.brand_code "
            "join (select _twstart join_ts,rule_id,station_code,threshold,"
            f"payload from {self.DB}.{station_rule} "
            "where station_code='S_SHARED') t4 "
            "on t3.join_ts=t4.join_ts and t3.station_code=t4.station_code "
            "where t0.has_nonzero_soc>0 and t1.prev_soc is not null"
            f"{order_clause}"
        )

    def _write_spill_rules(self):
        payload = "x" * 4093
        batch_size = 128
        for start in range(0, self.SPILL_RULES, batch_size):
            rows = []
            for rule in range(start, min(start + batch_size, self.SPILL_RULES)):
                rows.append(
                    "('2026-08-26 00:00:00',"
                    f"'SPILL_{rule:04d}','S_SHARED',5,'{payload}')"
                )
            tdSql.execute(
                f"insert into {self.DB}.station_rule_spill values "
                + " ".join(rows)
            )

    def _write_trigger_rows(self, tables):
        values = (
            "('2026-08-26 01:00:00',0,'TRADE_A') "
            "('2026-08-26 01:00:10',0,'TRADE_A') "
            "('2026-08-26 01:00:20',10,'TRADE_A') "
            "('2026-08-26 01:00:30',10,'TRADE_A') "
            "('2026-08-26 01:00:40',14,'TRADE_A') "
            "('2026-08-26 01:01:20',30,'TRADE_A') "
            "('2026-08-26 01:02:00',0,'TRADE_B') "
            "('2026-08-26 01:02:10',0,'TRADE_B') "
            "('2026-08-26 01:02:20',5,'TRADE_B') "
            "('2026-08-26 01:02:30',5,'TRADE_B') "
            "('2026-08-26 01:02:40',20,'TRADE_B') "
            "('2026-08-26 01:03:20',20,'TRADE_B')"
        )
        tdSql.execute(
            "insert into "
            + " ".join(
                f"{self.DB}.{table_name} values {values}"
                for table_name, _, _ in tables
            )
        )

    def _assert_singleton_grouped_result(self, table_name):
        grouped_sql = (
            "select base_table,ts,trade_no,curr_soc,prev_soc,threshold,flag "
            f"from {self.DB}.r_normal_grouped"
        )
        baseline_sql = (
            "select ts,trade_no,curr_soc,prev_soc,threshold,flag "
            f"from {self.DB}.r_normal_{table_name}"
        )
        self._wait_rows(grouped_sql, 12)
        self._wait_rows(baseline_sql, 12)
        grouped = self._query_rows(grouped_sql)
        assert {row[0] for row in grouped} == {table_name}, grouped
        baseline = [(table_name, *row) for row in self._query_rows(baseline_sql)]
        assert self._sorted_rows(grouped) == self._sorted_rows(baseline), (
            grouped,
            baseline,
        )

    def _assert_normal_results(self, tables):
        grouped_sql = (
            "select base_table,ts,trade_no,curr_soc,prev_soc,threshold,flag "
            f"from {self.DB}.r_normal_grouped"
        )
        self._wait_rows(grouped_sql, 24)
        grouped = self._sorted_rows(self._query_rows(grouped_sql))

        grouped_by_table = {
            table_name: [row[1:] for row in grouped if row[0] == table_name]
            for table_name, _, _ in tables
        }
        baseline = []
        for table_name, pile_code, _ in tables:
            baseline_sql = (
                "select ts,trade_no,curr_soc,prev_soc,threshold,flag "
                f"from {self.DB}.r_normal_{table_name}"
            )
            expected_rows = 12 if pile_code == "P_SHARED" else 0
            if expected_rows == 0:
                tdSql.query(f"select count(*) from {self.DB}.{table_name}")
                assert tdSql.getData(0, 0) == 12, table_name
                tdSql.query(baseline_sql)
                assert tdSql.getRows() == 0, self._query_rows(baseline_sql)
                assert grouped_by_table[table_name] == [], grouped_by_table
                continue
            self._wait_rows(baseline_sql, expected_rows)
            child_baseline = self._query_rows(baseline_sql)
            assert self._sorted_rows(grouped_by_table[table_name]) == self._sorted_rows(
                child_baseline
            ), (table_name, grouped_by_table[table_name], child_baseline)
            baseline.extend((table_name, *row) for row in child_baseline)
        baseline = self._sorted_rows(baseline)
        assert grouped == baseline, (grouped, baseline)

        first_table = tables[0][0]
        final_table = tables[-1][0]
        assert len(grouped_by_table[first_table]) == 12, grouped_by_table
        assert len(grouped_by_table[final_table]) == 12, grouped_by_table
        assert self._sorted_rows(grouped_by_table[first_table]) == self._sorted_rows(
            grouped_by_table[final_table]
        ), grouped_by_table

        shared_tables = {
            table_name
            for table_name, pile_code, _ in tables
            if pile_code == "P_SHARED"
        }
        assert {row[0] for row in grouped} == shared_tables, grouped
        assert all(row[4] is not None for row in grouped), grouped
        assert all(row[2] in ("TRADE_A", "TRADE_B") for row in grouped), grouped
        assert all(row[3] != 30 for row in grouped), grouped
        assert all(
            str(row[1])
            not in ("2026-08-26 01:01:20", "2026-08-26 01:03:20")
            for row in grouped
        ), grouped

        per_table_pairs = {}
        for row in grouped:
            per_table_pairs.setdefault(row[0], set()).add((row[2], row[3], row[4]))
        expected_pairs = {
            ("TRADE_A", 10, 10),
            ("TRADE_A", 14, 10),
            ("TRADE_B", 5, 5),
            ("TRADE_B", 20, 5),
        }
        assert all(
            pairs == expected_pairs for pairs in per_table_pairs.values()
        ), per_table_pairs

        threshold_five = [row for row in grouped if row[5] == 5]
        threshold_null = [row for row in grouped if row[5] is None]
        assert len(threshold_five) == 16, threshold_five
        assert len(threshold_null) == 8, threshold_null
        assert all(row[6] is None for row in threshold_null), threshold_null
        assert sum(row[6] == 1 for row in threshold_five) == 4, threshold_five
        assert sum(row[6] == 0 for row in threshold_five) == 12, threshold_five

        duplicate_counts = {}
        for row in threshold_five:
            duplicate_counts[row] = duplicate_counts.get(row, 0) + 1
        assert set(duplicate_counts.values()) == {2}, duplicate_counts

        final_threshold_five = [
            row for row in grouped if row[0] == final_table and row[5] == 5
        ]
        final_duplicate_counts = {}
        for row in final_threshold_five:
            final_duplicate_counts[row[1:]] = (
                final_duplicate_counts.get(row[1:], 0) + 1
            )
        assert len(final_threshold_five) == 8, final_threshold_five
        assert set(final_duplicate_counts.values()) == {2}, final_duplicate_counts

    def _assert_force_output_results(self, tables):
        grouped_sql = (
            "select base_table,ts,trade_no,curr_soc,prev_soc,threshold,flag "
            f"from {self.DB}.r_force_grouped"
        )
        self._wait_count(grouped_sql, 24)
        grouped = self._sorted_rows(self._query_rows(grouped_sql))

        baseline = []
        for table_name, pile_code, _ in tables:
            baseline_sql = (
                "select ts,trade_no,curr_soc,prev_soc,threshold,flag "
                f"from {self.DB}.r_force_{table_name}"
            )
            expected_rows = 12 if pile_code == "P_SHARED" else 0
            self._wait_count(baseline_sql, expected_rows)
            baseline.extend(
                (table_name, *row) for row in self._query_rows(baseline_sql)
            )
        assert grouped == self._sorted_rows(baseline), (grouped, baseline)

        zero_table = next(
            table_name
            for table_name, pile_code, _ in tables
            if pile_code == "P_ZERO"
        )
        assert not any(row[0] == zero_table for row in grouped), grouped

    def _assert_fallback_result(self, tables):
        fallback_sql = (
            "select base_table,ts,trade_no,curr_soc,prev_soc,threshold,flag "
            f"from {self.DB}.r_fallback_grouped"
        )
        self._wait_count(fallback_sql, 24)
        fallback = self._sorted_rows(self._query_rows(fallback_sql))
        baseline = []
        for table_name, _, _ in tables:
            baseline_sql = (
                "select ts,trade_no,curr_soc,prev_soc,threshold,flag "
                f"from {self.DB}.r_normal_{table_name}"
            )
            baseline.extend(
                (table_name, *row) for row in self._query_rows(baseline_sql)
            )
        assert fallback == self._sorted_rows(baseline), (fallback, baseline)

    def _assert_ordered_leaf_fallback_result(self, tables):
        result_sql = (
            "select base_table,ts,v0,v1,v2 "
            f"from {self.DB}.r_ordered_leaf_fallback"
        )
        closed_windows_per_table = 11
        self._wait_count(result_sql, len(tables) * closed_windows_per_table)
        result = self._sorted_rows(self._query_rows(result_sql))

        baseline = []
        for table_name, _, _ in tables:
            rows = self._query_rows(
                f"select ts,soc from {self.DB}.{table_name} "
                "where ts < '2026-08-26 01:03:20'"
            )
            baseline.extend(
                (table_name, ts, soc, soc, soc) for ts, soc in rows
            )
        baseline = self._sorted_rows(baseline)
        assert result == baseline, (result, baseline)

    def _assert_spill_results(self, tables):
        per_shared_table = 4 * self.SPILL_RULES
        grouped_expected = 2 * per_shared_table
        grouped_sql = (
            "select base_table,ts,trade_no,curr_soc,prev_soc,rule_id,"
            f"threshold,payload_len from {self.DB}.r_spill_grouped"
        )
        self._wait_count(grouped_sql, grouped_expected)
        grouped = self._sorted_rows(self._query_rows(grouped_sql))

        baseline = []
        for table_name, pile_code, _ in tables:
            baseline_sql = (
                "select ts,trade_no,curr_soc,prev_soc,rule_id,threshold,"
                f"payload_len from {self.DB}.r_spill_{table_name}"
            )
            expected_rows = per_shared_table if pile_code == "P_SHARED" else 0
            self._wait_count(baseline_sql, expected_rows)
            baseline.extend(
                (table_name, *row) for row in self._query_rows(baseline_sql)
            )
        baseline = self._sorted_rows(baseline)
        assert grouped == baseline, (len(grouped), len(baseline))
        assert all(row[6] == 5 and row[7] == 4093 for row in grouped), grouped[:10]

    def _wait_rows(self, sql, expected):
        tdSql.checkResultsByFunc(
            sql=sql,
            func=lambda: tdSql.getRows() == expected,
            retry=self.RETRY,
        )

    def _wait_count(self, sql, expected):
        tdSql.checkResultsByFunc(
            sql=f"select count(*) from ({sql})",
            func=lambda: tdSql.getData(0, 0) == expected,
            retry=self.RETRY,
        )

    def _wait_customer_soc_publications(
        self, result_sql, expected_rows, expected_tables
    ):
        count_sql = f"select count(*) from ({result_sql})"
        table_sql = (
            "select distinct tbname from "
            f"{self.SOC_DB}.result order by tbname"
        )

        def observe():
            count_rows = self._query_rows(count_sql)
            count = count_rows[0][0] if len(count_rows) == 1 else None
            return count, self._query_rows(result_sql), self._query_rows(table_sql)

        deadline = time.monotonic() + self.RETRY
        actual = (None, [], [])
        while time.monotonic() < deadline:
            try:
                actual = observe()
            except Exception:
                actual = (None, [], [])
            if actual == (len(expected_rows), expected_rows, expected_tables):
                break
            time.sleep(0.5)
        else:
            raise AssertionError(
                "unexpected customer SOC publications: "
                f"count={actual[0]!r}, rows={actual[1]!r}, "
                f"tables={actual[2]!r}"
            )

        stable_deadline = time.monotonic() + self.STABLE_SECONDS
        while time.monotonic() < stable_deadline:
            time.sleep(0.5)
            stable = observe()
            if stable != (len(expected_rows), expected_rows, expected_tables):
                raise AssertionError(
                    "customer SOC publications changed during stable period: "
                    f"count={stable[0]!r}, rows={stable[1]!r}, "
                    f"tables={stable[2]!r}"
                )
        tdLog.info(
            "stable customer SOC publications: "
            f"count={actual[0]!r}, rows={actual[1]!r}, "
            f"tables={actual[2]!r}"
        )

    @staticmethod
    def _query_rows(sql):
        tdSql.query(sql)
        return [
            tuple(tdSql.getData(row, col) for col in range(tdSql.getCols()))
            for row in range(tdSql.getRows())
        ]

    @staticmethod
    def _sorted_rows(rows):
        return sorted(
            rows,
            key=lambda row: tuple(
                "" if value is None else str(value) for value in row
            ),
        )
