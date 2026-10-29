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

"""PRE_FILTER on a virtual-table TAG must not prune every virtual table.

A virtual child table stores a TAG value that is a reference
(``TAGS (... from db.tb.tag)``) as NULL in its own tag blob; the reference is
only resolved at query time. The stream reader builds its table list by
filtering the virtual super table's children on their *local* tag values, so a
PRE_FILTER over such a referenced TAG matches no child at all: the reader ends
up with an empty table list, VTABLE_INFO resolves no vtable, and the stream
silently produces no output (the reader even reports itself as ready because
"no source table is referenced").

The ``literal`` parameter is the control: the same stream over literal TAG
values works, which isolates the defect to the referenced TAG.
"""

import time

import pytest

from new_test_framework.utils import tdCom, tdLog, tdSql, tdStream


class TestStreamVtableTagRefPreFilter:
    precision = "ms"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @pytest.mark.parametrize("tag_source", ["literal", "ref"])
    def test_prefilter_on_vtable_tag(self, tag_source):
        """Stream: PRE_FILTER over a virtual TAG reference still reads source data

        Create a virtual super table whose TAG is either a literal or a TAG
        reference (``from cfg.model``), let two virtual child tables map to two
        physical tables with different models, and create an event-window
        stream with ``pre_filter(`产品型号Tag` in (...))`` that matches only the
        first model. Only the matching vtable may contribute a window; the
        stream must produce data instead of idling with an empty table list.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.6

        Labels: common,ci,stream,vtable

        Feishu: None

        History:
            - 2026-10-27 Created. The reader's table list extraction evaluates
              ``pre_filter`` against the virtual child's local tag blob, where a
              TAG reference is stored as NULL, so every vtable is pruned and the
              stream silently emits nothing.
        """
        db = f"vtb_pf_{tag_source}"
        src_db = f"{db}_src"
        stream = "s_pf"

        tdCom.create_snode_if_not_exists()
        try:
            tdSql.execute(f"alter all dnodes 'debugflag 135'")
            tdSql.execute(f"create database {src_db} vgroups 1")
            tdSql.execute(f"create database {db} vgroups 1")
            tdSql.execute(f"use {db}")

            # TAG reference source: the model names live in a config table's TAG.
            tdSql.execute("create stable cfg_st (ts timestamp, dummy int) tags (model varchar(64))")
            tdSql.execute("create table cfg_match using cfg_st tags ('2Qe7C9')")
            tdSql.execute("create table cfg_other using cfg_st tags ('ZZZ9')")
            tdSql.execute("insert into cfg_match values (now, 0)")
            tdSql.execute("insert into cfg_other values (now, 0)")

            # Physical layer read by the virtual child tables.
            tdSql.execute(
                f"create stable {src_db}.meters (ts timestamp, v double, st tinyint) tags (dev varchar(32))"
            )
            tdSql.execute(f"create table {src_db}.well1 using {src_db}.meters tags ('W-1')")
            tdSql.execute(f"create table {src_db}.well2 using {src_db}.meters tags ('W-2')")

            tdSql.execute(
                "create stable vst (ts timestamp, v double, st tinyint) "
                "tags (`产品型号Tag` varchar(64), `设备ID_Tag` varchar(64), `整机SN_Tag` varchar(64)) virtual 1"
            )
            if tag_source == "ref":
                match_tag = "from cfg_match.model"
                other_tag = "from cfg_other.model"
            else:
                match_tag = "'2Qe7C9'"
                other_tag = "'ZZZ9'"
            tdSql.execute(
                f"create vtable vt_match (v from {src_db}.well1.v, st from {src_db}.well1.st) "
                f"using vst tags ({match_tag}, 'DEV-1', 'SN-1')"
            )
            tdSql.execute(
                f"create vtable vt_other (v from {src_db}.well2.v, st from {src_db}.well2.st) "
                f"using vst tags ({other_tag}, 'DEV-2', 'SN-2')"
            )

            tdSql.execute(
                f"create stream {stream} event_window(start with st > 0 end with st <= 0) "
                "from vst partition by tbname, `设备ID_Tag` "
                "stream_options(pre_filter(`产品型号Tag` in ('2Qe7C9', '5fG27j')) | ignore_disorder) "
                "into res tags(`设备ID_Tag` varchar(32) as `设备ID_Tag`) as "
                "select _twstart as ws, count(*) as n from %%tbname "
                "where ts >= _twstart and ts < _twend",
                queryTimes=1,
            )
            tdStream.checkStreamStatus(stream)

            ts = int(time.time() * 1000) // 1000 * 1000
            # Two rows inside the window [ts, ts + 2000) for the matching model...
            tdSql.execute(f"insert into {src_db}.well1 values ({ts}, 100.5, 1) ({ts + 1000}, 100.6, 1)")
            tdSql.execute(f"insert into {src_db}.well1 values ({ts + 2000}, 100.7, 0)")
            # ...and the same shape for a model rejected by PRE_FILTER.
            tdSql.execute(f"insert into {src_db}.well2 values ({ts}, 200.5, 1) ({ts + 1000}, 200.6, 1)")
            tdSql.execute(f"insert into {src_db}.well2 values ({ts + 2000}, 200.7, 0)")

            # Only vt_match may fire: one window with 2 rows. If the tag reference is
            # pruned away, no window is produced at all (the reported symptom).
            tdSql.checkResultsByArray(f"select sum(n) from {db}.res", [(2,)], retry=60)
            tdSql.checkResultsByArray(f"select count(*) from {db}.res", [(1,)], retry=1)
        finally:
            tdSql.execute(f"drop stream if exists {db}.{stream}")
            tdSql.execute(f"drop database if exists {db} force")
            tdSql.execute(f"drop database if exists {src_db} force")

    def test_tag_filter_cache_bypasses_ref_tag_update(self):
        """Tag filter cache must not serve a vtable list built from a ref TAG

        A virtual child table stores a referenced TAG (``TAGS (... from cfg.model)``)
        as NULL and resolves it at query time, so the literal value lives in the
        source table. Updating a source TAG only invalidates the caches of the source
        stable, never the virtual stable's tag filter cache. With ``tagFilterCache=1``
        the cached child list must therefore not be reused: the same condition has to
        match the children selected by the *new* TAG values.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.6

        Labels: common,ci,vtable

        Feishu: None

        History:
            - 2026-10-27 Created. A cached vtable list survived a source TAG update,
              so the same condition kept matching the old children. The check goes
              through a stream reader because that is the path that serves its table
              list from the tag filter cache.
        """
        db = "vtb_cache_ref"
        src_db = "vtb_cache_src"
        s1, s2 = "s_cache1", "s_cache2"

        # Remember each dnode's value so the case never leaves a modified setting behind.
        # SHOW DNODE <id> VARIABLES projects (dnode_id, name, value, scope, ...): the value
        # is at index 2, the last column is "reboot". ALTER expects 0/1.
        orig_cache = []
        try:
            tdSql.query("show dnodes")
            for row in tdSql.queryResult:
                dnode = row[0]
                tdSql.query(f"show dnode {dnode} variables like 'tagFilterCache'")
                if tdSql.queryRows > 0:
                    raw = str(tdSql.queryResult[0][2]).strip().lower()
                    orig_cache.append((dnode, "1" if raw in ("1", "true") else "0"))
        except Exception:
            orig_cache = []

        tdCom.create_snode_if_not_exists()
        tdSql.execute("alter all dnodes 'tagFilterCache 1'")
        try:
            tdSql.execute(f"create database {src_db} vgroups 1")
            tdSql.execute(f"create database {db} vgroups 1")
            tdSql.execute(f"use {db}")

            # Reference sources: the model names live in a config table's TAG.
            tdSql.execute("create stable cfg_st (ts timestamp, dummy int) tags (model varchar(64))")
            tdSql.execute("create table cfg_a using cfg_st tags ('A')")
            tdSql.execute("create table cfg_b using cfg_st tags ('B')")
            tdSql.execute("insert into cfg_a values (now, 0)")
            tdSql.execute("insert into cfg_b values (now, 0)")

            # Physical layer read by the virtual child tables.
            tdSql.execute(
                f"create stable {src_db}.meters (ts timestamp, v double, st tinyint) tags (dev varchar(32))"
            )
            tdSql.execute(f"create table {src_db}.well1 using {src_db}.meters tags ('W-1')")
            tdSql.execute(f"create table {src_db}.well2 using {src_db}.meters tags ('W-2')")

            tdSql.execute(
                "create stable vst (ts timestamp, v double, st tinyint) "
                "tags (`型号Tag` varchar(64), `设备Tag` varchar(64)) virtual 1"
            )
            tdSql.execute(
                f"create vtable vt_a (v from {src_db}.well1.v, st from {src_db}.well1.st) "
                f"using vst tags (from cfg_a.model, 'DEV-1')"
            )
            tdSql.execute(
                f"create vtable vt_b (v from {src_db}.well2.v, st from {src_db}.well2.st) "
                f"using vst tags (from cfg_b.model, 'DEV-2')"
            )

            stream_sql = (
                "event_window(start with st > 0 end with st <= 0) "
                "from vst partition by tbname, `设备Tag` "
                "stream_options(pre_filter(`型号Tag`='A') | ignore_disorder) "
                "into {res} tags(`设备Tag` varchar(32) as `设备Tag`) as "
                "select _twstart as ws, count(*) as n from %%tbname "
                "where ts >= _twstart and ts < _twend"
            )

            # First stream matches vt_a and makes the reader cache its table list
            # ({vt_a}) under the condition digest.
            tdSql.execute(f"create stream {s1} " + stream_sql.format(res="res1"), queryTimes=1)
            tdStream.checkStreamStatus(s1)

            ts = int(time.time() * 1000) // 1000 * 1000
            tdSql.execute(f"insert into {src_db}.well1 values ({ts}, 100.5, 1) ({ts + 1000}, 100.6, 1)")
            tdSql.execute(f"insert into {src_db}.well1 values ({ts + 2000}, 100.7, 0)")
            tdSql.checkResultsByArray(f"select sum(n) from {db}.res1", [(2,)], retry=60)

            # Point the reference sources at the opposite models: only vt_b matches now.
            tdSql.execute("alter table cfg_a set tag model='B'")
            tdSql.execute("alter table cfg_b set tag model='A'")

            # Second stream with the very same condition. A reused cached list would
            # keep only vt_a as its source, so vt_b's window below never fires.
            tdSql.execute(f"create stream {s2} " + stream_sql.format(res="res2"), queryTimes=1)
            tdStream.checkStreamStatus(s2)

            ts2 = ts + 60000
            tdSql.execute(f"insert into {src_db}.well2 values ({ts2}, 200.5, 1) ({ts2 + 1000}, 200.6, 1)")
            tdSql.execute(f"insert into {src_db}.well2 values ({ts2 + 2000}, 200.7, 0)")
            tdSql.checkResultsByArray(f"select sum(n) from {db}.res2", [(2,)], retry=60)
            tdSql.checkResultsByArray(f"select count(*) from {db}.res2", [(1,)], retry=1)
        finally:
            tdSql.execute(f"drop stream if exists {db}.{s1}")
            tdSql.execute(f"drop stream if exists {db}.{s2}")
            tdSql.execute(f"drop database if exists {db} force")
            tdSql.execute(f"drop database if exists {src_db} force")
            if orig_cache:
                for dnode, val in orig_cache:
                    tdSql.execute(f"alter dnode {dnode} 'tagFilterCache' '{val}'")
            else:
                tdSql.execute("alter all dnodes 'tagFilterCache 0'")

    def test_prefilter_on_json_literal_tag_does_not_crash(self):
        """A JSON literal TAG must not reach the cid-based ref resolver

        A virtual super table may declare a single JSON TAG. The chain resolver reads
        literal tags by cid, but a JSON STag is keyed by inner JSON key: ``tTagGet``
        switches to ``tTagValJsonCmprFn`` and would read the cid union member as a
        ``pKey`` pointer. Building the table list for a stream whose ``pre_filter``
        touches such a TAG must keep the children's original STag instead.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.6

        Labels: common,ci,stream,vtable

        Feishu: None

        History:
            - 2026-10-27 Created. The filter path handed the JSON TAG cid to the ref
              resolver, so the reader crashed while building its table list.
        """
        db = "vtb_json_tag"
        src_db = "vtb_json_tag_src"
        stream = "s_json"
        tdCom.create_snode_if_not_exists()
        try:
            tdSql.execute(f"create database {src_db} vgroups 1")
            tdSql.execute(f"create database {db} vgroups 1")
            tdSql.execute(f"use {db}")

            tdSql.execute(
                f"create stable {src_db}.meters (ts timestamp, v double, st tinyint) tags (dev varchar(32))"
            )
            tdSql.execute(f"create table {src_db}.well1 using {src_db}.meters tags ('W-1')")
            tdSql.execute(f"insert into {src_db}.well1 values (now, 1.0, 1)")

            # A virtual super table with a single JSON TAG is allowed by the DDL checks.
            tdSql.execute("create stable vst (ts timestamp, v double, st tinyint) tags (j json) virtual 1")
            json_tag = '\'{"k":1}\''
            tdSql.execute(
                f"create vtable vt_j (v from {src_db}.well1.v, st from {src_db}.well1.st) "
                f"using vst tags ({json_tag})"
            )

            tdSql.execute(
                f"create stream {stream} event_window(start with st > 0 end with st <= 0) "
                "from vst partition by tbname "
                "stream_options(pre_filter(j->'k'=2) | ignore_disorder) "
                "into resj as select _twstart as ws, count(*) as n from %%tbname "
                "where ts >= _twstart and ts < _twend",
                queryTimes=1,
            )
            tdStream.checkStreamStatus(stream)

            # The condition matches nothing. What matters is that the reader built its
            # table list without crashing, so the cluster still answers new statements.
            ts = int(time.time() * 1000) // 1000 * 1000
            tdSql.execute(f"insert into {src_db}.well1 values ({ts}, 1.0, 1) ({ts + 1000}, 1.0, 0)")
            tdSql.query("show dnodes")
            tdSql.checkResultsByArray(f"select count(*) from {db}.resj", [(0,)], retry=10)
        finally:
            tdSql.execute(f"drop stream if exists {db}.{stream}")
            tdSql.execute(f"drop database if exists {db} force")
            tdSql.execute(f"drop database if exists {src_db} force")

    def test_ref_resolve_failure_fails_grouping(self):
        """A broken TAG reference must fail grouping, not merge the other groups

        The resolver rebuilds the TAG blob of every virtual child and reports the uids
        it could not rebuild. When a grouping caller only logs that error and keeps
        computing, the children whose references still resolve are folded into the same
        NULL group as the broken one -- three models A/B/G silently become one group.
        Grouping on such a referenced TAG must fail instead.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.6

        Labels: common,ci,vtable

        Feishu: None

        History:
            - 2026-10-28 Created. The resolver bailed out at the first unresolved uid
              and both grouping call sites ignored the error, so one broken reference
              merged the remaining groups together.
        """
        db = "vtb_ref_fail"
        src_db = "vtb_ref_fail_src"
        tdCom.create_snode_if_not_exists()
        try:
            tdSql.execute(f"create database {src_db} vgroups 1")
            tdSql.execute(f"create database {db} vgroups 1")
            tdSql.execute(f"use {db}")

            # Reference sources: one of them disappears later.
            tdSql.execute("create stable cfg_st (ts timestamp, dummy int) tags (model varchar(64))")
            for name, model in (("cfg_a", "A"), ("cfg_b", "B"), ("cfg_gone", "G")):
                tdSql.execute(f"create table {name} using cfg_st tags ('{model}')")
                tdSql.execute(f"insert into {name} values (now, 0)")

            tdSql.execute(
                f"create stable {src_db}.meters (ts timestamp, v double, st tinyint) tags (dev varchar(32))"
            )
            for idx in (1, 2, 3):
                tdSql.execute(f"create table {src_db}.well{idx} using {src_db}.meters tags ('W-{idx}')")

            tdSql.execute(
                "create stable vst (ts timestamp, v double, st tinyint) "
                "tags (`型号Tag` varchar(64), `设备Tag` varchar(64)) virtual 1"
            )
            for vtb, well, cfg, dev in (
                ("vt_a", "well1", "cfg_a", "DEV-1"),
                ("vt_b", "well2", "cfg_b", "DEV-2"),
                ("vt_gone", "well3", "cfg_gone", "DEV-3"),
            ):
                tdSql.execute(
                    f"create vtable {vtb} (v from {src_db}.{well}.v, st from {src_db}.{well}.st) "
                    f"using vst tags (from {cfg}.model, '{dev}')"
                )

            # vt_gone's reference can no longer be resolved while vt_a/vt_b still can.
            tdSql.execute("drop table cfg_gone")
            tdSql.error("select `型号Tag`, count(*) from vst group by `型号Tag`")
        finally:
            tdSql.execute(f"drop database if exists {db} force")
            tdSql.execute(f"drop database if exists {src_db} force")
