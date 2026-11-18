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
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from new_test_framework.utils import clusterComCheck, tdLog, tdSql, tdStream


class TestSingleGroupJoinMatrix:
    START = "2025-01-01 00:00:00"
    END = "2025-01-01 00:00:10"
    LEFT = "(select ts,k,v from a where ts >= _twstart and ts < _twend) a"
    RIGHT = "(select ts,k,v from b where ts >= _twstart and ts < _twend) b"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare(self):
        tdStream.dropAllStreamsAndDbs()
        tdSql.prepare(dbname="single_group_join_matrix", vgroups=1)
        clusterComCheck.checkDbReady("single_group_join_matrix")
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode(1)
        tdSql.execute("create table trig (ts timestamp, v int)")
        tdSql.execute("create table a (ts timestamp, k int, v int)")
        tdSql.execute("create table b (ts timestamp, k int, v int)")
        tdSql.execute(
            "insert into a values "
            "('2025-01-01 00:00:01',1,11) ('2025-01-01 00:00:02',1,12) "
            "('2025-01-01 00:00:03',2,13) ('2025-01-01 00:00:04',null,14)"
        )
        tdSql.execute(
            "insert into b values "
            "('2025-01-01 00:00:01',1,21) ('2025-01-01 00:00:05',2,23) "
            "('2025-01-01 00:00:06',3,24) ('2025-01-01 00:00:07',null,25)"
        )
        tdSql.execute(
            "insert into trig values "
            "('2025-01-01 00:00:01',1) ('2025-01-01 00:00:11',1) "
            "('2025-01-01 00:00:21',1)"
        )

    def cases(self):
        return (
            ("m01", "inner join", "a.ts = b.ts", [1121]),
            ("m02", "left join", "a.ts = b.ts", [1121, 1200, 1300, 1400]),
            ("m03", "right join", "a.ts = b.ts", [23, 24, 25, 1121]),
            ("m04", "full join", "a.ts = b.ts", [23, 24, 25, 1121, 1200, 1300, 1400]),
            ("m05", "inner join", "a.k = b.k", [1121, 1221, 1323]),
            ("m06", "left join", "a.k = b.k", [1121, 1221, 1323, 1400]),
            ("m07", "right join", "a.k = b.k", [24, 25, 1121, 1221, 1323]),
            ("m08", "full join", "a.k = b.k", [24, 25, 1121, 1221, 1323, 1400]),
            ("m09", "left semi join", "a.ts = b.ts", [11]),
            ("m10", "right semi join", "a.ts = b.ts", [21]),
            ("m11", "left anti join", "a.ts = b.ts", [12, 13, 14]),
            ("m12", "right anti join", "a.ts = b.ts", [23, 24, 25]),
            ("m13", "left semi join", "a.k = b.k", [11, 12, 13]),
            ("m14", "right semi join", "a.k = b.k", [21, 23]),
            ("m15", "left anti join", "a.k = b.k", [14]),
            ("m16", "right anti join", "a.k = b.k", [24, 25]),
            ("m17", "cross join", None, self.cross_keys()),
            ("m18", ",", None, self.cross_keys()),
            ("m19", "inner join", "a.v + 10 < b.v", [1123, 1124, 1125, 1223, 1224, 1225, 1324, 1325, 1425]),
            ("m20", "left join", "a.v + 10 < b.v", [1123, 1124, 1125, 1223, 1224, 1225, 1324, 1325, 1425]),
            ("m21", "right join", "a.v + 10 < b.v", [21, 1123, 1124, 1125, 1223, 1224, 1225, 1324, 1325, 1425]),
            ("m22", "full join", "a.v + 10 < b.v", [21, 1123, 1124, 1125, 1223, 1224, 1225, 1324, 1325, 1425]),
            ("m23", "inner join", "a.k = b.k or a.v = 14", [1121, 1221, 1323, 1421, 1423, 1424, 1425]),
            ("m24", "left join", "a.k = b.k or (a.v = 11 and b.v = 24)", [1121, 1124, 1221, 1323, 1400]),
            ("m25", "right join", "a.k = b.k or (a.v = 11 and b.v = 24)", [25, 1121, 1124, 1221, 1323]),
            ("m26", "full join", "a.k = b.k or (a.v = 11 and b.v = 24)", [25, 1121, 1124, 1221, 1323, 1400]),
            ("m27", "left asof join", "a.ts <= b.ts jlimit 2", [1121, 1123, 1223, 1224, 1323, 1324, 1423, 1424]),
            ("m28", "right asof join", "a.ts <= b.ts jlimit 2", [1121, 1323, 1324, 1325, 1423, 1424, 1425]),
            ("m29", "left window join", "window_offset(-2s, 2s)", [1121, 1221, 1321, 1323, 1423, 1424]),
            ("m30", "right window join", "window_offset(-2s, 2s)", [25, 1121, 1221, 1321, 1323, 1423, 1424]),
        )

    def cross_keys(self):
        return [a * 100 + b for a in (11, 12, 13, 14) for b in (21, 23, 24, 25)]

    def source(self, name, join, condition):
        if name in ("m27", "m28", "m29", "m30"):
            side = "a" if name in ("m27", "m29") else "b"
            where = f"where {side}.ts >= _twstart and {side}.ts < _twend"
            on = f"on {condition}" if "asof" in join else condition
            return f"a a {join} b b {on} {where}"
        if join == ",":
            return f"{self.LEFT}, {self.RIGHT}"
        on = f" on {condition}" if condition else ""
        return f"{self.LEFT} {join} {self.RIGHT}{on}"

    def projection(self, join):
        if "semi" in join or "anti" in join:
            side = "a" if join.startswith("left") else "b"
            return f"{side}.v pair_id, {side}.v av", "(ts, pair_id composite key, av)", "pair_id,av"
        return (
            "cast(coalesce(a.v,0)*100+coalesce(b.v,0) as bigint) pair_id, a.v av, b.v bv",
            "(ts, pair_id composite key, av, bv)",
            "pair_id,av,bv",
        )

    def do_matrix(self):
        cases = self.cases()
        for name, join, condition, _ in cases:
            projection, columns, _ = self.projection(join)
            tdSql.execute(
                f"create stream s_{name} interval(10s) sliding(10s) "
                "from trig stream_options(fill_history) "
                f"into out_{name} {columns} as select _twstart ts, {projection} "
                f"from {self.source(name, join, condition)}"
            )
        for name, join, condition, keys in cases:
            projection, _, output = self.projection(join)
            source = self.source(name, join, condition)
            batch_source = source.replace("_twstart", f"'{self.START}'").replace("_twend", f"'{self.END}'")
            tdSql.query(f"select {projection} from {batch_source} order by pair_id")
            tdSql.checkRows(len(keys))
            expected = list(tdSql.queryResult)
            assert [row[0] for row in expected] == sorted(keys), name
            tdSql.checkResultsByFunc(
                sql=f"select {output} from out_{name} where ts = '{self.START}' order by pair_id",
                func=lambda expected=expected: tdSql.queryResult == expected,
            )
        print("single group JOIN matrix M01-M30 ................... [ passed ]")

    def test_single_group_join_matrix(self):
        """All 30 single group JOIN result sets match fixed rows and batch queries

        1. Assert each fixed JOIN row set, including NULL and duplicate business keys
        2. Compare persisted stream rows with the corresponding batch query
        3. Preserve all 16 CROSS rows under one output timestamp

        Catalog:
            - Streams:SubQuery

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-10 Add M01-M30 single group JOIN result matrix
        """
        self.prepare()
        self.do_matrix()
