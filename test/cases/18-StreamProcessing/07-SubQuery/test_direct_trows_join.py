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

from new_test_framework.utils import clusterComCheck, tdLog, tdSql, tdStream


class TestDirectTrowsJoin:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare(self):
        tdStream.dropAllStreamsAndDbs()
        tdSql.prepare(dbname="direct_trows_join", vgroups=1)
        clusterComCheck.checkDbReady("direct_trows_join")
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode(1)
        tdSql.execute("create table trig (ts timestamp, v int)")
        tdSql.execute("create table rhs (ts timestamp, v int)")
        tdSql.execute(
            "insert into rhs values "
            "('2026-01-01 00:00:01', 1) "
            "('2026-01-01 00:00:02', 2) "
            "('2026-01-01 00:00:03', 3)"
        )
        tdSql.execute(
            "insert into trig values "
            "('2026-01-01 00:00:01', 1) "
            "('2026-01-01 00:00:02', 2) "
            "('2026-01-01 00:00:04', 4)"
        )

    def cases(self):
        return (
            ("inner", "a.ts, a.v, a.v, b.v", "inner join", "on a.v = b.v", 2),
            ("left", "a.ts, a.v, a.v, b.v", "left join", "on a.v = b.v", 3),
            ("right", "b.ts, b.v, a.v, b.v", "right join", "on a.v = b.v", 3),
            ("full", "coalesce(a.ts,b.ts), coalesce(a.v,b.v), a.v, b.v", "full join", "on a.v = b.v", 4),
            ("semi", "a.ts, a.v, a.v", "left semi join", "on a.v = b.v", 2),
            ("anti", "a.ts, a.v, a.v", "left anti join", "on a.v = b.v", 1),
            ("right_semi", "b.ts, b.v, b.v", "right semi join", "on a.v = b.v", 2),
            ("right_anti", "b.ts, b.v, b.v", "right anti join", "on a.v = b.v", 1),
            ("cross", "a.ts, cast(a.v*10+b.v as int), a.v, b.v", "cross join", "", 9),
            ("asof", "a.ts, a.v, a.v, b.v", "left asof join", "on a.ts >= b.ts jlimit 1", 3),
            ("window", "a.ts, cast(a.v*10+b.v as int), a.v, b.v", "left window join", "window_offset(-1s,1s)", 6),
            ("right_asof", "b.ts, b.v, a.v, b.v", "right asof join", "on a.ts <= b.ts jlimit 1", 3),
            ("right_window", "b.ts, cast(a.v*10+b.v as int), a.v, b.v", "right window join", "window_offset(-1s,1s)", 6),
        )

    def do_join_families(self):
        for name, columns, join, condition, _ in self.cases():
            output = "ts, k primary key, av" if "semi" in name or "anti" in name else "ts, k primary key, av, bv"
            tdSql.execute(
                f"create stream s_{name} interval(10s) sliding(10s) "
                "from trig stream_options(fill_history) "
                f"into out_{name} ({output}) as select {columns} "
                f"from %%trows a {join} rhs b {condition}"
            )

        for name, columns, join, condition, count in self.cases():
            tdSql.query(
                f"select {columns} from trig a {join} rhs b {condition} order by 1, 2"
            )
            tdSql.checkRows(count)
            expected = list(tdSql.queryResult)
            tdSql.checkResultsByFunc(
                sql=f"select * from out_{name} order by 1, 2",
                func=lambda expected=expected: tdSql.queryResult == expected,
            )
        print("direct trows JOIN families ......................... [ passed ]")

    def do_join_boundaries(self):
        variants = (
            (
                "post_where",
                "a.ts, a.v, a.v, b.v",
                "%%trows a right join rhs b on a.v = b.v "
                "where a.ts >= _twstart and a.ts < _twend",
                "trig a right join rhs b on a.v = b.v "
                "where a.ts >= '2026-01-01 00:00:00' and a.ts < '2026-01-01 00:00:10'",
                2,
            ),
            (
                "derived",
                "a.ts, a.v, a.v, b.v",
                "%%trows a inner join (select ts,v from rhs) b on a.v = b.v",
                "trig a inner join (select ts,v from rhs) b on a.v = b.v",
                2,
            ),
            (
                "input_right",
                "a.ts, a.v, a.v, b.v",
                "rhs a left join %%trows b on a.v = b.v",
                "rhs a left join trig b on a.v = b.v",
                3,
            ),
            (
                "range",
                "a.ts, cast(a.v*10+b.v as int), a.v, b.v",
                "%%trows a inner join rhs b on a.v < b.v",
                "trig a inner join rhs b on a.v < b.v",
                3,
            ),
            (
                "or_condition",
                "a.ts, cast(a.v*10+b.v as int), a.v, b.v",
                "%%trows a inner join rhs b on a.v = b.v or a.v = 4",
                "trig a inner join rhs b on a.v = b.v or a.v = 4",
                5,
            ),
        )
        for name, columns, source, _, _ in variants:
            tdSql.execute(
                f"create stream s_{name} interval(10s) sliding(10s) "
                "from trig stream_options(fill_history) "
                f"into out_{name} (ts, k primary key, av, bv) "
                f"as select {columns} from {source}"
            )
        for name, columns, _, source, count in variants:
            tdSql.query(f"select {columns} from {source} order by 1, 2")
            tdSql.checkRows(count)
            expected = list(tdSql.queryResult)
            tdSql.checkResultsByFunc(
                sql=f"select * from out_{name} order by 1, 2",
                func=lambda expected=expected: tdSql.queryResult == expected,
            )
        print("direct trows JOIN boundaries ....................... [ passed ]")

    def do_outer_conditions(self):
        variants = (
            ("range", "a.v < b.v", {"left": 4, "right": 4, "full": 5}),
            ("or", "a.v = b.v or a.v = 4", {"left": 5, "right": 5, "full": 5}),
        )
        columns = "_twstart, cast(coalesce(a.v,0)*10+coalesce(b.v,0) as int), a.v, b.v"
        for suffix, condition, joins in variants:
            for join, count in joins.items():
                name = f"{join}_{suffix}"
                tdSql.execute(
                    f"create stream s_{name} interval(10s) sliding(10s) "
                    "from trig stream_options(fill_history) "
                    f"into out_{name} (ts,k composite key,av,bv) as select {columns} "
                    f"from %%trows a {join} join rhs b on {condition}"
                )
                tdSql.query(
                    "select cast(coalesce(a.v,0)*10+coalesce(b.v,0) as int), a.v, b.v "
                    f"from trig a {join} join rhs b on {condition} order by 1"
                )
                tdSql.checkRows(count)
                expected = list(tdSql.queryResult)
                tdSql.checkResultsByFunc(
                    sql=f"select k,av,bv from out_{name} where ts = '2026-01-01 00:00:00' order by k",
                    func=lambda expected=expected: tdSql.queryResult == expected,
                )
        print("direct trows OUTER conditions ..................... [ passed ]")

    def do_cache_side_filters(self):
        tdSql.execute("create table filter_trig (ts timestamp, v int, flag int)")
        tdSql.execute("create table filter_rhs (ts timestamp, v int)")
        tdSql.execute(
            "insert into filter_trig values ('2026-07-01 00:00:01',1,0) "
            "('2026-07-01 00:00:02',2,1) ('2026-07-01 00:00:03',3,NULL)"
        )
        tdSql.execute(
            "insert into filter_rhs values ('2026-07-01 00:00:01',1) "
            "('2026-07-01 00:00:02',2) ('2026-07-01 00:00:03',3) "
            "('2026-07-01 00:00:11',11) ('2026-07-01 00:00:12',12) ('2026-07-01 00:00:13',13)"
        )
        variants = []
        for side in ("left", "right"):
            for outer in (False, True):
                if side == "left":
                    source = f"%%trows a {'left' if outer else 'inner'} join filter_rhs b"
                else:
                    source = f"filter_rhs b {'right' if outer else 'inner'} join %%trows a"
                for clause in ("where", "and"):
                    name = f"filter_{side}_{int(outer)}_{clause}"
                    variants.append((name, f"{source} on a.v=b.v {clause} a.flag>0", outer and clause == "and", False))
                    if not outer:
                        name = f"time_filter_{side}_{clause}"
                        condition = "a.ts>'2026-07-01 00:00:01' and a.ts!='2026-07-01 00:00:11'"
                        variants.append((name, f"{source} on a.v=b.v {clause} {condition}", False, True))
        for name, source, _, _ in variants:
            tdSql.execute(
                f"create stream s_{name} interval(10s) sliding(10s) "
                "from filter_trig stream_options(fill_history) "
                f"into out_{name} (ts,av,bv) as select a.ts,a.v,b.v from {source}"
            )
        for name, _, _, _ in variants:
            tdStream.checkStreamStatus(f"s_{name}")

        for offset in (0, 10):
            if offset:
                tdSql.execute(
                    "insert into filter_trig values ('2026-07-01 00:00:11',11,0) "
                    "('2026-07-01 00:00:12',12,1) ('2026-07-01 00:00:13',13,NULL) "
                    "('2026-07-01 00:00:21',NULL,NULL)"
                )
            start = f"2026-07-01 00:00:{offset:02d}"
            end = f"2026-07-01 00:00:{offset + 10:02d}"
            trigger = f"(select * from filter_trig where ts >= '{start}' and ts < '{end}')"
            for name, source, preserve_unmatched, time_filter in variants:
                batch_source = source.replace("%%trows", trigger)
                tdSql.query(f"select a.ts,a.v,b.v from {batch_source} order by 1")
                expected = list(tdSql.queryResult)
                pairs = [(offset + 2, offset + 2)]
                if preserve_unmatched:
                    pairs = [(offset + 1, None), (offset + 2, offset + 2), (offset + 3, None)]
                elif time_filter:
                    pairs.append((offset + 3, offset + 3))
                assert [row[1:] for row in expected] == pairs, (name, expected)
                tdSql.checkResultsByFunc(
                    sql=f"select ts,av,bv from out_{name} where ts >= '{start}' and ts < '{end}' order by ts",
                    func=lambda expected=expected: tdSql.queryResult == expected,
                )
        print("direct trows cache WHERE/ON filters ............... [ passed ]")

    def do_dynamic_cache_time_filters(self):
        tdSql.execute("create table dynamic_trig (ts timestamp, v int)")
        tdSql.execute("create table dynamic_rhs (ts timestamp, v int)")
        tdSql.execute(
            "insert into dynamic_trig values ('2026-09-01 00:00:01',1) "
            "('2026-09-01 00:00:05',2) ('2026-09-01 00:00:06',3) "
            "('2026-09-01 00:00:08',5) ('2026-09-01 00:00:09',4)"
        )
        # Some matching RHS timestamps lie outside the trigger window; cache bounds must not prune them.
        tdSql.execute(
            "insert into dynamic_rhs values ('2026-09-01 00:00:06',1) "
            "('2026-09-01 00:00:07',2) ('2026-09-01 00:01:00',3) "
            "('2026-09-01 00:01:01',4) ('2026-09-01 00:01:02',6) "
            "('2026-09-01 00:00:16',11) ('2026-09-01 00:00:17',12) "
            "('2026-09-01 00:01:10',13) ('2026-09-01 00:01:11',14) ('2026-09-01 00:01:12',16)"
        )
        variants = []
        for side in ("left", "right"):
            for mode in ("inner", "preserved", "nullable"):
                join = "inner" if mode == "inner" else ("left" if mode == "preserved" else "right")
                if side == "left":
                    source = f"%%trows a {join} join dynamic_rhs b"
                else:
                    join = {"inner": "inner", "left": "right", "right": "left"}[join]
                    source = f"dynamic_rhs b {join} join %%trows a"
                for bound, condition, values in (
                    ("lower", "a.ts>=_twstart+5s and a.ts<_twend", [2, 3, 5, 4] if mode == "preserved" else [2, 3, 4]),
                    ("upper", "a.ts>=_twstart and a.ts<_twend-2s", [1, 2, 3]),
                ):
                    variants.append((f"{side}_{mode}_{bound}", source, condition, values))
            source = "%%trows a join dynamic_rhs b" if side == "left" else "dynamic_rhs b join %%trows a"
            for bound, condition, values in (
                ("strict", "_twstart+5s<a.ts and _twend-2s>=a.ts", [3]),
                ("equal", "_twstart+5s=a.ts", [2]),
                ("mixed", "a.ts>=_twstart+5s and b.ts>=_twstart and b.ts<_twend", [2]),
            ):
                variants.append((f"{side}_{bound}", source, condition, values))
            for mode in ("preserved", "full", "derived"):
                join = {"preserved": "left" if side == "left" else "right",
                        "full": "full", "derived": "inner"}[mode]
                rhs = "(select ts,v from dynamic_rhs)" if mode == "derived" else "dynamic_rhs"
                source = (f"%%trows a {join} join {rhs} b" if side == "left"
                          else f"{rhs} b {join} join %%trows a")
                # Reject both absent RHS rows and matches excluded by its dynamic time bounds.
                for bound, condition, values in (
                    ("lower", "b.ts>=_twstart+7s and b.ts<_twend", [2]),
                    ("upper", "b.ts>=_twstart and b.ts<_twend-3s", [1]),
                    ("strict", "_twstart+6s<b.ts and _twend>b.ts", [2]),
                    ("equal", "_twstart+6s=b.ts", [1]),
                    ("mixed", "a.ts>=_twstart+5s and b.ts>=_twstart and b.ts<_twend", [2]),
                ):
                    variants.append((f"{side}_{mode}_rhs_{bound}", source, condition, values))
        for name, source, condition, _ in variants:
            tdSql.execute(
                f"create stream s_dynamic_{name} interval(10s) sliding(10s) "
                "from dynamic_trig stream_options(fill_history) "
                f"into out_dynamic_{name} (ts,av,bv) as select a.ts,a.v,b.v "
                f"from {source} on a.v=b.v where {condition}"
            )
        for name, _, _, _ in variants:
            tdStream.checkStreamStatus(f"s_dynamic_{name}")

        for offset in (0, 10):
            if offset:
                tdSql.execute(
                    "insert into dynamic_trig values ('2026-09-01 00:00:11',11) "
                    "('2026-09-01 00:00:15',12) ('2026-09-01 00:00:16',13) "
                    "('2026-09-01 00:00:18',15) ('2026-09-01 00:00:19',14) "
                    "('2026-09-01 00:00:21',NULL)"
                )
            start = f"2026-09-01 00:00:{offset:02d}"
            end = f"2026-09-01 00:00:{offset + 10:02d}"
            trigger = f"(select * from dynamic_trig where ts >= '{start}' and ts < '{end}')"
            for name, source, condition, values in variants:
                batch_source = source.replace("%%trows", trigger)
                batch_cond = condition.replace("_twstart", f"cast('{start}' as timestamp)")
                batch_cond = batch_cond.replace("_twend", f"cast('{end}' as timestamp)")
                tdSql.query(f"select a.ts,a.v,b.v from {batch_source} on a.v=b.v where {batch_cond} order by 1")
                expected = list(tdSql.queryResult)
                pairs = [(offset + v, None if v == 5 else offset + v) for v in values]
                assert [row[1:] for row in expected] == pairs, (name, expected)
                tdSql.checkResultsByFunc(
                    sql=f"select ts,av,bv from out_dynamic_{name} where ts >= '{start}' and ts < '{end}' order by ts",
                    func=lambda expected=expected: tdSql.queryResult == expected,
                )
        print("direct trows dynamic cache time filters ........... [ passed ]")

    def do_temporal_derived_order(self):
        tdSql.execute("create table order_trig (ts timestamp, v int)")
        tdSql.execute("create table order_rhs (ts timestamp, v int)")
        for table in ("order_trig", "order_rhs"):
            tdSql.execute(
                f"insert into {table} values ('2026-10-01 00:00:01',1) "
                "('2026-10-01 00:00:04',4) ('2026-10-01 00:00:07',7) ('2026-10-01 00:00:09',9)"
            )
        # Keep DESC/LIMIT inside the derived input; only its final three rows may join.
        derived = ("(select ts,v from order_rhs where ts>=_twstart and ts<_twend "
                   "order by ts desc limit 3) b")
        variants = (
            ("left_window", f"%%trows a right window join {derived} "
                            "window_time(a.ts,b.ts) window_offset(-1s,1s)"),
            ("right_window", f"{derived} left window join %%trows a "
                             "window_time(b.ts,a.ts) window_offset(-1s,1s)"),
            ("left_asof", f"%%trows a right asof join {derived} on a.ts<=b.ts jlimit 1"),
            ("right_asof", f"{derived} left asof join %%trows a on b.ts>=a.ts jlimit 1"),
        )
        for name, source in variants:
            tdSql.execute(
                f"create stream s_order_{name} interval(10s) from order_trig stream_options(fill_history) "
                f"into out_order_{name} (ts,bv,av) as select b.ts,b.v,a.v from {source}"
            )
            tdStream.checkStreamStatus(f"s_order_{name}")
        for offset in (0, 10):
            if offset:
                for table in ("order_rhs", "order_trig"):
                    tdSql.execute(
                        f"insert into {table} values ('2026-10-01 00:00:11',11) "
                        "('2026-10-01 00:00:14',14) ('2026-10-01 00:00:17',17) ('2026-10-01 00:00:19',19)"
                    )
                tdSql.execute("insert into order_trig values ('2026-10-01 00:00:21',NULL)")
            start = f"2026-10-01 00:00:{offset:02d}"
            end = f"2026-10-01 00:00:{offset + 10:02d}"
            trigger = f"(select * from order_trig where ts>='{start}' and ts<'{end}')"
            for name, source in variants:
                batch = source.replace("%%trows", trigger)
                batch = batch.replace("_twstart", f"cast('{start}' as timestamp)")
                batch = batch.replace("_twend", f"cast('{end}' as timestamp)")
                tdSql.query(f"select b.ts,b.v,a.v from {batch} order by 1")
                expected = list(tdSql.queryResult)
                assert [row[1:] for row in expected] == [(offset + v, offset + v) for v in (4, 7, 9)]
                tdSql.checkResultsByFunc(
                    sql=f"select ts,bv,av from out_order_{name} where ts>='{start}' and ts<'{end}' order by ts",
                    func=lambda expected=expected: tdSql.queryResult == expected,
                )
        print("direct trows temporal derived input order ......... [ passed ]")

    def do_row_dependent_time_filters(self):
        tdSql.execute("create table row_trig (ts timestamp, k int, v int)")
        tdSql.execute("create table row_rhs (ts timestamp, k int, v int)")
        tdSql.execute(
            "insert into row_trig values ('2026-10-02 00:00:01',1,2000) "
            "('2026-10-02 00:00:02',2,5000) ('2026-10-02 00:00:03',3,NULL) "
            "('2026-10-02 00:00:04',4,-1000)"
        )
        tdSql.execute(
            "insert into row_rhs values ('2026-10-02 00:00:01',1,2000) "
            "('2026-10-02 00:00:06',2,5000) ('2026-10-02 00:00:07',3,NULL) "
            "('2026-10-02 00:00:08',4,-1000)"
        )
        variants = []
        for side in ("left", "right"):
            source = "%%trows a join row_rhs b" if side == "left" else "row_rhs b join %%trows a"
            for name, condition, keys in (
                ("lower", "b.ts>=_twstart+a.v", [2, 4]),
                ("reverse", "_twstart+a.v<b.ts", [2, 4]),
                ("upper", "b.ts<_twend-a.v", [1, 4]),
                ("scalar", "b.ts>=_twstart+abs(a.v)", [2, 4]),
                ("ordinary", "b.ts>=_twstart+b.v", [2, 4]),
                ("cast", "b.ts>=_twstart+cast(a.v as bigint)", [2, 4]),
                ("case", "b.ts>=_twstart+case when a.v>0 then a.v else 0 end", [2, 3, 4]),
                ("mixed", "b.ts>=_twstart+a.v and b.ts>=_twstart and b.ts<_twend", [2, 4]),
            ):
                variants.append((f"{side}_{name}", source, condition, keys))
        for name, source, condition, _ in variants:
            tdSql.execute(
                f"create stream s_row_{name} interval(10s) from row_trig stream_options(fill_history) "
                f"into out_row_{name} (ts,av,bv) as select a.ts,a.k,b.k from {source} "
                f"on a.k=b.k where {condition}"
            )
            tdStream.checkStreamStatus(f"s_row_{name}")
        for offset in (0, 10):
            if offset:
                tdSql.execute(
                    "insert into row_rhs values ('2026-10-02 00:00:11',11,2000) "
                    "('2026-10-02 00:00:16',12,5000) ('2026-10-02 00:00:17',13,NULL) "
                    "('2026-10-02 00:00:18',14,-1000)"
                )
                tdSql.execute(
                    "insert into row_trig values ('2026-10-02 00:00:11',11,2000) "
                    "('2026-10-02 00:00:12',12,5000) ('2026-10-02 00:00:13',13,NULL) "
                    "('2026-10-02 00:00:14',14,-1000) ('2026-10-02 00:00:21',NULL,NULL)"
                )
            start = f"2026-10-02 00:00:{offset:02d}"
            end = f"2026-10-02 00:00:{offset + 10:02d}"
            trigger = f"(select * from row_trig where ts>='{start}' and ts<'{end}')"
            for name, source, condition, keys in variants:
                batch = source.replace("%%trows", trigger)
                condition = condition.replace("_twstart", f"cast('{start}' as timestamp)")
                condition = condition.replace("_twend", f"cast('{end}' as timestamp)")
                tdSql.query(f"select a.ts,a.k,b.k from {batch} on a.k=b.k where {condition} order by a.ts")
                expected = list(tdSql.queryResult)
                assert [row[1:] for row in expected] == [(offset + k, offset + k) for k in keys], (name, expected)
                tdSql.checkResultsByFunc(
                    sql=f"select ts,av,bv from out_row_{name} where ts>='{start}' and ts<'{end}' order by ts",
                    func=lambda expected=expected: tdSql.queryResult == expected,
                )
        print("direct trows row-dependent time bounds ............ [ passed ]")

    def do_timestamp_join_order(self):
        tdSql.execute("create table sort_trig (ts timestamp, v int)")
        tdSql.execute("create table sort_rhs (ts timestamp, v int)")
        for table, last in (("sort_trig", 4), ("sort_rhs", 5)):
            tdSql.execute(
                f"insert into {table} values ('2026-10-03 00:00:01',1) "
                "('2026-10-03 00:00:02',2) ('2026-10-03 00:00:03',3) "
                f"('2026-10-03 00:00:0{last}',{last})"
            )
        variants = []
        for side in ("left", "right"):
            for outer in (False, True):
                join = ("left" if side == "left" else "right") if outer else "inner"
                source = (f"%%trows a {join} join sort_rhs b" if side == "left"
                          else f"sort_rhs b {join} join %%trows a")
                for order in ("asc", "desc"):
                    name = f"{side}_{join}_{order}"
                    variants.append((name, source, order, outer))
                    tdSql.execute(
                        f"create stream s_sort_{name} interval(10s) from sort_trig stream_options(fill_history) "
                        f"into out_sort_{name} (ts,av,bv) as select a.ts,a.v,b.v from {source} "
                        f"on a.ts=b.ts order by a.ts {order}"
                    )
                    tdStream.checkStreamStatus(f"s_sort_{name}")
        for offset in (0, 10):
            if offset:
                for table, last in (("sort_rhs", 5), ("sort_trig", 4)):
                    tdSql.execute(
                        f"insert into {table} values ('2026-10-03 00:00:11',11) "
                        "('2026-10-03 00:00:12',12) ('2026-10-03 00:00:13',13) "
                        f"('2026-10-03 00:00:1{last}',{offset + last})"
                    )
                tdSql.execute("insert into sort_trig values ('2026-10-03 00:00:21',NULL)")
            start = f"2026-10-03 00:00:{offset:02d}"
            end = f"2026-10-03 00:00:{offset + 10:02d}"
            trigger = f"(select * from sort_trig where ts>='{start}' and ts<'{end}')"
            for name, source, order, outer in variants:
                batch = source.replace("%%trows", trigger)
                tdSql.query(f"select a.ts,a.v,b.v from {batch} on a.ts=b.ts order by a.ts {order}")
                expected = sorted(tdSql.queryResult, key=lambda row: row[0])
                pairs = [(offset + v, offset + v) for v in (1, 2, 3)]
                if outer:
                    pairs.append((offset + 4, None))
                assert [row[1:] for row in expected] == pairs, (name, expected)
                tdSql.checkResultsByFunc(
                    sql=f"select ts,av,bv from out_sort_{name} where ts>='{start}' and ts<'{end}' order by ts",
                    func=lambda expected=expected: tdSql.queryResult == expected,
                )
        print("direct trows timestamp JOIN ASC/DESC .............. [ passed ]")

    def do_window_jlimit_time_filters(self):
        tdSql.execute("create table jlimit_trig (ts timestamp, v int)")
        tdSql.execute("create table jlimit_rhs (ts timestamp, v int)")
        tdSql.execute(
            "insert into jlimit_trig values ('2026-11-01 00:00:05',5) "
            "('2026-11-01 00:00:15',15) ('2026-11-01 00:00:21',NULL)"
        )
        tdSql.execute(
            "insert into jlimit_rhs values ('2026-11-01 00:00:01',1) "
            "('2026-11-01 00:00:06',6) ('2026-11-01 00:00:07',7) "
            "('2026-11-01 00:00:16',16) ('2026-11-01 00:00:17',17) ('2026-11-01 00:00:18',18)"
        )
        variants = []
        for side in ("left", "right"):
            source = ("%%trows a left window join jlimit_rhs b" if side == "left"
                      else "jlimit_rhs b right window join %%trows a")
            for name, limit, condition, keys, checkpoint in (
                ("one", "jlimit 1", "b.ts>=_twstart+5s", [], [16]),
                ("two", "jlimit 2", "b.ts>=_twstart+5s", [6], [16, 17]),
                ("all", "", "b.ts>=_twstart+5s", [6, 7], [16, 17, 18]),
                ("reverse", "jlimit 1", "_twstart+5s<=b.ts", [], [16]),
                ("mixed", "jlimit 1", "b.ts>=_twstart+5s and b.ts<_twend and a.ts>=_twstart", [], [16]),
            ):
                name = f"{side}_{name}"
                variants.append((name, source, limit, condition, keys, checkpoint))
                tdSql.execute(
                    f"create stream s_jlimit_{name} interval(10s) from jlimit_trig stream_options(fill_history) "
                    f"into out_jlimit_{name} (ts,k primary key,bts) as select a.ts,b.v,b.ts from {source} "
                    f"window_offset(-5s,5s) {limit} where {condition}"
                )
                tdStream.checkStreamStatus(f"s_jlimit_{name}")
        for offset in (0, 20):
            if offset:
                tdSql.execute(
                    "insert into jlimit_rhs values ('2026-11-01 00:00:21',21) "
                    "('2026-11-01 00:00:26',26) ('2026-11-01 00:00:27',27) "
                    "('2026-11-01 00:00:36',36) ('2026-11-01 00:00:37',37) ('2026-11-01 00:00:38',38)"
                )
                tdSql.execute(
                    "insert into jlimit_trig values ('2026-11-01 00:00:25',25) "
                    "('2026-11-01 00:00:35',35) ('2026-11-01 00:00:41',NULL)"
                )
            start = f"2026-11-01 00:00:{offset:02d}"
            end = f"2026-11-01 00:00:{offset + 10:02d}"
            trigger_ts = f"2026-11-01 00:00:{offset + 5:02d}"
            checkpoint_ts = f"2026-11-01 00:00:{offset + 15:02d}"
            for name, source, limit, condition, keys, checkpoint in variants:
                # A later output from this stream confirms the empty window was processed.
                progress = [(offset + key,) for key in checkpoint]
                tdSql.checkResultsByFunc(
                    sql=f"select k from out_jlimit_{name} where ts='{checkpoint_ts}' order by k",
                    func=lambda progress=progress: tdSql.queryResult == progress,
                )
                batch = source.replace("%%trows", f"(select * from jlimit_trig where ts='{trigger_ts}')")
                predicate = condition.replace("_twstart", f"cast('{start}' as timestamp)")
                predicate = predicate.replace("_twend", f"cast('{end}' as timestamp)")
                tdSql.query(
                    f"select a.ts,b.v,b.ts from {batch} window_offset(-5s,5s) {limit} "
                    f"where {predicate} order by b.v"
                )
                tdSql.checkRows(len(keys))
                for row, key in enumerate(keys):
                    tdSql.checkData(row, 1, offset + key)
                expected = list(tdSql.queryResult)
                tdSql.query(f"select ts,k,bts from out_jlimit_{name} where ts='{trigger_ts}' order by k")
                tdSql.checkRows(len(keys))
                for row, values in enumerate(expected):
                    for column, value in enumerate(values):
                        tdSql.checkData(row, column, value)
        print("direct trows WINDOW JLIMIT post-JOIN filters ...... [ passed ]")

    def do_window_time_cache_column(self):
        tdSql.execute("create table time_trig (ts timestamp, v int, event_ts timestamp)")
        tdSql.execute("create table time_rhs (ts timestamp, v int)")
        tdSql.execute(
            "insert into time_trig values ('2026-08-01 00:00:01',1,'2026-08-01 00:01:01') "
            "('2026-08-01 00:00:02',2,'2026-08-01 00:01:04') "
            "('2026-08-01 00:00:03',3,'2026-08-01 00:01:07')"
        )
        tdSql.execute(
            "insert into time_rhs values ('2026-08-01 00:01:01',101) "
            "('2026-08-01 00:01:04',102) ('2026-08-01 00:01:07',103) "
            "('2026-08-01 00:02:01',201) ('2026-08-01 00:02:04',202) ('2026-08-01 00:02:07',203)"
        )
        variants = (
            ("left", "%%trows a left window join time_rhs b window_time(a.event_ts,b.ts)"),
            ("right", "time_rhs b right window join %%trows a window_time(b.ts,a.event_ts)"),
        )
        for name, source in variants:
            # event_ts is deliberately absent from the projection and trigger definition.
            tdSql.execute(
                f"create stream s_cache_time_{name} interval(10s) sliding(10s) "
                "from time_trig stream_options(fill_history) "
                f"into out_cache_time_{name} (ts,av,bv) as select a.ts,a.v,b.v "
                f"from {source} window_offset(-1s,1s)"
            )
        for name, _ in variants:
            tdStream.checkStreamStatus(f"s_cache_time_{name}")

        for offset in (0, 10):
            if offset:
                tdSql.execute(
                    "insert into time_trig values ('2026-08-01 00:00:11',11,'2026-08-01 00:02:01') "
                    "('2026-08-01 00:00:12',12,'2026-08-01 00:02:04') "
                    "('2026-08-01 00:00:13',13,'2026-08-01 00:02:07') "
                    "('2026-08-01 00:00:21',NULL,NULL)"
                )
            start = f"2026-08-01 00:00:{offset:02d}"
            end = f"2026-08-01 00:00:{offset + 10:02d}"
            trigger = f"(select * from time_trig where ts >= '{start}' and ts < '{end}')"
            for name, source in variants:
                tdSql.query(
                    f"select a.ts,a.v,b.v from {source.replace('%%trows', trigger)} "
                    "window_offset(-1s,1s) order by 1"
                )
                expected = list(tdSql.queryResult)
                assert [row[1:] for row in expected] == [(offset + i, 100 + offset * 10 + i) for i in (1, 2, 3)]
                tdSql.checkResultsByFunc(
                    sql=f"select ts,av,bv from out_cache_time_{name} where ts >= '{start}' and ts < '{end}' order by ts",
                    func=lambda expected=expected: tdSql.queryResult == expected,
                )
        print("direct trows WINDOW_TIME-only cache column ........ [ passed ]")

    def do_invalid_shapes(self):
        prefix = "create stream s_invalid interval(10s) sliding(10s) from trig into out_invalid as "
        queries = [
            "select a.ts, a.v from %%trows a join %%trows b on a.ts = b.ts",
            "select a.ts, a.v from (%%trows a join rhs b on a.ts = b.ts) "
            "join rhs c on a.ts = c.ts",
            "select a.ts, a.v from %%trows a join "
            "(select * from (select * from %%trows) q) b on a.ts = b.ts",
            "select a.ts, a.v from %%trows a join rhs b "
            "on a.v = b.v and b.v in (select v from %%trows)",
            "select a.ts, (select count(*) from %%trows) n "
            "from %%trows a join rhs b on a.v = b.v",
            "select a.ts, a.v from %%trows a join rhs b on a.v = b.v "
            "where b.v in (select v from %%trows)",
            "select a.ts, a.v from %%trows a join "
            "(select ts, v, (select count(*) from %%trows) n from rhs) b on a.v = b.v",
        ]
        for query in queries:
            tdSql.error(prefix + query, expectedErrno=0x2645)
        print("direct trows invalid JOIN shapes ................. [ passed ]")

    def do_output_key_contract(self):
        tdSql.execute(
            "create stream s_default_key interval(10s) sliding(10s) "
            "from trig stream_options(fill_history) into out_default_key (ts, av, bv) "
            "as select _twstart, a.v, b.v from %%trows a cross join rhs b"
        )
        tdSql.query("select a.v, b.v from trig a cross join rhs b order by 1, 2")
        tdSql.checkRows(9)
        batch_rows = list(tdSql.queryResult)
        tdSql.checkResultsByFunc(
            sql="select av, bv from out_default_key",
            func=lambda: len(tdSql.queryResult) == 1 and tdSql.queryResult[0] in batch_rows,
        )
        tdSql.query("select ts, k, av, bv from out_cross order by ts, k")
        tdSql.checkRows(9)
        print("direct trows output key contract ................. [ passed ]")

    def do_live_window(self):
        tdSql.execute("insert into trig values ('2026-01-01 00:00:11', 5)")
        tdSql.execute("insert into trig values ('2026-01-01 00:00:21', 6)")
        tdSql.query(
            "select a.ts, cast(a.v*10+b.v as int), a.v, b.v "
            "from trig a cross join rhs b "
            "where a.ts >= '2026-01-01 00:00:10' and a.ts < '2026-01-01 00:00:20' "
            "order by 1, 2"
        )
        tdSql.checkRows(3)
        expected = list(tdSql.queryResult)
        tdSql.checkResultsByFunc(
            sql="select * from out_cross where ts >= '2026-01-01 00:00:10' "
                "and ts < '2026-01-01 00:00:20' order by 1, 2",
            func=lambda: tdSql.queryResult == expected,
        )
        print("direct trows live window ......................... [ passed ]")

    def do_recalculation(self):
        tdSql.query(
            "select a.ts, cast(a.v*10+b.v as int), a.v, b.v "
            "from trig a cross join rhs b "
            "where a.ts < '2026-01-01 00:00:20' order by 1, 2"
        )
        tdSql.checkRows(12)
        expected = list(tdSql.queryResult)
        tdSql.execute("stop stream s_cross")
        tdSql.execute("delete from out_cross")
        tdSql.query("select * from out_cross")
        tdSql.checkRows(0)
        tdSql.execute("start stream s_cross")
        tdStream.checkStreamStatus("s_cross")
        tdSql.execute(
            "recalculate stream s_cross from '2026-01-01 00:00:00' "
            "to '2026-01-01 00:00:20'"
        )
        tdSql.checkResultsByFunc(
            sql="select * from out_cross order by 1, 2",
            func=lambda: tdSql.queryResult == expected,
        )
        tdSql.execute("stop stream s_cross")
        tdSql.execute("start stream s_cross")
        tdStream.checkStreamStatus("s_cross")
        tdSql.query("select * from out_cross order by 1, 2")
        assert tdSql.queryResult == expected
        print("direct trows recalculation and restart ........... [ passed ]")

    def do_partition_isolation(self):
        tdSql.execute("create stable trig_groups (ts timestamp, v int, marker int) tags (gid int)")
        tdSql.execute("create table g1 using trig_groups tags (1)")
        tdSql.execute("create table g2 using trig_groups tags (2)")
        tdSql.execute("create table dim_groups (ts timestamp, v int)")
        tdSql.execute(
            "insert into dim_groups values ('2026-02-01 00:00:01',1) "
            "('2026-02-01 00:00:02',2) ('2026-02-01 00:00:03',3)"
        )
        tdSql.execute(
            "insert into g1 values ('2026-02-01 00:00:01',1,101) "
            "('2026-02-01 00:00:02',2,102) ('2026-02-01 00:00:04',4,104) "
            "('2026-02-01 00:00:11',2,112) ('2026-02-01 00:00:12',4,114) "
            "('2026-02-01 00:00:21',9,109)"
        )
        tdSql.execute(
            "insert into g2 values ('2026-02-01 00:00:01',1,201) "
            "('2026-02-01 00:00:02',3,203) ('2026-02-01 00:00:04',5,205) "
            "('2026-02-01 00:00:11',3,213) ('2026-02-01 00:00:12',5,215) "
            "('2026-02-01 00:00:21',9,209)"
        )
        variants = []
        for name, _, join, condition, count in self.cases():
            if name in ("semi", "anti"):
                columns = "a.marker,a.marker,cast(null as int)"
            elif name in ("right_semi", "right_anti"):
                columns = "b.v,cast(null as int),b.v"
            else:
                columns = "cast(coalesce(a.marker,0)*10+coalesce(b.v,0) as int),a.marker,b.v"
            variants.append((name, columns, join, condition, count))
            tdSql.execute(
                f"create stream s_groups_{name} interval(10s) sliding(10s) from trig_groups "
                "partition by tbname stream_options(fill_history) "
                f"into out_groups_{name} output_subtable(concat('out_grp_{name}_',tbname)) "
                f"(ts,k composite key,marker,bv) as select _twstart,{columns} "
                f"from %%trows a {join} dim_groups b {condition}"
            )
        second_counts = {
            "inner": 1, "left": 2, "right": 3, "full": 4,
            "semi": 1, "anti": 1, "right_semi": 1, "right_anti": 2,
            "cross": 6, "asof": 2, "window": 2, "right_asof": 3, "right_window": 3,
        }
        for name, columns, join, condition, count in variants:
            for table in ("g1", "g2"):
                for start, end, expected_count in (("00", "10", count), ("10", "20", second_counts[name])):
                    tdSql.query(
                        f"select {columns} from (select * from {table} "
                        f"where ts >= '2026-02-01 00:00:{start}' "
                        f"and ts < '2026-02-01 00:00:{end}') a "
                        f"{join} dim_groups b {condition} order by 1"
                    )
                    tdSql.checkRows(expected_count)
                    expected = list(tdSql.queryResult)
                    if name in ("right_semi", "right_anti"):
                        if start == "00":
                            keys = [1, 2] if table == "g1" else [1, 3]
                        else:
                            keys = [2] if table == "g1" else [3]
                        if name == "right_anti":
                            keys = [key for key in (1, 2, 3) if key not in keys]
                        assert expected == [(key, None, key) for key in keys]
                    tdSql.checkResultsByFunc(
                        sql=f"select k,marker,bv from out_grp_{name}_{table} "
                            f"where ts = '2026-02-01 00:00:{start}' order by k",
                        func=lambda expected=expected: tdSql.queryResult == expected,
                    )
        print("direct trows JOIN family partition isolation ...... [ passed ]")

    def do_null_and_duplicate_keys(self):
        tdSql.execute("create table trig_keys (ts timestamp, v int, marker int)")
        tdSql.execute(
            "insert into trig_keys values "
            "('2026-03-01 00:00:01',null,11) "
            "('2026-03-01 00:00:02',1,12) "
            "('2026-03-01 00:00:03',1,13) "
            "('2026-03-01 00:00:11',4,14)"
        )
        tdSql.execute(
            "create stream s_keys interval(10s) sliding(10s) "
            "from trig_keys stream_options(fill_history) "
            "into out_keys (ts, marker composite key, av, bv) as "
            "select a.ts, a.marker, a.v, b.v "
            "from %%trows a left join rhs b on a.v = b.v"
        )
        tdSql.query(
            "select a.ts, a.marker, a.v, b.v from trig_keys a "
            "left join rhs b on a.v = b.v "
            "where a.ts < '2026-03-01 00:00:10' order by 1, 2"
        )
        tdSql.checkRows(3)
        expected = list(tdSql.queryResult)
        assert [row[1] for row in expected] == [11, 12, 13]
        assert expected[0][2:] == (None, None)
        tdSql.checkResultsByFunc(
            sql="select ts,marker,av,bv from out_keys "
                "where ts < '2026-03-01 00:00:10' order by 1, 2",
            func=lambda: tdSql.queryResult == expected,
        )
        print("direct trows NULL and duplicate keys ............. [ passed ]")

    def do_large_trigger_group(self):
        tdSql.execute("create table trig_large (ts timestamp, v int, marker int)")
        values = []
        for index in range(5000):
            second, millis = divmod(index, 1000)
            values.append(f"('2026-04-01 00:00:0{second}.{millis:03d}',1,{index + 1})")
        tdSql.execute("insert into trig_large values " + " ".join(values))
        tdSql.execute("insert into trig_large values ('2026-04-01 00:00:21',4,5001)")
        tdSql.execute(
            "create stream s_large interval(10s) sliding(10s) "
            "from trig_large stream_options(fill_history) "
            "into out_large (ts, marker composite key, av, bv) as "
            "select _twstart, a.marker, a.v, b.v "
            "from %%trows a inner join rhs b on a.v = b.v"
        )
        tdSql.query(
            "select a.marker, a.v, b.v from trig_large a inner join rhs b "
            "on a.v = b.v where a.ts < '2026-04-01 00:00:10' "
            "order by a.marker"
        )
        tdSql.checkRows(5000)
        expected = list(tdSql.queryResult)
        tdSql.checkResultsByFunc(
            sql="select count(*) from out_large where ts = '2026-04-01 00:00:00'",
            func=lambda: tdSql.queryResult == [(5000,)],
        )
        tdSql.query(
            "select marker,av,bv from out_large "
            "where ts = '2026-04-01 00:00:00' order by marker"
        )
        assert tdSql.queryResult == expected
        print("direct trows large trigger group .................. [ passed ]")

    def do_empty_trigger_rows(self):
        tdSql.execute("create table trig_empty (ts timestamp, v int)")
        tdSql.execute("create table empty_input (ts timestamp, v int)")
        tdSql.execute(
            "insert into trig_empty values ('2026-05-01 00:00:01',1) "
            "('2026-05-01 00:00:31',4)"
        )
        variants = (
            ("right", "%%trows a right join rhs b on a.v = b.v", "a.v"),
            ("full", "%%trows a full join rhs b on a.v = b.v", "a.v"),
            ("anti", "%%trows a right anti join rhs b on a.v = b.v", "cast(null as int)"),
            ("reverse", "rhs b left join %%trows a on a.v = b.v", "a.v"),
            ("asof", "%%trows a right asof join rhs b on a.ts <= b.ts jlimit 1", "a.v"),
            ("window", "%%trows a right window join rhs b window_offset(-1s,1s)", "a.v"),
        )
        for name, source, av in variants:
            key = "coalesce(a.v,b.v)" if name == "full" else "b.v"
            tdSql.execute(
                f"create stream s_empty_{name} interval(10s) sliding(10s) "
                "from trig_empty stream_options(fill_history | event_type(window_close)) "
                f"into out_empty_{name} (ts,k composite key,n,av,bv) "
                f"as select _twstart,{key},_twrownum,{av},b.v from {source}"
            )
        for name, source, av in variants:
            tdSql.query(f"select b.v,cast(0 as bigint),{av},b.v from "
                        f"{source.replace('%%trows', 'empty_input')} order by b.v")
            tdSql.checkRows(3)
            expected = list(tdSql.queryResult)
            assert expected == [(1, 0, None, 1), (2, 0, None, 2), (3, 0, None, 3)]
            for start in ("2026-05-01 00:00:10", "2026-05-01 00:00:20"):
                tdSql.checkResultsByFunc(
                    sql=f"select k,n,av,bv from out_empty_{name} where ts = '{start}' order by k",
                    func=lambda expected=expected: tdSql.queryResult == expected,
                )
        for name, _, join, condition, _ in self.cases():
            tdSql.execute(
                f"create stream s_both_empty_{name} interval(10s) sliding(10s) "
                "from trig_empty stream_options(fill_history | event_type(window_close)) "
                f"into out_both_empty_{name} (ts,n,c) "
                "as select _twstart,_twrownum,count(*) "
                f"from %%trows a {join} empty_input b {condition}"
            )
        for name, _, join, condition, _ in self.cases():
            # COUNT emits one row even for an empty JOIN, proving the window ran.
            tdSql.query(f"select count(*) from empty_input a {join} empty_input b {condition}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            for start in ("2026-05-01 00:00:10", "2026-05-01 00:00:20"):
                tdSql.checkResultsByFunc(
                    sql=f"select n,c from out_both_empty_{name} where ts = '{start}'",
                    func=lambda: tdSql.queryResult == [(0, 0)],
                )
            retained = 1 if name in ("left", "full", "anti", "asof", "window") else 0
            tdSql.query(
                "select count(*) from "
                "(select * from trig_empty where ts < '2026-05-01 00:00:10') a "
                f"{join} empty_input b {condition}"
            )
            tdSql.checkData(0, 0, retained)
            tdSql.checkResultsByFunc(
                sql=f"select n,c from out_both_empty_{name} where ts = '2026-05-01 00:00:00'",
                func=lambda retained=retained: tdSql.queryResult == [(1, retained)],
            )
        print("direct trows empty WINDOW_CLOSE inputs ............ [ passed ]")

    def do_recalculation_changed_keys(self):
        tdSql.execute("create table trig_rekey (ts timestamp, v int, id int)")
        tdSql.execute("create table dim_rekey (ts timestamp, v int, payload int)")
        tdSql.execute(
            "insert into trig_rekey values ('2026-06-01 00:00:01',1,101) "
            "('2026-06-01 00:00:02',2,102) ('2026-06-01 00:00:21',9,103)"
        )
        tdSql.execute(
            "insert into dim_rekey values ('2026-06-01 00:00:01',1,10) "
            "('2026-06-01 00:00:02',2,20) ('2026-06-01 00:00:03',3,30)"
        )
        tdSql.execute(
            "create stream s_rekey interval(10s) sliding(10s) "
            "from trig_rekey stream_options(fill_history) "
            "into out_rekey (ts,k composite key,id,payload) as "
            "select _twstart,cast(a.id*100+b.v as int),a.id,b.payload "
            "from %%trows a inner join dim_rekey b on a.v = b.v"
        )
        output = "select k,id,payload from out_rekey order by k"
        tdSql.checkResultsByFunc(
            sql=output,
            func=lambda: tdSql.queryResult == [(10101, 101, 10), (10202, 102, 20)],
        )
        tdSql.execute("stop stream s_rekey")
        tdSql.execute("insert into trig_rekey values ('2026-06-01 00:00:01',3,101)")
        tdSql.execute("insert into dim_rekey values ('2026-06-01 00:00:02',2,22)")
        tdSql.query(
            "select cast(a.id*100+b.v as int),a.id,b.payload from trig_rekey a "
            "inner join dim_rekey b on a.v = b.v "
            "where a.ts < '2026-06-01 00:00:10' order by 1"
        )
        expected = list(tdSql.queryResult)
        assert expected == [(10103, 101, 30), (10202, 102, 22)]
        tdSql.execute("start stream s_rekey")
        tdStream.checkStreamStatus("s_rekey")
        recalc = "recalculate stream s_rekey from '2026-06-01 00:00:00' to '2026-06-01 00:00:10'"
        tdSql.execute(recalc)
        tdSql.checkResultsByFunc(
            sql=output,
            func=lambda: tdSql.queryResult == [(10101, 101, 10)] + expected,
        )
        tdSql.execute("stop stream s_rekey")
        tdSql.execute("delete from out_rekey")
        tdSql.execute("start stream s_rekey")
        tdStream.checkStreamStatus("s_rekey")
        tdSql.execute(recalc)
        tdSql.checkResultsByFunc(sql=output, func=lambda: tdSql.queryResult == expected)
        print("direct trows changed keys retain old output ........ [ passed ]")

    def test_direct_trows_join(self):
        """Direct trigger rows JOIN results match batch queries

        1. Compare both directions of each JOIN family with batch queries
        2. Preserve all nine CROSS rows with a composite result key
        3. Verify reversed and derived inputs, INNER/OUTER range and OR, and JOIN post-filtering
        4. Reject repeated trigger rows and nested direct JOIN references
        5. Reproduce default timestamp key overwrite and verify live windows
        6. Recalculate after clearing results and restart the stream
        7. Isolate shared and distinct keys across trigger groups for all JOIN families; retain NULL, duplicate keys
        8. Compare all 5000 rows of a large trigger group with batch results
        9. Check either and both empty inputs; aggregate output proves empty WINDOW_CLOSE execution
        10. Verify old-key retention, same-key overwrite and new-key insertion on recalculation
        11. Check independent cache WHERE/ON filters, NULLs, timestamp ranges and preserved outer rows in history/live windows
        12. Retain WINDOW_TIME-only secondary timestamps in both cache input directions in history/live windows
        13. Narrow dynamic cache time bounds in history/live windows without pruning ordinary rows or losing outer semantics
        14. Retain ordinary-side dynamic WHERE null rejection for LEFT/RIGHT/FULL JOIN history/live results
        15. Retain derived-table dynamic time filters and DESC/LIMIT temporal JOIN results in both input directions
        16. Evaluate row-dependent dynamic time bounds after joining, including NULL and negative offsets
        17. Preserve INNER/OUTER timestamp JOIN matches with ASC/DESC in both input directions
        18. Apply dynamic WINDOW JOIN filters after JLIMIT selection in history/live windows and both directions

        Catalog:
            - Streams:SubQuery

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-10 Add direct trigger rows JOIN result coverage
            - 2026-11-11 Cover empty windows, output-key changes, input boundaries and large groups
            - 2026-11-12 Cover partition isolation and both empty inputs for every JOIN family
            - 2026-11-15 Cover cache filters and WINDOW_TIME-only columns from human review
            - 2026-11-16 Cover dynamic cache time bounds, both input sides and outer WHERE semantics
            - 2026-11-17 Cover nullable ordinary-side dynamic time filters after outer JOINs
            - 2026-11-17 Cover derived dynamic time bounds and WINDOW/ASOF cache ordering
            - 2026-11-17 Cover row-dependent time bounds and ordinary timestamp JOIN ordering
            - 2026-11-18 Cover WINDOW JOIN JLIMIT before dynamic time filtering
        """
        self.prepare()
        self.do_join_families()
        self.do_join_boundaries()
        self.do_outer_conditions()
        self.do_cache_side_filters()
        self.do_dynamic_cache_time_filters()
        self.do_temporal_derived_order()
        self.do_row_dependent_time_filters()
        self.do_timestamp_join_order()
        self.do_window_jlimit_time_filters()
        self.do_window_time_cache_column()
        self.do_invalid_shapes()
        self.do_output_key_contract()
        self.do_live_window()
        self.do_recalculation()
        self.do_partition_isolation()
        self.do_null_and_duplicate_keys()
        self.do_large_trigger_group()
        self.do_empty_trigger_rows()
        self.do_recalculation_changed_keys()
