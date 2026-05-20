from new_test_framework.utils import tdLog, tdSql, tdStream, StreamCheckItem


class TestTrueFor:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode()

    def test_truefor_event_window(self):
        """true_for start/end streak tests for EVENT_WINDOW streams.

        Catalog:
            - Streams:Others

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-05-14 Created

        Test cases:
            s1  start(count 2)             : window opens only after 2 CONSECUTIVE start
                                             rows; one interruption resets the streak.
            s2  end(count 2)               : window closes only after 2 CONSECUTIVE end
                                             rows; one interruption resets the streak.
            s3  start(count 2),end(count 2): both conditions require independent streaks.
            s4  start(2s)                  : start cond must hold continuously for >= 2s.
            s5  end(2s)                    : end cond must hold continuously for >= 2s.
            s6  true_for(3s)               : backward compat – window must last >= 3s.
            s7  start(2s or count 2)       : OR fires as soon as count reaches 2,
                                             even before 2s have elapsed.
            s8  start(2s and count 3)      : AND requires BOTH duration >= 2s AND count >= 3
                                             simultaneously.
            s9  sub-event + start(count 2) : per-condition streak IS applied to sub-event
                                             windows (START WITH (cond1, cond2)); each
                                             condition tracks its own independent streak.
                                             Window opens only after N consecutive rows of
                                             the same condition.
            s10 sub-event condition switch : two start conditions; when the active condition
                                             changes, the previous sub-window is force-closed
                                             and a new one opened.  numSubWindows=2 produces
                                             three output rows: sub-win1, sub-win2, parent.
            s11 sub-event switch + streak  : same as s10 but with true_for(start(count 2)).
                                             After a condition switch the new condition must
                                             also accumulate count 2 before its sub-window
                                             opens.  Interleaved cond1/cond2 rows reset each
                                             other's streaks (strict-consecutive semantics).

        See also: test_truefor_arg_order — covers all 8 new permutations of args.
        """

        streams = [
            self.StartCount2(),
            self.EndCount2(),
            self.StartCount2EndCount2(),
            self.StartDur2s(),
            self.EndDur2s(),
            self.WindowDur3s(),
            self.StartOrDurCount(),
            self.StartAndDurCount(),
            self.SubEventStartCountIgnored(),
            self.SubEventTrueForRejected(),
        ]

        tdStream.checkAll(streams)

    # ── s1: start(count 2) ──────────────────────────────────────────────────
    class StartCount2(StreamCheckItem):
        def __init__(self):
            self.db = "db_s1"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb1 using meters tags(1);")
            # Window opens only after 2 CONSECUTIVE start rows.
            # An isolated start row (count=1) must NOT open a window.
            tdSql.execute(
                "create stream s1 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(start(count 2)) "
                "FROM tb1 PARTITION BY tbname "
                "INTO out1 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # t=01 v=221 → start, streak=1         (no window – streak not met)
            # t=02 v=100 → NOT start → streak RESET (interruption proves consecutive rule)
            # t=03 v=222 → start, streak=1 (firstTs=03)
            # t=04 v=223 → start, streak=2 → window opens skey=03 (first streak row)
            # t=05 v=100 → end              → window closes ekey=05  window1 cnt=3 [t03..t05]
            # t=06 v=221 → start, streak=1 (firstTs=06)
            # t=07 v=222 → start, streak=2 → window opens skey=06
            # t=08 v=100 → end              → window closes ekey=08  window2 cnt=3 [t06..t08]
            tdSql.executes([
                "insert into tb1 values ('2025-01-01 00:00:01', 221);",
                "insert into tb1 values ('2025-01-01 00:00:02', 100);",
                "insert into tb1 values ('2025-01-01 00:00:03', 222);",
                "insert into tb1 values ('2025-01-01 00:00:04', 223);",
                "insert into tb1 values ('2025-01-01 00:00:05', 100);",
                "insert into tb1 values ('2025-01-01 00:00:06', 221);",
                "insert into tb1 values ('2025-01-01 00:00:07', 222);",
                "insert into tb1 values ('2025-01-01 00:00:08', 100);",
            ])

        def check1(self):
            # 2 windows; skey = first streak row; cnt includes all rows in [skey, ekey].
            tdSql.checkResultsByFunc(
                sql="select ts, cnt from out1 order by ts",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:03.000")
                and tdSql.compareData(0, 1, 3)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:06.000")
                and tdSql.compareData(1, 1, 3),
            )

    # ── s2: end(count 2) ────────────────────────────────────────────────────
    class EndCount2(StreamCheckItem):
        def __init__(self):
            self.db = "db_s2"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb2 using meters tags(1);")
            tdSql.execute(
                "create stream s2 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(end(count 2)) "
                "FROM tb2 PARTITION BY tbname "
                "INTO out2 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # t=01 v=221 → start → window opens (skey=01)
            # t=02 v=100 → end, streak=1 (firstTs=02)     (no close – not consecutive yet)
            # t=03 v=221 → NOT end → streak RESET
            # t=04 v=100 → end, streak=1 (firstTs=04)     (no close)
            # t=05 v=99  → end, streak=2 → window closes  ekey=04 (first streak row) cnt=4
            tdSql.executes([
                "insert into tb2 values ('2025-01-01 00:00:01', 221);",
                "insert into tb2 values ('2025-01-01 00:00:02', 100);",
                "insert into tb2 values ('2025-01-01 00:00:03', 221);",
                "insert into tb2 values ('2025-01-01 00:00:04', 100);",
                "insert into tb2 values ('2025-01-01 00:00:05', 99);",
            ])

        def check1(self):
            # 1 window; ekey = first row of satisfying end streak = t=04; cnt=4 [t01..t04].
            tdSql.checkResultsByFunc(
                sql="select ts, cnt from out2 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01.000")
                and tdSql.compareData(0, 1, 4),
            )

    # ── s3: start(count 2) + end(count 2) ───────────────────────────────────
    class StartCount2EndCount2(StreamCheckItem):
        def __init__(self):
            self.db = "db_s3"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb3 using meters tags(1);")
            tdSql.execute(
                "create stream s3 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(start(count 2), end(count 2)) "
                "FROM tb3 PARTITION BY tbname "
                "INTO out3 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # t=01 v=221 → start streak=1
            # t=02 v=100 → NOT start → reset start streak
            # t=03 v=222 → start streak=1 (firstTs=03)
            # t=04 v=223 → start streak=2 → window opens skey=03 (first streak row)
            # t=05 v=100 → end streak=1 (firstTs=05)
            # t=06 v=221 → NOT end → reset end streak
            # t=07 v=100 → end streak=1 (firstTs=07)
            # t=08 v=99  → end streak=2 → window closes ekey=07 (first streak row) cnt=5 [t03..t07]
            tdSql.executes([
                "insert into tb3 values ('2025-01-01 00:00:01', 221);",
                "insert into tb3 values ('2025-01-01 00:00:02', 100);",
                "insert into tb3 values ('2025-01-01 00:00:03', 222);",
                "insert into tb3 values ('2025-01-01 00:00:04', 223);",
                "insert into tb3 values ('2025-01-01 00:00:05', 100);",
                "insert into tb3 values ('2025-01-01 00:00:06', 221);",
                "insert into tb3 values ('2025-01-01 00:00:07', 100);",
                "insert into tb3 values ('2025-01-01 00:00:08', 99);",
            ])

        def check1(self):
            # 1 window: skey=03 ekey=07 cnt=5.  User confirmed these exact timestamps.
            tdSql.checkResultsByFunc(
                sql="select ts, cnt from out3 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:03.000")
                and tdSql.compareData(0, 1, 5),
            )

    # ── s4: start(2s) ───────────────────────────────────────────────────────
    class StartDur2s(StreamCheckItem):
        def __init__(self):
            self.db = "db_s4"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb4 using meters tags(1);")
            tdSql.execute(
                "create stream s4 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(start(2s)) "
                "FROM tb4 PARTITION BY tbname "
                "INTO out4 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # t=00 v=221 → start, firstTs=00, dur=0s < 2s   (no window)
            # t=01 v=100 → NOT start → RESET streak
            # t=02 v=221 → start, firstTs=02, dur=0s
            # t=03 v=222 → start, dur=1s < 2s
            # t=04 v=223 → start, dur=2s >= 2s → window opens skey=02 (firstTs) cnt=4 [t02..t05]
            # t=05 v=100 → end (immediate)     → window closes ekey=05
            tdSql.executes([
                "insert into tb4 values ('2025-01-01 00:00:00', 221);",
                "insert into tb4 values ('2025-01-01 00:00:01', 100);",
                "insert into tb4 values ('2025-01-01 00:00:02', 221);",
                "insert into tb4 values ('2025-01-01 00:00:03', 222);",
                "insert into tb4 values ('2025-01-01 00:00:04', 223);",
                "insert into tb4 values ('2025-01-01 00:00:05', 100);",
            ])

        def check1(self):
            # 1 window skey=02 cnt=4 [t02, t03, t04, t05].
            tdSql.checkResultsByFunc(
                sql="select ts, cnt from out4 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:02.000")
                and tdSql.compareData(0, 1, 4),
            )

    # ── s5: end(2s) ─────────────────────────────────────────────────────────
    class EndDur2s(StreamCheckItem):
        def __init__(self):
            self.db = "db_s5"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb5 using meters tags(1);")
            tdSql.execute(
                "create stream s5 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(end(2s)) "
                "FROM tb5 PARTITION BY tbname "
                "INTO out5 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # t=00 v=221 → start → window opens (skey=00)
            # t=01 v=100 → end, firstTs=01, dur=0s           (no close)
            # t=02 v=221 → NOT end → RESET end streak
            # t=03 v=100 → end, firstTs=03, dur=0s
            # t=04 v=99  → end, dur=1s < 2s                  (no close)
            # t=05 v=98  → end, dur=2s >= 2s → closes ekey=03 (firstTs) cnt=4 [t00..t03]
            tdSql.executes([
                "insert into tb5 values ('2025-01-01 00:00:00', 221);",
                "insert into tb5 values ('2025-01-01 00:00:01', 100);",
                "insert into tb5 values ('2025-01-01 00:00:02', 221);",
                "insert into tb5 values ('2025-01-01 00:00:03', 100);",
                "insert into tb5 values ('2025-01-01 00:00:04', 99);",
                "insert into tb5 values ('2025-01-01 00:00:05', 98);",
            ])

        def check1(self):
            # 1 window skey=00 ekey=03 cnt=4 [t00..t03].
            tdSql.checkResultsByFunc(
                sql="select ts, cnt from out5 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                and tdSql.compareData(0, 1, 4),
            )

    # ── s6: true_for(3s) backward compat ────────────────────────────────────
    class WindowDur3s(StreamCheckItem):
        def __init__(self):
            self.db = "db_s6"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb6 using meters tags(1);")
            # Original true_for: whole window must span >= 3s; shorter windows discarded.
            tdSql.execute(
                "create stream s6 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(3s) "
                "FROM tb6 PARTITION BY tbname "
                "INTO out6 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # window1: t=00..t=02 → duration=2s < 3s → DISCARDED
            # window2: t=10..t=13 → duration=3s >= 3s → output cnt=4
            tdSql.executes([
                "insert into tb6 values ('2025-01-01 00:00:00', 221);",
                "insert into tb6 values ('2025-01-01 00:00:01', 222);",
                "insert into tb6 values ('2025-01-01 00:00:02', 100);",
                "insert into tb6 values ('2025-01-01 00:00:10', 221);",
                "insert into tb6 values ('2025-01-01 00:00:11', 222);",
                "insert into tb6 values ('2025-01-01 00:00:12', 223);",
                "insert into tb6 values ('2025-01-01 00:00:13', 100);",
            ])

        def check1(self):
            # Only window2 appears; window1 filtered by 3s requirement.
            tdSql.checkResultsByFunc(
                sql="select ts, cnt from out6 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10.000")
                and tdSql.compareData(0, 1, 4),
            )

    # ── s7: start(2s or count 2) ────────────────────────────────────────────
    class StartOrDurCount(StreamCheckItem):
        def __init__(self):
            self.db = "db_s7"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb7 using meters tags(1);")
            tdSql.execute(
                "create stream s7 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(start(2s or count 2)) "
                "FROM tb7 PARTITION BY tbname "
                "INTO out7 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # t=01 v=221 → start streak=1 (firstTs=01), OR: count=1<2 & dur=0<2s → not yet
            # t=02 v=222 → start streak=2, OR: count=2>=2 → window opens skey=01 (firstTs)
            # t=03 v=100 → end → window closes ekey=03 cnt=3 [t01..t03]
            tdSql.executes([
                "insert into tb7 values ('2025-01-01 00:00:01', 221);",
                "insert into tb7 values ('2025-01-01 00:00:02', 222);",
                "insert into tb7 values ('2025-01-01 00:00:03', 100);",
            ])

        def check1(self):
            # 1 window skey=01 cnt=3. OR fired at count=2 before 2s elapsed.
            tdSql.checkResultsByFunc(
                sql="select ts, cnt from out7 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01.000")
                and tdSql.compareData(0, 1, 3),
            )

    # ── s8: start(2s and count 3) ───────────────────────────────────────────
    class StartAndDurCount(StreamCheckItem):
        def __init__(self):
            self.db = "db_s8"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb8 using meters tags(1);")
            tdSql.execute(
                "create stream s8 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(start(2s and count 3)) "
                "FROM tb8 PARTITION BY tbname "
                "INTO out8 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # t=00 v=221 → start, count=1, dur=0s  (firstTs=00) — AND: false
            # t=01 v=222 → start, count=2, dur=1s  — AND: dur<2s → false
            # t=02 v=223 → start, count=3, dur=2s  — AND: dur>=2s & count>=3 → window opens skey=00 (firstTs)
            # t=03 v=100 → end → window closes ekey=03 cnt=4 [t00..t03]
            tdSql.executes([
                "insert into tb8 values ('2025-01-01 00:00:00', 221);",
                "insert into tb8 values ('2025-01-01 00:00:01', 222);",
                "insert into tb8 values ('2025-01-01 00:00:02', 223);",
                "insert into tb8 values ('2025-01-01 00:00:03', 100);",
            ])

        def check1(self):
            # 1 window skey=00 cnt=4. AND delayed open vs OR (s7 opens at skey=01 due to count=2).
            tdSql.checkResultsByFunc(
                sql="select ts, cnt from out8 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                and tdSql.compareData(0, 1, 4),
            )

    # ── s9: sub-event + true_for(start/end) rejected at parse time ──────────
    class SubEventStartCountIgnored(StreamCheckItem):
        """true_for(start(...)) is NOT supported in sub-event windows.
        Verifies that the parser rejects such a CREATE STREAM statement.

        Schema: voltage only.
          cond1: voltage >= 220 AND voltage < 250
          cond2: voltage >= 250
          end  : voltage < 220
        """

        def __init__(self):
            self.db = "db_s9"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb9 using meters tags(1);")
            # sub-event + true_for(start(...)) must be rejected by the parser
            tdSql.error(
                "create stream s9 "
                "EVENT_WINDOW (START WITH (voltage >= 220 AND voltage < 250, voltage >= 250) "
                "END WITH voltage < 220) "
                "true_for(start(count 2)) "
                "FROM tb9 PARTITION BY tbname "
                "INTO out9 "
                "AS SELECT _twstart ts, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            pass  # stream was rejected, nothing to insert

        def check1(self):
            pass  # error already validated in create()

    # ── s10: sub-event + true_for(start/end) must be rejected at parse time ──
    class SubEventTrueForRejected(StreamCheckItem):
        """true_for(start(...)) and true_for(end(...)) are NOT supported in sub-event
        windows (START WITH multiple conditions). The parser must return an error.

        Schema: voltage only.
          cond1: voltage >= 220 AND voltage < 250
          cond2: voltage >= 250
          end  : voltage < 220
        """

        def __init__(self):
            self.db = "db_s11"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb11 using meters tags(1);")
            # true_for(start(...)) with sub-event must be rejected
            tdSql.error(
                "create stream s11_start "
                "EVENT_WINDOW (START WITH (voltage >= 220 AND voltage < 250, voltage >= 250) "
                "END WITH voltage < 220) "
                "true_for(start(count 2)) "
                "FROM tb11 PARTITION BY tbname "
                "INTO out11_start "
                "AS SELECT _twstart ts, count(voltage) cnt FROM %%trows;"
            )
            # true_for(end(...)) with sub-event must also be rejected
            tdSql.error(
                "create stream s11_end "
                "EVENT_WINDOW (START WITH (voltage >= 220 AND voltage < 250, voltage >= 250) "
                "END WITH voltage < 220) "
                "true_for(end(count 2)) "
                "FROM tb11 PARTITION BY tbname "
                "INTO out11_end "
                "AS SELECT _twstart ts, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            pass  # no streams created, nothing to insert

        def check1(self):
            pass  # errors already validated in create()

    # ═══════════════════════════════════════════════════════════════════════════
    # Argument-ordering tests
    # ═══════════════════════════════════════════════════════════════════════════

    def test_truefor_arg_order(self):
        """Verify true_for() arguments can appear in any order.

        Catalog:
            - Streams:Others

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-05-16 Created

        Test cases:
            o1  end(2s), start(count 2)                      — 2-arg E,S (end=duration, start=count)
            o2  end(count 2), count 3                        — 2-arg E,W (end=count, window=count)
            o3  start(2s), count 3                           — 2-arg S,W (start=duration, window=count)
            o4  count 3, end(2s), start(count 2)             — 3-arg W,E,S (end=duration)
            o5  start(2s AND count 2), count 3, end(count 2) — 3-arg S,W,E (start=AND combo)
            o6  end(2s OR count 2), start(count 2), count 3  — 3-arg E,S,W (end=OR combo)
            o7  end(count 2), count 3, start(2s AND count 2) — 3-arg E,W,S (start=AND combo)
            o8  start(2s OR count 2), end(count 2), count 3  — 3-arg S,E,W (start=OR combo)
            o9  end(2s OR count 2), count 10, start(2s AND count 2) — E,W,S filter: 6-row window
                                                               filtered by count 10
        """

        tdStream.checkAll([
            self.TrueForOrderES(),
            self.TrueForOrderEW(),
            self.TrueForOrderSW(),
            self.TrueForOrderWES(),
            self.TrueForOrderSWE(),
            self.TrueForOrderESW(),
            self.TrueForOrderEWS(),
            self.TrueForOrderSEW(),
            self.TrueForOrderWindowFilter(),
        ])

    # Shared data helper: timestamps 2 s apart so duration-based and count-based
    # streak conditions fire simultaneously (e.g. "2s" fires at the same row as
    # "count 2").
    #
    # Timeline (each step is 2 s):
    #   t=02 v=221 → start streak=1
    #   t=04 v=100 → NOT start → reset streak
    #   t=06 v=222 → start streak=1 (firstTs=t06=00:00:06)
    #   t=08 v=223 → start streak=2, dur=2 s ≥ 2 s → count 2 AND 2s both fire
    #   t=10 v=224 → extra in-window row
    #   t=12 v=100 → end streak=1 (firstTs=t12=00:00:12), dur=0 s
    #   t=14 v=221 → NOT end → reset end streak
    #   t=16 v=100 → end streak=1 (firstTs=t16=00:00:16), dur=0 s
    #   t=18 v=099 → end streak=2, dur=2 s ≥ 2 s → close; ekey=t16 (firstTs)
    #
    # Window: rows t06, t08, t10, t12, t14, t16 = 6 rows
    # Expected for any satisfied condition: skey=t06 ekey=t16 cnt=6
    _ORDER_INSERTS = [
        "insert into {tb} values ('2025-01-01 00:00:02', 221);",
        "insert into {tb} values ('2025-01-01 00:00:04', 100);",
        "insert into {tb} values ('2025-01-01 00:00:06', 222);",
        "insert into {tb} values ('2025-01-01 00:00:08', 223);",
        "insert into {tb} values ('2025-01-01 00:00:10', 224);",
        "insert into {tb} values ('2025-01-01 00:00:12', 100);",
        "insert into {tb} values ('2025-01-01 00:00:14', 221);",
        "insert into {tb} values ('2025-01-01 00:00:16', 100);",
        "insert into {tb} values ('2025-01-01 00:00:18', 099);",
    ]

    # ── o1: end(2s), start(count 2)  ────────────────────────────────────────
    class TrueForOrderES(StreamCheckItem):
        """2-arg E,S reversed order.  end uses duration streak (2s), start uses
        count streak (count 2).  Both fire at the same data row due to 2 s spacing.
        Expected: skey=t06, ekey=t16, cnt=6."""

        def __init__(self):
            self.db = "db_o1"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_o1 using meters tags(1);")
            tdSql.execute(
                "create stream s_o1 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(end(2s), start(count 2)) "
                "FROM tb_o1 PARTITION BY tbname "
                "INTO out_o1 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            tdSql.executes([s.format(tb="tb_o1") for s in TestTrueFor._ORDER_INSERTS])

        def check1(self):
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_o1 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:16.000")
                and tdSql.compareData(0, 2, 6),
            )

    # ── o2: end(count 2), count 3  ──────────────────────────────────────────
    class TrueForOrderEW(StreamCheckItem):
        """2-arg E,W order: end uses count streak, window_limit uses count.
        true_for(end(count 2), count 3): no start_limit → window opens immediately at t02.
        End streak fires at t18 (count=2, firstTs=t16), ekey=t16.
        Window rows t02..t16 = 8 rows ≥ 3 → passes.
        Expected: skey=t02, ekey=t16, cnt=8."""

        def __init__(self):
            self.db = "db_o2"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_o2 using meters tags(1);")
            tdSql.execute(
                "create stream s_o2 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(end(count 2), count 3) "
                "FROM tb_o2 PARTITION BY tbname "
                "INTO out_o2 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            tdSql.executes([s.format(tb="tb_o2") for s in TestTrueFor._ORDER_INSERTS])

        def check1(self):
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_o2 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:02.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:16.000")
                and tdSql.compareData(0, 2, 8),
            )

    # ── o3: start(2s), count 3  ─────────────────────────────────────────────
    class TrueForOrderSW(StreamCheckItem):
        """2-arg S,W order: start uses duration streak (2s), window_limit uses count.
        true_for(start(2s), count 3): start streak fires at t08 (t08-t06=2s), skey=t06.
        No end_limit → window closes immediately at first end-condition row t12, ekey=t12.
        Window rows t06..t12 = 4 rows ≥ 3 → passes.
        Expected: skey=t06, ekey=t12, cnt=4."""

        def __init__(self):
            self.db = "db_o3"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_o3 using meters tags(1);")
            tdSql.execute(
                "create stream s_o3 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(start(2s), count 3) "
                "FROM tb_o3 PARTITION BY tbname "
                "INTO out_o3 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            tdSql.executes([s.format(tb="tb_o3") for s in TestTrueFor._ORDER_INSERTS])

        def check1(self):
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_o3 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:12.000")
                and tdSql.compareData(0, 2, 4),
            )

    # ── o4: count 3, end(2s), start(count 2)  ───────────────────────────────
    class TrueForOrderWES(StreamCheckItem):
        """3-arg W,E,S order.  end uses duration streak (2s), start uses count.
        true_for(count 3, end(2s), start(count 2)).
        At t18: firstTs=t16, t18-t16=2s ≥ 2s → end fires; window 6 rows ≥ 3.
        Expected: skey=t06, ekey=t16, cnt=6."""

        def __init__(self):
            self.db = "db_o4"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_o4 using meters tags(1);")
            tdSql.execute(
                "create stream s_o4 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(count 3, end(2s), start(count 2)) "
                "FROM tb_o4 PARTITION BY tbname "
                "INTO out_o4 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            tdSql.executes([s.format(tb="tb_o4") for s in TestTrueFor._ORDER_INSERTS])

        def check1(self):
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_o4 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:16.000")
                and tdSql.compareData(0, 2, 6),
            )

    # ── o5: start(2s AND count 2), count 3, end(count 2)  ───────────────────
    class TrueForOrderSWE(StreamCheckItem):
        """3-arg S,W,E order.  start uses AND combo streak (both 2s and count 2).
        true_for(start(2s AND count 2), count 3, end(count 2)).
        At t08: count=2 AND dur=2s → both satisfied simultaneously → streak fires.
        Expected: skey=t06, ekey=t16, cnt=6."""

        def __init__(self):
            self.db = "db_o5"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_o5 using meters tags(1);")
            tdSql.execute(
                "create stream s_o5 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(start(2s AND count 2), count 3, end(count 2)) "
                "FROM tb_o5 PARTITION BY tbname "
                "INTO out_o5 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            tdSql.executes([s.format(tb="tb_o5") for s in TestTrueFor._ORDER_INSERTS])

        def check1(self):
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_o5 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:16.000")
                and tdSql.compareData(0, 2, 6),
            )

    # ── o6: end(2s OR count 2), start(count 2), count 3  ────────────────────
    class TrueForOrderESW(StreamCheckItem):
        """3-arg E,S,W order.  end uses OR combo streak (2s or count 2).
        true_for(end(2s OR count 2), start(count 2), count 3).
        At t18: count=2 OR dur=2s → both satisfied → fires on whichever threshold
        comes first (here both at t18 simultaneously).
        Expected: skey=t06, ekey=t16, cnt=6."""

        def __init__(self):
            self.db = "db_o6"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_o6 using meters tags(1);")
            tdSql.execute(
                "create stream s_o6 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(end(2s OR count 2), start(count 2), count 3) "
                "FROM tb_o6 PARTITION BY tbname "
                "INTO out_o6 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            tdSql.executes([s.format(tb="tb_o6") for s in TestTrueFor._ORDER_INSERTS])

        def check1(self):
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_o6 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:16.000")
                and tdSql.compareData(0, 2, 6),
            )

    # ── o7: end(count 2), count 3, start(2s AND count 2)  ───────────────────
    class TrueForOrderEWS(StreamCheckItem):
        """3-arg E,W,S order.  start uses AND combo streak.
        true_for(end(count 2), count 3, start(2s AND count 2)).
        Expected: skey=t06, ekey=t16, cnt=6."""

        def __init__(self):
            self.db = "db_o7"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_o7 using meters tags(1);")
            tdSql.execute(
                "create stream s_o7 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(end(count 2), count 3, start(2s AND count 2)) "
                "FROM tb_o7 PARTITION BY tbname "
                "INTO out_o7 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            tdSql.executes([s.format(tb="tb_o7") for s in TestTrueFor._ORDER_INSERTS])

        def check1(self):
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_o7 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:16.000")
                and tdSql.compareData(0, 2, 6),
            )

    # ── o8: start(2s OR count 2), end(count 2), count 3  ────────────────────
    class TrueForOrderSEW(StreamCheckItem):
        """3-arg S,E,W order.  start uses OR combo streak.
        true_for(start(2s OR count 2), end(count 2), count 3).
        At t08: dur=2s ≥ 2s OR count=2 → OR fires immediately on first satisfied.
        Expected: skey=t06, ekey=t16, cnt=6."""

        def __init__(self):
            self.db = "db_o8"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_o8 using meters tags(1);")
            tdSql.execute(
                "create stream s_o8 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(start(2s OR count 2), end(count 2), count 3) "
                "FROM tb_o8 PARTITION BY tbname "
                "INTO out_o8 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            tdSql.executes([s.format(tb="tb_o8") for s in TestTrueFor._ORDER_INSERTS])

        def check1(self):
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_o8 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:16.000")
                and tdSql.compareData(0, 2, 6),
            )

    # ── o9: window_limit actually filters (non-canonical E,W,S order) ───────
    class TrueForOrderWindowFilter(StreamCheckItem):
        """Prove window_limit in non-canonical order (E,W,S) really filters.

        true_for(end(2s OR count 2), count 10, start(2s AND count 2)):
        window must have ≥10 rows; data produces only 6 → filtered (0 results).
        Also exercises OR on end and AND on start simultaneously.
        """

        def __init__(self):
            self.db = "db_o9"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_o9 using meters tags(1);")
            tdSql.execute(
                "create stream s_o9 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(end(2s OR count 2), count 10, start(2s AND count 2)) "
                "FROM tb_o9 PARTITION BY tbname "
                "INTO out_o9 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            tdSql.executes([s.format(tb="tb_o9") for s in TestTrueFor._ORDER_INSERTS])

        def check1(self):
            # window_limit=10 not satisfied (6 rows < 10) → 0 rows expected.
            # Cannot use checkResultsByFunc for 0-row checks: query() returns 0 (falsy)
            # so func() is never called and the loop hangs for 300 s.
            # By the time this check runs, o1-o8 checks have already completed,
            # giving the stream ample processing time.
            import time
            time.sleep(1)
            tdSql.query("select ts, te, cnt from out_o9 order by ts")
            tdSql.checkRows(0)

    # ════════════════════════════════════════════════════════════════════════
    # test_truefor_query_event_window
    # ════════════════════════════════════════════════════════════════════════

    def test_truefor_query_event_window(self):
        """true_for start/end streak tests for direct SELECT EVENT_WINDOW queries.

        Catalog:
            - Streams:Others

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-05-16 Created

        Unlike stream tests, these use synchronous SELECT … EVENT_WINDOW queries
        (no CREATE STREAM).  Results are available immediately after INSERT, so
        no retry loop is needed.

        Query syntax used:
            SELECT _wstart, _wend, count(voltage) cnt
            FROM <table>
            EVENT_WINDOW START WITH voltage >= 220 END WITH voltage < 220
            true_for(…)

        Test cases:
            q1  start(count 2)              : window opens only after 2 consecutive start rows
            q2  end(count 2)                : window closes only after 2 consecutive end rows
            q3  start(count 2), end(count 2): both conditions require independent streaks
            q4  start(2s), end(2s)          : duration-based streaks (2 s timestamp spacing)
            q5  count 3                     : window_limit — window with < 3 rows is dropped
        """

        tdSql.execute("create database qdb vgroups 1")
        tdSql.execute("use qdb")
        tdSql.execute(
            "create table tb_q (ts timestamp, voltage int)"
        )

        # ── q1: start(count 2) ───────────────────────────────────────────────
        # t01=221 streak=1; t02=100 reset; t03=222 streak=1(firstTs=t03);
        # t04=223 streak=2 → open skey=t03; t05=100 → close ekey=t05 cnt=3
        # t06=221 streak=1(firstTs=t06); t07=222 streak=2 → open skey=t06;
        # t08=100 → close ekey=t08 cnt=3
        # Expected: 2 windows
        tdSql.execute("delete from tb_q")
        tdSql.executes([
            "insert into tb_q values ('2025-01-01 00:00:01', 221);",
            "insert into tb_q values ('2025-01-01 00:00:02', 100);",
            "insert into tb_q values ('2025-01-01 00:00:03', 222);",
            "insert into tb_q values ('2025-01-01 00:00:04', 223);",
            "insert into tb_q values ('2025-01-01 00:00:05', 100);",
            "insert into tb_q values ('2025-01-01 00:00:06', 221);",
            "insert into tb_q values ('2025-01-01 00:00:07', 222);",
            "insert into tb_q values ('2025-01-01 00:00:08', 100);",
        ])
        tdSql.query(
            "SELECT _wstart ts, _wend te, count(voltage) cnt FROM tb_q "
            "EVENT_WINDOW START WITH voltage >= 220 END WITH voltage < 220 "
            "true_for(start(count 2)) ORDER BY ts"
        )
        tdSql.checkRows(2)
        tdSql.compareData(0, 0, "2025-01-01 00:00:03.000")
        tdSql.compareData(0, 1, "2025-01-01 00:00:05.000")
        tdSql.compareData(0, 2, 3)
        tdSql.compareData(1, 0, "2025-01-01 00:00:06.000")
        tdSql.compareData(1, 1, "2025-01-01 00:00:08.000")
        tdSql.compareData(1, 2, 3)

        # ── q2: end(count 2) ─────────────────────────────────────────────────
        # t01=221 open(skey=t01); t02=100 end streak=1(firstTs=t02);
        # t03=221 reset end streak; t04=100 end streak=1(firstTs=t04);
        # t05=99  end streak=2 → close ekey=t04 cnt=4 [t01..t04]
        # Expected: 1 window
        tdSql.execute("delete from tb_q")
        tdSql.executes([
            "insert into tb_q values ('2025-01-01 00:00:01', 221);",
            "insert into tb_q values ('2025-01-01 00:00:02', 100);",
            "insert into tb_q values ('2025-01-01 00:00:03', 221);",
            "insert into tb_q values ('2025-01-01 00:00:04', 100);",
            "insert into tb_q values ('2025-01-01 00:00:05',  99);",
        ])
        tdSql.query(
            "SELECT _wstart ts, _wend te, count(voltage) cnt FROM tb_q "
            "EVENT_WINDOW START WITH voltage >= 220 END WITH voltage < 220 "
            "true_for(end(count 2)) ORDER BY ts"
        )
        tdSql.checkRows(1)
        tdSql.compareData(0, 0, "2025-01-01 00:00:01.000")
        tdSql.compareData(0, 1, "2025-01-01 00:00:04.000")
        tdSql.compareData(0, 2, 4)

        # ── q3: start(count 2) + end(count 2) ───────────────────────────────
        # t01=221 start streak=1; t02=100 reset; t03=222 streak=1(firstTs=t03);
        # t04=223 streak=2 → open skey=t03; t05=100 end streak=1(firstTs=t05);
        # t06=221 reset end streak; t07=100 end streak=1(firstTs=t07);
        # t08=99  end streak=2 → close ekey=t07 cnt=5 [t03..t07]
        # Expected: 1 window
        tdSql.execute("delete from tb_q")
        tdSql.executes([
            "insert into tb_q values ('2025-01-01 00:00:01', 221);",
            "insert into tb_q values ('2025-01-01 00:00:02', 100);",
            "insert into tb_q values ('2025-01-01 00:00:03', 222);",
            "insert into tb_q values ('2025-01-01 00:00:04', 223);",
            "insert into tb_q values ('2025-01-01 00:00:05', 100);",
            "insert into tb_q values ('2025-01-01 00:00:06', 221);",
            "insert into tb_q values ('2025-01-01 00:00:07', 100);",
            "insert into tb_q values ('2025-01-01 00:00:08',  99);",
        ])
        tdSql.query(
            "SELECT _wstart ts, _wend te, count(voltage) cnt FROM tb_q "
            "EVENT_WINDOW START WITH voltage >= 220 END WITH voltage < 220 "
            "true_for(start(count 2), end(count 2)) ORDER BY ts"
        )
        tdSql.checkRows(1)
        tdSql.compareData(0, 0, "2025-01-01 00:00:03.000")
        tdSql.compareData(0, 1, "2025-01-01 00:00:07.000")
        tdSql.compareData(0, 2, 5)

        # ── q4: start(2s), end(2s) ───────────────────────────────────────────
        # 2-second timestamp spacing: t02, t04, t06, t08, t10, t12, t14, t16, t18
        # t02=221 start streak=1(firstTs=t02, dur=0s); t04=100 reset;
        # t06=222 streak=1(firstTs=t06); t08=223 dur=t08-t06=2s → open skey=t06;
        # t10=224 in-window; t12=100 end streak=1(firstTs=t12, dur=0s);
        # t14=221 reset end streak; t16=100 end streak=1(firstTs=t16);
        # t18=99  dur=t18-t16=2s → close ekey=t16 cnt=6 [t06..t16]
        # Expected: 1 window
        tdSql.execute("delete from tb_q")
        tdSql.executes([
            "insert into tb_q values ('2025-01-01 00:00:02', 221);",
            "insert into tb_q values ('2025-01-01 00:00:04', 100);",
            "insert into tb_q values ('2025-01-01 00:00:06', 222);",
            "insert into tb_q values ('2025-01-01 00:00:08', 223);",
            "insert into tb_q values ('2025-01-01 00:00:10', 224);",
            "insert into tb_q values ('2025-01-01 00:00:12', 100);",
            "insert into tb_q values ('2025-01-01 00:00:14', 221);",
            "insert into tb_q values ('2025-01-01 00:00:16', 100);",
            "insert into tb_q values ('2025-01-01 00:00:18',  99);",
        ])
        tdSql.query(
            "SELECT _wstart ts, _wend te, count(voltage) cnt FROM tb_q "
            "EVENT_WINDOW START WITH voltage >= 220 END WITH voltage < 220 "
            "true_for(start(2s), end(2s)) ORDER BY ts"
        )
        tdSql.checkRows(1)
        tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
        tdSql.compareData(0, 1, "2025-01-01 00:00:16.000")
        tdSql.compareData(0, 2, 6)

        # ── q5: count 3 (window_limit) ───────────────────────────────────────
        # Data: t01=221 open, t02=100 → 2-row window → filtered (< 3 rows)
        #       t03=222 open(no start_limit), t04=223, t05=224, t06=100 → 4-row window passes
        # Expected: 1 window (the second one, with 4 rows)
        tdSql.execute("delete from tb_q")
        tdSql.executes([
            "insert into tb_q values ('2025-01-01 00:00:01', 221);",
            "insert into tb_q values ('2025-01-01 00:00:02', 100);",
            "insert into tb_q values ('2025-01-01 00:00:03', 222);",
            "insert into tb_q values ('2025-01-01 00:00:04', 223);",
            "insert into tb_q values ('2025-01-01 00:00:05', 224);",
            "insert into tb_q values ('2025-01-01 00:00:06', 100);",
        ])
        tdSql.query(
            "SELECT _wstart ts, _wend te, count(voltage) cnt FROM tb_q "
            "EVENT_WINDOW START WITH voltage >= 220 END WITH voltage < 220 "
            "true_for(count 3) ORDER BY ts"
        )
        tdSql.checkRows(1)
        tdSql.compareData(0, 0, "2025-01-01 00:00:03.000")
        tdSql.compareData(0, 1, "2025-01-01 00:00:06.000")
        tdSql.compareData(0, 2, 4)

    # ════════════════════════════════════════════════════════════════════════
    # test_truefor_restart_before_close
    # ════════════════════════════════════════════════════════════════════════

    def test_truefor_restart_before_close(self):
        """Restart stream (stop/start) while a streak is in progress.

        Catalog:
            - Streams:Others

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-05-18 Created

        Validates the doneVer freeze fix: when a group has an in-progress streak
        at checkpoint time, doneVer is frozen so the streak rows are replayed from
        WAL on restart and the streak state is rebuilt naturally.

        Three streams run in parallel, each covering a different streak scenario:

        r1 (EndStreakRestartBeforeClose) — end-only streak, restart during end streak:
            t=01 v=221  window1 opens  (skey=01)
            t=02 v=100  end streak=1
            t=03 v=99   end streak=2   window1 closes (ekey=02, cnt=2)
            ── check1: 1 window ──────────────────────────────────────────
            t=04 v=221  window2 opens  (skey=04)
            t=05 v=100  end streak=1   ← streak in progress
            STOP/START  (doneVer frozen before t=05; t=05 replayed)
            ── check2: 1 window (window2 open, not closed) ───────────────
            t=06 v=99   end streak=2   window2 closes (ekey=05, cnt=2)
            ── check3: 2 windows ─────────────────────────────────────────

        r2 (StartStreakRestartBeforeOpen) — start-only streak, restart before window opens:
            t=01 v=221  start streak=1
            t=02 v=221  start streak=2
            t=03 v=221  start streak=3  window1 opens (skey=01)
            t=04 v=100  end condition   window1 closes (ekey=04, cnt=4)
            ── check1: 1 window ──────────────────────────────────────────
            t=05 v=221  start streak=1
            t=06 v=221  start streak=2  ← streak in progress (window not open)
            STOP/START  (doneVer frozen before t=05; t=05,t=06 replayed)
            ── check2: 1 window (window2 not open) ───────────────────────
            t=07 v=221  start streak=3  window2 opens (skey=05)
            t=08 v=100  end condition   window2 closes (ekey=08, cnt=4)
            ── check3: 2 windows ─────────────────────────────────────────

        r3 (BothStreakRestart) — start+end streak, restart during end streak:
            t=01 v=221  start streak=1
            t=02 v=221  start streak=2  window1 opens (skey=01)
            t=03 v=100  end streak=1
            t=04 v=99   end streak=2    window1 closes (ekey=03, cnt=3)
            ── check1: 1 window ──────────────────────────────────────────
            t=05 v=221  start streak=1
            t=06 v=221  start streak=2  window2 opens (skey=05)
            t=07 v=100  end streak=1    ← end streak in progress
            STOP/START  (doneVer frozen before t=07; t=07 replayed)
            ── check2: 1 window (window2 open, not closed) ───────────────
            t=08 v=99   end streak=2    window2 closes (ekey=07, cnt=3)
            ── check3: 2 windows ─────────────────────────────────────────
        """

        tdStream.checkAll([
            self.EndStreakRestartBeforeClose(),
            self.StartStreakRestartBeforeOpen(),
            self.BothStreakRestart(),
        ])

    # ── r1: end streak restart before close ─────────────────────────────────
    class EndStreakRestartBeforeClose(StreamCheckItem):

        def __init__(self):
            self.db = "db_r1"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_r using meters tags(1);")
            tdSql.execute(
                "create stream s_restart "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(end(count 2)) "
                "FROM tb_r PARTITION BY tbname "
                "INTO out_restart "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # Phase 1: complete window1.
            # t=01 → opens window, t=02 → end streak=1, t=03 → end streak=2 → closes.
            tdSql.executes([
                "insert into tb_r values ('2025-01-01 00:00:01', 221);",
                "insert into tb_r values ('2025-01-01 00:00:02', 100);",
                "insert into tb_r values ('2025-01-01 00:00:03',  99);",
            ])

        def check1(self):
            # Synchronisation barrier: wait until window1 is confirmed closed.
            # This guarantees the stream has processed t=01..t=03 before we
            # proceed to the stop/start phase.
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_restart order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:02.000")
                and tdSql.compareData(0, 2, 2),
            )

        def insert2(self):
            import time
            # Phase 2: open window2 and advance end streak to count=1.
            tdSql.executes([
                "insert into tb_r values ('2025-01-01 00:00:04', 221);",
                "insert into tb_r values ('2025-01-01 00:00:05', 100);",
            ])
            # Allow the stream to process these two rows so the in-progress streak
            # is visible at checkpoint time.
            time.sleep(3)
            # Stop the stream: triggers a checkpoint.  Our WAL-rewind fix caps
            # doneVer to just before t=05's WAL entry.
            tdSql.execute("stop stream s_restart")
            # Restart: WAL replay begins from the rewound doneVer, re-processing
            # t=05 and rebuilding streak=1 naturally.
            tdSql.execute("start stream s_restart")

        def check2(self):
            import time
            # Give the stream time to finish WAL replay after restart.
            # Window2 is open (skey=04) with end streak=1 but not yet closed.
            # Output must still show only the original window1.
            time.sleep(3)
            tdSql.query("select ts, te, cnt from out_restart order by ts")
            tdSql.checkRows(1)

        def insert3(self):
            # Phase 3: deliver the second consecutive end-condition row.
            # This makes end streak=2, satisfying true_for(end(count 2)).
            tdSql.executes([
                "insert into tb_r values ('2025-01-01 00:00:06',  99);",
            ])

        def check3(self):
            # Both windows must appear: window1 (t01-t02, cnt=2) and
            # window2 (t04-t05, cnt=2).
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_restart order by ts",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:02.000")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:04.000")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:05.000")
                and tdSql.compareData(1, 2, 2),
            )

    # ── r2: start streak restart before window open ──────────────────────────
    class StartStreakRestartBeforeOpen(StreamCheckItem):

        def __init__(self):
            self.db = "db_r2"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_r using meters tags(1);")
            tdSql.execute(
                "create stream s_r2 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(start(count 3)) "
                "FROM tb_r PARTITION BY tbname "
                "INTO out_r2 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # Phase 1: complete window1.
            # t=01..t=03: start streak=3 → window opens (skey=01); t=04: closes.
            tdSql.executes([
                "insert into tb_r values ('2025-01-01 00:00:01', 221);",
                "insert into tb_r values ('2025-01-01 00:00:02', 221);",
                "insert into tb_r values ('2025-01-01 00:00:03', 221);",
                "insert into tb_r values ('2025-01-01 00:00:04', 100);",
            ])

        def check1(self):
            # Sync barrier: wait for window1 [t01, t04, cnt=4].
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_r2 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04.000")
                and tdSql.compareData(0, 2, 4),
            )

        def insert2(self):
            import time
            # Phase 2: advance start streak to count=2 (threshold=3, not yet open).
            tdSql.executes([
                "insert into tb_r values ('2025-01-01 00:00:05', 221);",
                "insert into tb_r values ('2025-01-01 00:00:06', 221);",
            ])
            # Wait for the stream to process both rows so startCondCount=2 is
            # stable at checkpoint time.
            time.sleep(3)
            # STOP triggers a checkpoint; doneVer must be frozen before t=05.
            tdSql.execute("stop stream s_r2")
            # START replays from the frozen doneVer, rebuilding startCondCount=2.
            tdSql.execute("start stream s_r2")

        def check2(self):
            import time
            # After restart streak=2 is rebuilt but window has not opened yet.
            time.sleep(3)
            tdSql.query("select ts, te, cnt from out_r2 order by ts")
            tdSql.checkRows(1)

        def insert3(self):
            # Phase 3: third streak row satisfies count=3; window opens at skey=05.
            tdSql.executes([
                "insert into tb_r values ('2025-01-01 00:00:07', 221);",
                "insert into tb_r values ('2025-01-01 00:00:08', 100);",
            ])

        def check3(self):
            # window1: [t01, t04, cnt=4]; window2: [t05, t08, cnt=4].
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_r2 order by ts",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04.000")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05.000")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:08.000")
                and tdSql.compareData(1, 2, 2),
            )

    # ── r3: both start+end streak, restart during end streak ─────────────────
    class BothStreakRestart(StreamCheckItem):

        def __init__(self):
            self.db = "db_r3"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_r using meters tags(1);")
            tdSql.execute(
                "create stream s_r3 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(start(count 2), end(count 2)) "
                "FROM tb_r PARTITION BY tbname "
                "INTO out_r3 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # Phase 1: complete window1 via both start and end streaks.
            tdSql.executes([
                "insert into tb_r values ('2025-01-01 00:00:01', 221);",
                "insert into tb_r values ('2025-01-01 00:00:02', 221);",
                "insert into tb_r values ('2025-01-01 00:00:03', 100);",
                "insert into tb_r values ('2025-01-01 00:00:04',  99);",
            ])

        def check1(self):
            # Sync barrier: window1 [t01, t03, cnt=3].
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_r3 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03.000")
                and tdSql.compareData(0, 2, 3),
            )

        def insert2(self):
            import time
            # Phase 2: open window2 (start streak satisfied), then first end-streak row.
            tdSql.executes([
                "insert into tb_r values ('2025-01-01 00:00:05', 221);",
                "insert into tb_r values ('2025-01-01 00:00:06', 221);",
                "insert into tb_r values ('2025-01-01 00:00:07', 100);",
            ])
            # Wait for the stream to process all three rows so endCondCount=1 is
            # stable at checkpoint time.
            time.sleep(3)
            # STOP triggers checkpoint; doneVer frozen before t=07 (end streak row).
            tdSql.execute("stop stream s_r3")
            # START replays from frozen doneVer; t=07 re-processed → endCondCount=1.
            tdSql.execute("start stream s_r3")

        def check2(self):
            import time
            # Window2 is open with endCondCount=1 after replay; not yet closed.
            time.sleep(3)
            tdSql.query("select ts, te, cnt from out_r3 order by ts")
            tdSql.checkRows(1)

        def insert3(self):
            # Phase 3: second consecutive end-condition row → end streak=2 satisfied.
            tdSql.executes([
                "insert into tb_r values ('2025-01-01 00:00:08',  99);",
            ])

        def check3(self):
            # window1: [t01, t03, cnt=3]; window2: [t05, t07, cnt=3].
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_r3 order by ts",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03.000")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05.000")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:07.000")
                and tdSql.compareData(1, 2, 3),
            )

    # ════════════════════════════════════════════════════════════════════════
    # test_truefor_start_streak_cross_block
    # ════════════════════════════════════════════════════════════════════════

    def test_truefor_start_streak_cross_block(self):
        """Start-streak rows in separate batches must all contribute to window COUNT.

        Catalog:
            - Streams:Others

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-05-19 Created

        Regression test for the cross-block start-streak aggregation bug:
        when a start(count N) streak is satisfied across multiple SSDataBlocks,
        the executor previously clamped streakStartIdx to 0 inside the current
        block, silently dropping all streak rows from earlier blocks.  The fix
        aggregates each streak row eagerly (tentative agg) so all rows land in
        pInfo->pRow regardless of which block they arrived in.

        Three streams run in parallel, each exploiting a different cross-block
        scenario:

        cb1 (StartStreakSplit2Plus1) — count streak split 2+1:
            Batch 1: T1(start) T2(start)             → count=2, not satisfied
            ── check1: 0 windows ──────────────────────────────────────────
            Batch 2: T3(start) → count=3 opens (skey=T1)  T4(end) → closes
            ── check2: 1 window [skey=T1, cnt=4] ─────────────────────────
            Without fix: cnt=2 (only T3+T4 in current block counted).
            With fix:    cnt=4 (T1+T2 from batch 1 also counted).

        cb2 (StartStreakBreakThenCrossBlock) — streak breaks, retry spans 2 batches:
            Batch 1: T1(start) T2(start) T3(end-cond) → streak reset at T3
            ── check1: 0 windows ──────────────────────────────────────────
            Batch 2: T4(start) T5(start)              → count=2 of 3
            ── check2: 0 windows ──────────────────────────────────────────
            Batch 3: T6(start) → count=3 opens (skey=T4)  T7(end) → closes
            ── check3: 1 window [skey=T4, cnt=4] ─────────────────────────
            Verifies that tentative agg is correctly discarded on streak break
            and the subsequent cross-block streak works from a clean state.

        cb3 (StartStreakDurationCrossBlock) — duration streak split across batches:
            Batch 1: T1(start) at t=01s                          → duration=0 < 2s
            ── check1: 0 windows ──────────────────────────────────────────
            Batch 2: T2(start) at t=04s → duration=3s >= 2s  opens (skey=T1)
                     T3(end)   at t=05s → closes
            ── check2: 1 window [skey=T1, cnt=3] ─────────────────────────
            Without fix: cnt=2 (T1 from previous batch lost).
            With fix:    cnt=3.
        """

        tdStream.checkAll([
            self.StartStreakSplit2Plus1(),
            self.StartStreakBreakThenCrossBlock(),
            self.StartStreakDurationCrossBlock(),
        ])

    # ── cb1: count streak split 2+1 across two batches ──────────────────────
    class StartStreakSplit2Plus1(StreamCheckItem):

        def __init__(self):
            self.db = "db_cb1"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_cb1 using meters tags(1);")
            tdSql.execute(
                "create stream s_cb1 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(start(count 3)) "
                "FROM tb_cb1 PARTITION BY tbname "
                "INTO out_cb1 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # Batch 1: first two streak rows only (count=2 of 3 needed).
            tdSql.executes([
                "insert into tb_cb1 values ('2025-01-01 00:00:01', 221);",
                "insert into tb_cb1 values ('2025-01-01 00:00:02', 222);",
            ])

        def check1(self):
            import time
            # Allow batch 1 to be fully processed; no window should exist yet.
            time.sleep(2)
            tdSql.query("select ts, cnt from out_cb1 order by ts")
            tdSql.checkRows(0)

        def insert2(self):
            # Batch 2: third streak row satisfies count=3 (window opens skey=T1),
            # then end row closes it.
            tdSql.executes([
                "insert into tb_cb1 values ('2025-01-01 00:00:03', 223);",
                "insert into tb_cb1 values ('2025-01-01 00:00:04', 100);",
            ])

        def check2(self):
            # Window: skey=T1, ekey=T4.  cnt MUST be 4 (T1+T2 from batch 1 included).
            # A cnt of 2 (only T3+T4) indicates the cross-block bug is present.
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_cb1 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04.000")
                and tdSql.compareData(0, 2, 4),
            )

    # ── cb2: streak breaks, retry new streak spans two batches ───────────────
    class StartStreakBreakThenCrossBlock(StreamCheckItem):

        def __init__(self):
            self.db = "db_cb2"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_cb2 using meters tags(1);")
            tdSql.execute(
                "create stream s_cb2 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(start(count 3)) "
                "FROM tb_cb2 PARTITION BY tbname "
                "INTO out_cb2 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # Batch 1: two start rows then an end-condition row that breaks the streak.
            # This also verifies that tentative agg is correctly discarded on break.
            tdSql.executes([
                "insert into tb_cb2 values ('2025-01-01 00:00:01', 221);",
                "insert into tb_cb2 values ('2025-01-01 00:00:02', 222);",
                "insert into tb_cb2 values ('2025-01-01 00:00:03', 100);",
            ])

        def check1(self):
            import time
            # Streak was reset at T3; no window should exist.
            time.sleep(2)
            tdSql.query("select ts, cnt from out_cb2 order by ts")
            tdSql.checkRows(0)

        def insert2(self):
            # Batch 2: start a fresh streak with two rows (count=2 of 3).
            tdSql.executes([
                "insert into tb_cb2 values ('2025-01-01 00:00:04', 221);",
                "insert into tb_cb2 values ('2025-01-01 00:00:05', 222);",
            ])

        def check2(self):
            import time
            # Still count=2 of 3; no window yet.
            time.sleep(2)
            tdSql.query("select ts, cnt from out_cb2 order by ts")
            tdSql.checkRows(0)

        def insert3(self):
            # Batch 3: third streak row satisfies (skey=T4), end row closes.
            tdSql.executes([
                "insert into tb_cb2 values ('2025-01-01 00:00:06', 223);",
                "insert into tb_cb2 values ('2025-01-01 00:00:07', 100);",
            ])

        def check3(self):
            # Window: skey=T4, ekey=T7, cnt=4 (T4+T5 from batch 2, T6+T7 from batch 3).
            # skey must be T4 (not T1): the broken streak must be fully discarded.
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_cb2 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:04.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:07.000")
                and tdSql.compareData(0, 2, 4),
            )

    # ── cb3: duration-based streak split across two batches ──────────────────
    class StartStreakDurationCrossBlock(StreamCheckItem):

        def __init__(self):
            self.db = "db_cb3"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create stable meters (ts timestamp, voltage int) tags (gid int);")
            tdSql.execute("create table tb_cb3 using meters tags(1);")
            tdSql.execute(
                "create stream s_cb3 "
                "EVENT_WINDOW (START WITH voltage >= 220 END WITH voltage < 220) "
                "true_for(start(2s)) "
                "FROM tb_cb3 PARTITION BY tbname "
                "INTO out_cb3 "
                "AS SELECT _twstart ts, _twend te, count(voltage) cnt FROM %%trows;"
            )

        def insert1(self):
            # Batch 1: single start row; duration=0s < 2s, streak in progress.
            tdSql.executes([
                "insert into tb_cb3 values ('2025-01-01 00:00:01', 221);",
            ])

        def check1(self):
            import time
            # 0 elapsed since firstTs=T1; threshold not met.
            time.sleep(2)
            tdSql.query("select ts, cnt from out_cb3 order by ts")
            tdSql.checkRows(0)

        def insert2(self):
            # Batch 2: second start row at t=4s → duration=3s >= 2s → satisfied
            #          (skey=T1).  End row at t=5s closes the window.
            tdSql.executes([
                "insert into tb_cb3 values ('2025-01-01 00:00:04', 221);",
                "insert into tb_cb3 values ('2025-01-01 00:00:05', 100);",
            ])

        def check2(self):
            # Window: skey=T1(t=01s), ekey=T3(t=05s), cnt=3 (T1 from batch 1 included).
            # cnt=2 would indicate T1 was dropped (cross-block bug).
            tdSql.checkResultsByFunc(
                sql="select ts, te, cnt from out_cb3 order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05.000")
                and tdSql.compareData(0, 2, 3),
            )
