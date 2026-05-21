from new_test_framework.utils import tdLog, tdSql, tdStream, StreamCheckItem


class TestTrueFor:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_truefor_event_window(self):
        """true_for start/end streak tests for EVENT_WINDOW streams.

        Catalog:
            - Streams:Others

        Since: v3.3.3.x

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

        tdStream.createSnode()

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

        Since: v3.3.3.x

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
