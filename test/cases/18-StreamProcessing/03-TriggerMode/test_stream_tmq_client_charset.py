# -*- coding: utf-8 -*-
import os
import tempfile
import subprocess

from taos.tmq import Consumer
from new_test_framework.utils import tdLog, tdSql, tdStream, tdDnodes, tdCom


class TestStreamTmqClientCharset:
    """Creator-charset pass-down for stream computation AND TMQ subscriptions.

    Both streams and topics persist their query/filter as a serialized plan
    string that already carries the per-node charset name (phase-1
    serialization). At runtime the vnode rebuilds each node's charsetCxt from
    that name and per-node scalar eval applies it — so the *creator's* charset
    (not the server's) drives every NCHAR<->MBS conversion. NCHAR is stored as
    charset-independent UCS-4, so a GBK creator
    and a UTF-8 creator with the same logical predicate over the same data MUST
    produce identical results; a fallback to the server charset would mis-decode
    the creator's GBK literal bytes and diverge.

    Both stream and subscription are exercised in one cluster deployment (one CI
    case) across every NCHAR-bearing node type the codec serializes separately:
    VALUE/OPERATOR (=, IN, LIKE, range), FUNCTION (concat), CASE WHEN, plus the
    NCHAR-tag scan path and the stream trigger-side event-window condition.

    Every CREATE STREAM / CREATE TOPIC is issued through the `taos` CLI reading a
    -f file encoded in the creator's charset: taospy always encodes SQL as UTF-8
    and so cannot represent a real non-UTF-8 creator. Assertions read only ASCII
    (counts) or NCHAR *data* (UCS-4, safe via taospy) — never the raw stored SQL
    text (`ins_streams.sql`), whose GBK bytes crash taospy's UTF-8 decode.
    """

    updatecfgDict = {"charset": "UTF-8"}

    # Deterministic topic names, dropped defensively at setup and teardown: a
    # database with live topics cannot be dropped ("Topic must be dropped
    # first"), and cluster state can persist across reruns.
    TOPICS = [
        "tp_eq_g", "tp_in_g", "tp_like_g",
        "tp_concat_g", "tp_case_g", "tp_tag_g", "tp_eq_u",
    ]

    def _drop_topics(self):
        for t in self.TOPICS:
            tdSql.execute(f"drop topic if exists {t}")

    # ---- shared CLI-charset helpers (a creator's charset is only expressible
    # ---- via a -f file encoded in that charset) ----

    def _setup_cli(self):
        build = tdCom.getBuildPath()
        self.taos = os.path.join(build, "build", "bin", "taos")
        self.workdir = tempfile.mkdtemp(prefix="cs_stream_tmq_cli_")
        srccfg = os.path.join(tdDnodes.dnodes[0].cfgDir, "taos.cfg")
        with open(srccfg) as f:
            base = [
                ln for ln in f
                if not ln.strip().lower().startswith("charset")
            ]
        self.cfg = {}
        for cs in ("UTF-8", "GBK"):
            d = os.path.join(self.workdir, f"cfg_{cs}")
            os.makedirs(d, exist_ok=True)
            with open(os.path.join(d, "taos.cfg"), "w") as f:
                f.writelines(base)
                f.write(f"charset {cs}\n")
            self.cfg[cs] = d

    def _run_cli(self, charset, sql):
        # taos -f runs line by line, so each statement must be on ONE line.
        sql = " ".join(sql.split())
        fpath = os.path.join(self.workdir, "q.sql")
        with open(fpath, "wb") as fh:
            fh.write((sql + ";\n").encode(charset))
        proc = subprocess.run(
            [self.taos, "-c", self.cfg[charset], "-f", fpath],
            capture_output=True, timeout=60,
        )
        out = proc.stdout.decode(charset, "replace")
        err = proc.stderr.decode(charset, "replace")
        if proc.returncode != 0:
            tdLog.exit(
                f"""taos CLI failed charset={charset} rc={proc.returncode}
                sql={sql} stdout={out} stderr={err}"""
            )
        return out

    # ---- schema + data ----

    def _setup_schema(self):
        self._drop_topics()  # so a leftover ccst is droppable
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.execute("drop database if exists ccst")
        tdSql.execute(
            "create database ccst vgroups 2 wal_retention_period 3600"
        )
        tdSql.execute("use ccst")
        tdSql.execute(
            "create stable stb (ts timestamp, nc nchar(32), v int) "
            "tags(tnc nchar(32))"
        )
        # child tables created empty; data inserted AFTER streams are created.
        tdSql.execute("create table ct1 using stb tags('标签甲')")
        tdSql.execute("create table ct2 using stb tags('标签乙')")
        # event-window trigger source (normal table).
        tdSql.execute("create table ev (ts timestamp, nc nchar(32), v int)")

    def _insert_data(self):
        # stb data (for topics + calc streams). Matching counts over stb:
        #   nc='中文测试'            -> 3 (ct1x2 + ct2x1)
        #   nc in (中文测试,你好世界) -> 4
        #   nc like '中文%'          -> 4 (中文测试x3 + 中文abc x1)
        #   concat(nc,'尾')='中文测试尾' -> 3
        #   case nc when '中文测试' -> 3
        #   tag tnc='标签甲'          -> 5 (all ct1)
        ct1 = [
            ("2026-01-01 00:00:00.000", "中文测试", 1),
            ("2026-01-01 00:00:00.300", "中文测试", 2),
            ("2026-01-01 00:00:01.000", "你好世界", 3),
            ("2026-01-01 00:00:02.000", "中文abc", 4),
            ("2026-01-01 00:00:03.000", "ASCII", 5),
        ]
        for ts, nc, v in ct1:
            tdSql.execute(f"insert into ct1 values ('{ts}','{nc}',{v})")
        tdSql.execute(
            "insert into ct2 values ('2026-01-01 00:00:00.000','中文测试',6)"
        )
        # event-window data: window opens at nc='中文测试', closes at nc='结束'.
        ev = [
            ("2026-01-01 00:00:00.000", "其他", 1),
            ("2026-01-01 00:00:01.000", "中文测试", 2),
            ("2026-01-01 00:00:02.000", "数据", 3),
            ("2026-01-01 00:00:03.000", "结束", 4),
            ("2026-01-01 00:00:04.000", "其他", 5),
        ]
        for ts, nc, v in ev:
            tdSql.execute(f"insert into ev values ('{ts}','{nc}',{v})")

    # ---- TMQ subscription ----

    def _create_topic(self, charset, tname, where):
        self._run_cli(
            charset,
            f"use ccst; create topic {tname} as "
            f"select ts, v from stb where {where}",
        )

    def _consume(self, tname, gid):
        # Consumer charset is irrelevant (creator binding). Count delivered
        # rows; poll until 3 consecutive empty polls.
        consumer = Consumer({
            "group.id": gid,
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "msg.with.table.name": "false",
        })
        total = 0
        try:
            consumer.subscribe([tname])
            empty = 0
            while empty < 3:
                res = consumer.poll(1)
                if not res:
                    empty += 1
                    continue
                empty = 0
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    total += len(block.fetchall())
        finally:
            consumer.close()
        return total

    def _check_topics(self):
        # (label, WHERE, expected) — each exercises a distinct charset-bearer
        # node type / scan path. GBK is the discriminator; a server-charset
        # fallback would mis-decode the GBK literal and deliver 0 / wrong count.
        scenarios = [
            ("eq",     "nc = '中文测试'", 3),
            ("in",     "nc in ('中文测试','你好世界')", 4),
            ("like",   "nc like '中文%'", 4),
            ("concat", "concat(nc,'尾') = '中文测试尾'", 3),
            ("case",   "(case nc when '中文测试' then 1 else 0 end) = 1", 3),
            ("tag",    "tnc = '标签甲'", 5),
        ]
        for i, (label, where, expect) in enumerate(scenarios):
            self._create_topic("GBK", f"tp_{label}_g", where)
            g = self._consume(f"tp_{label}_g", f"gg_{i}")
            if g != expect:
                tdLog.exit(
                    f"FAIL tmq[{label}] GBK: expect={expect} got={g} "
                    f"where={where}"
                )
            # UTF-8 harness sanity on the first scenario only (server==UTF-8 so
            # a UTF-8 creator is correct by construction).
            if label == "eq":
                self._create_topic("UTF-8", f"tp_{label}_u", where)
                u = self._consume(f"tp_{label}_u", f"uu_{i}")
                if u != expect:
                    tdLog.exit(f"FAIL tmq[{label}] UTF-8: expect={expect} "
                               f"got={u}")
            tdLog.info(f"PASS tmq[{label}]: gbk={g}==expect={expect}")

    # ---- stream ----

    def _wait_streams_ready(self, n):
        # ASCII columns of ins_stream_tasks only (never ins_streams.sql).
        tdSql.checkResultsByFunc(
            sql=(
                "select count(*) from information_schema.ins_stream_tasks "
                "where type='Trigger' and status='Running'"
            ),
            func=lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == n,
        )

    def _create_calc_stream(self, charset, sname, rtbl, where):
        self._run_cli(
            charset,
            f"use ccst; create stream {sname} interval(1s) sliding(1s) "
            f"from stb into {rtbl} as "
            f"select _twstart ts, count(*) c from stb where {where}",
        )

    def _create_event_stream(self, charset, sname, rtbl):
        # Event-window trigger: the start/end conditions compare an NCHAR column
        # to NCHAR literals and run on the trigger-side path (distinct from the
        # calc filter). A fallback would mis-decode the GBK literals so the
        # window never opens -> empty output.
        self._run_cli(
            charset,
            f"use ccst; create stream {sname} "
            f"event_window(start with nc='中文测试' end with nc='结束') "
            f"from ev into {rtbl} as select ts, v from %%trows",
        )

    def _check_event(self):
        # Both event streams read the same source `ev`; if the creator charset
        # is applied they open/close identical windows. Compare full output.
        for t in ("res_ev_u", "res_ev_g"):
            tdSql.checkResultsByFunc(
                sql=f"select count(*) from ccst.{t}",
                func=lambda: tdSql.getRows() == 1
                and tdSql.getData(0, 0) is not None
                and tdSql.getData(0, 0) >= 1,
            )
        tdSql.query("select ts, v from ccst.res_ev_u order by ts, v")
        u = tdSql.queryResult
        tdSql.query("select ts, v from ccst.res_ev_g order by ts, v")
        g = tdSql.queryResult
        if g != u or len(g) < 1:
            tdLog.exit(f"FAIL stream event-window: gbk={g} utf8={u}")
        tdLog.info(f"PASS stream event-window: gbk==utf8, rows={len(g)}")

    def _check_window_count(self, tbl, expect):
        # Calc has no window-correlated predicate, so count(*) reports the whole
        # -table matching count at every window. min==max==expect (>=1 window)
        # is timing-robust and catches any c==0 fallback window.
        tdSql.checkResultsByFunc(
            sql=f"select min(c), max(c), count(*) from ccst.{tbl}",
            func=lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 0) == expect
            and tdSql.getData(0, 1) == expect
            and tdSql.getData(0, 2) is not None
            and tdSql.getData(0, 2) >= 1,
        )

    def _restart_and_check_persisted_charset(self):
        # Stream/topic plans are persisted. Restart before inserting a new GBK
        # matching row so the vnode must rebuild its charset contexts from the
        # stored metadata rather than retain the creator connection's context.
        tdLog.info(
            "restart taosd to reload persisted stream/topic charset metadata")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.execute("use ccst")
        self._wait_streams_ready(6)

        # Continue the source event-time timeline. Jumping from the fixed
        # January data to `now` can exceed the stream's recalculation range and
        # leave the new interval outside the recovered trigger's scan range.
        # The second, non-matching row advances the watermark so the matching
        # row's 1s window closes and the reloaded stream emits its result.
        tdSql.execute(
            "insert into ct1 values "
            "('2026-01-01 00:00:04.000','中文测试',7)"
        )
        tdSql.execute(
            "insert into ct1 values "
            "('2026-01-01 00:00:06.000','ASCII',8)"
        )

        # concat mixes the NCHAR column with a GBK VARCHAR literal at runtime,
        # so this assertion depends on the recovered per-node charsetCxt. Plain
        # NCHAR equality could pass with a create-time UCS-4 constant.
        tdSql.checkResultsByFunc(
            sql="select count(*) from ccst.res_concat_g where c = 4",
            func=lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 0) >= 1,
        )
        tdLog.info("PASS persisted GBK stream charset after restart")

        # A new consumer group reads the persisted GBK topic from the WAL,
        # including the row inserted after restart.
        got = self._consume("tp_eq_g", "gg_restart")
        if got != 4:
            tdLog.exit(
                f"FAIL persisted GBK topic after restart: expect=4 got={got}")

        tdLog.info("PASS persisted GBK topic charset after restart")

    def test_stream_tmq_client_charset(self):
        """Stream and subscription results are driven by the creator's charset.

        Subscription: topics filtering NCHAR via =, IN, LIKE, concat(), CASE
        WHEN, and an NCHAR tag deliver exactly the matching rows for a GBK
        creator (== the expected count computed from the data).

        Stream: a GBK-created and a UTF-8-created interval stream both count all
        matching NCHAR rows per window (identical), and an event-window stream
        whose start/end conditions compare NCHAR opens/closes identically for
        both creators.

        A server-charset fallback would mis-decode the GBK literal bytes and
        diverge on any of these.

        Since: v3.4.3.0

        Labels: common,ci,functional

        Catalog:
            - Streams:03-TriggerMode

        Jira: None
        """
        self._setup_schema()
        self._setup_cli()
        if not os.path.isfile(self.taos):
            tdLog.exit(f"taos CLI not found at {self.taos}")

        # Calc/event streams created before data so they process the inserts
        # (streams do not replay historical data by default). Topics are
        # created later, in _check_topics() after the insert below; that's
        # fine since consumers use auto.offset.reset=earliest and read the
        # rows back out of the WAL regardless of topic-creation order.
        self._create_calc_stream("UTF-8", "s_eq_u", "res_eq_u", "nc='中文测试'")
        self._create_calc_stream("GBK", "s_eq_g", "res_eq_g", "nc='中文测试'")
        self._create_calc_stream(
            "GBK", "s_in_g", "res_in_g", "nc in ('中文测试','你好世界')"
        )
        self._create_calc_stream(
            "GBK",
            "s_concat_g",
            "res_concat_g",
            "concat(nc,'尾')='中文测试尾'",
        )
        self._create_event_stream("UTF-8", "s_ev_u", "res_ev_u")
        self._create_event_stream("GBK", "s_ev_g", "res_ev_g")
        self._wait_streams_ready(6)
        self._insert_data()

        # Subscription scenarios (all delivered from the WAL).
        self._check_topics()

        # Stream calc scenarios: every window's matching count is correct.
        self._check_window_count("res_eq_u", 3)
        self._check_window_count("res_eq_g", 3)
        self._check_window_count("res_in_g", 4)
        self._check_window_count("res_concat_g", 3)
        tdLog.info(
            "PASS stream calc: gbk==utf8, eq=3 in=4 concat=3 per window"
        )

        # Stream trigger-side scenario: event-window NCHAR conditions.
        self._check_event()

        # Reload persisted plans, then verify a new GBK-matching row is still
        # recognized by both the persisted stream and topic paths.
        self._restart_and_check_persisted_charset()

        # Cleanup via execute() (no result fetch) so GBK stored SQL is never
        # decoded by taospy. Drop topics before the database (a db with live
        # topics cannot be dropped).
        for s in (
            "s_eq_u",
            "s_eq_g",
            "s_in_g",
            "s_concat_g",
            "s_ev_u",
            "s_ev_g",
        ):
            tdSql.execute(f"drop stream if exists ccst.{s}")
        self._drop_topics()
        tdSql.execute("drop database if exists ccst")
        tdLog.success(f"{__file__} passed")
