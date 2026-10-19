from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamCalcRangeExtraTsCond:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---

    def prepare(self, dbname):
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname=dbname, vgroups=1)
        tdSql.execute(
            f"create stable {dbname}.meters (ts timestamp, v1 int) tags (code varchar(16))"
        )
        tdSql.execute(f"create table {dbname}.d1 using {dbname}.meters tags ('c1')")

    def writeRows(self, dbname):
        # one statement per row, so every row forms its own count_window(1) batch.
        # A count window is a point window: _twstart == _twend.
        tdSql.execute(f"insert into {dbname}.d1 values (now, 1)")
        tdSql.execute(f"insert into {dbname}.d1 values (now + 1s, 2)")
        tdSql.execute(f"insert into {dbname}.d1 values (now + 2s, 3)")

    def checkThreeRows(self, outTable):
        tdSql.checkResultsByFunc(
            f"select ts, v1 from {outTable} order by ts",
            lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(1, 1, 2)
            and tdSql.compareData(2, 1, 3),
            retry=30,
        )

    # --- impl ---

    def do_extra_loose_lower_bound_literal(self):
        tdLog.info("=== case1: extra lower bound is a loose literal ===")
        self.prepare("range_lit")

        # "ts > '1970-01-02 ...'" is looser than _twstart, so the effective lower
        # bound must stay _twstart.  Before the fix the operator of this strict
        # condition was taken as the range operator (last evaluated wins) while the
        # value stayed _twstart, so skey became _twstart + 1 and the point window
        # degenerated into the empty range [skey > ekey] => no stream output at all.
        tdSql.execute(
            "create stream range_lit.s1 count_window(1) "
            "from range_lit.meters partition by tbname "
            "into range_lit.out1 output_subtable(concat('o_', %%1)) "
            "as select _twstart as ts, last(v1) as v1 "
            "from %%tbname "
            "where ts >= _twstart and ts <= _twend and ts > '1970-01-02 00:00:00.000'"
        )
        tdStream.checkStreamStatus("s1")

        self.writeRows("range_lit")
        self.checkThreeRows("range_lit.out1")

        tdLog.info("extra loose lower bound (literal) ......................... [ passed ]")

    def do_extra_loose_lower_bound_now(self):
        tdLog.info("=== case2: extra lower bound is now - 30d ===")
        self.prepare("range_now")

        # Same shape as the reported stream:
        #   where ts >= _twstart and ts <= _twend and ts > now - 30d
        tdSql.execute(
            "create stream range_now.s2 count_window(1) "
            "from range_now.meters partition by tbname "
            "into range_now.out2 output_subtable(concat('o_', %%1)) "
            "as select _twstart as ts, last(v1) as v1 "
            "from %%tbname "
            "where ts >= _twstart and ts <= _twend and ts > now - 30d"
        )
        tdStream.checkStreamStatus("s2")

        self.writeRows("range_now")
        self.checkThreeRows("range_now.out2")

        tdLog.info("extra loose lower bound (now - 30d) ......................... [ passed ]")

    def do_extra_loose_upper_bound(self):
        tdLog.info("=== case3: extra upper bound is a loose expression ===")
        self.prepare("range_up")

        # Mirror case: the extra condition is on the upper side and is looser than
        # _twend, so ekey must stay _twend.  Before the fix the '<' condition was
        # taken as the range operator, which made ekey = _twend - 1 and emptied the
        # point window as well.
        tdSql.execute(
            "create stream range_up.s3 count_window(1) "
            "from range_up.meters partition by tbname "
            "into range_up.out3 output_subtable(concat('o_', %%1)) "
            "as select _twstart as ts, last(v1) as v1 "
            "from %%tbname "
            "where ts >= _twstart and ts <= _twend and ts < now + 1d"
        )
        tdStream.checkStreamStatus("s3")

        self.writeRows("range_up")
        self.checkThreeRows("range_up.out3")

        tdLog.info("extra loose upper bound (now + 1d) ......................... [ passed ]")

    def do_cleanup(self):
        tdStream.dropAllStreamsAndDbs()
        tdLog.info("cleanup ......................... [ passed ]")

    # --- main ---

    def test_stream_calc_range_extra_ts_cond(self):
        """Stream calc scan range must follow the binding ts condition

        1. count_window(1) stream whose calc query carries an extra lower ts bound
           that is looser than _twstart (`ts > '1970-01-02 00:00:00.000'`); the stream
           must still emit one row per window.
        2. Same with the reported form `ts > now - 30d`.
        3. Mirror case with an extra upper ts bound looser than _twend
           (`ts < now + 1d`).

        Catalog:
            - Streams:Window

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Jira: None

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7132071991

        History:
            - 2026-10-10 GitHub Copilot Create Case

        """
        self.do_extra_loose_lower_bound_literal()
        self.do_extra_loose_lower_bound_now()
        self.do_extra_loose_upper_bound()
        self.do_cleanup()
