###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
import time

from new_test_framework.utils import tdLog, tdSql


class TestFunGreatestLeastPerf:
    """Performance regression for GREATEST under default and
    ``ignoreNullInGreatest=1`` configurations.

    Maps to GTL-PERF-001 in ``Func-GreatestLeast-TS.md`` and covers
    RS §5 + RS §7.5 item 8 ("默认配置下无性能退化").

    Strategy
    --------
    The check is intentionally CI-friendly: we don't have a stable
    historical baseline to compare against, so we use two soft bounds
    that catch order-of-magnitude regressions without flaking on noisy
    runners:

    1. Absolute upper bound on the default-config run (30s for 100K
       rows, 4 INT columns).  ASan-mode taosd is several times slower
       than release, so this is sized accordingly; a healthy build
       finishes in well under a second outside ASan.
    2. Relative bound: ``ignoreNullInGreatest=1`` may add a per-row
       branch but must not be more than ``RATIO_LIMIT`` × the default
       run.  The default code path itself must produce identical timing
       characteristics to the pre-feature implementation (only one
       extra constant TINYINT param is appended at translate time).
    """

    db = "gtl_perf"
    nrows = 100_000
    rows_per_insert = 5000
    abs_limit_sec = 30.0
    ratio_limit = 3.0

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute(f"drop database if exists {cls.db}")
        tdSql.execute(f"create database {cls.db} vgroups 2")
        tdSql.execute(f"use {cls.db}")
        tdSql.execute(
            "create table tperf (ts timestamp, c1 int, c2 int, "
            "c3 int, c4 int)"
        )
        # Bulk-load deterministic rows.  Values are chosen so each
        # column "wins" the GREATEST on roughly 1/4 of rows, exercising
        # the per-row argmax path uniformly.
        base_ts = 1_700_000_000_000
        batch = []
        for i in range(cls.nrows):
            v1 = (i * 7) & 0xFFFF
            v2 = (i * 11) & 0xFFFF
            v3 = (i * 13) & 0xFFFF
            v4 = (i * 17) & 0xFFFF
            batch.append(f"({base_ts + i}, {v1}, {v2}, {v3}, {v4})")
            if len(batch) >= cls.rows_per_insert:
                tdSql.execute(
                    "insert into tperf values " + ",".join(batch)
                )
                batch.clear()
        if batch:
            tdSql.execute("insert into tperf values " + ",".join(batch))
        # Warm caches with one full scan so subsequent timings reflect
        # query-engine cost, not first-touch I/O.
        tdSql.query(f"select count(*) from tperf")
        tdSql.checkData(0, 0, cls.nrows)

    def teardown_class(cls):
        try:
            tdSql.execute("alter local 'ignoreNullInGreatest' '0'")
        except Exception:
            pass
        tdSql.execute(f"drop database if exists {cls.db}")

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _set_ignore_null(self, value):
        tdSql.execute(f"alter local 'ignoreNullInGreatest' '{value}'")

    def _time_query(self, sql, repeats=3):
        """Run ``sql`` ``repeats`` times and return the best wall-clock
        elapsed in seconds.  Best-of-N suppresses outliers from
        co-tenant noise that would otherwise mask a real regression."""
        best = None
        for _ in range(repeats):
            t0 = time.perf_counter()
            tdSql.query(sql)
            elapsed = time.perf_counter() - t0
            assert tdSql.queryRows == self.nrows, (
                f"unexpected row count {tdSql.queryRows} "
                f"(expected {self.nrows})"
            )
            best = elapsed if best is None else min(best, elapsed)
        return best

    # ------------------------------------------------------------------
    # GTL-PERF-001 default-config has no observable regression and
    # ignoreNullInGreatest=1 stays within a sane ratio of default.
    # ------------------------------------------------------------------
    def case_perf_001(self):
        sql = f"select greatest(c1, c2, c3, c4) from {self.db}.tperf"

        self._set_ignore_null(0)
        t_default = self._time_query(sql)
        tdLog.info(
            f"[GTL-PERF-001] default ignoreNullInGreatest=0 "
            f"best-of-3 elapsed = {t_default:.3f}s "
            f"({self.nrows} rows)"
        )
        assert t_default < self.abs_limit_sec, (
            f"default-config GREATEST took {t_default:.3f}s, "
            f"exceeds {self.abs_limit_sec}s upper bound — possible "
            "regression vs pre-feature implementation"
        )

        self._set_ignore_null(1)
        t_ignore = self._time_query(sql)
        tdLog.info(
            f"[GTL-PERF-001] ignoreNullInGreatest=1 "
            f"best-of-3 elapsed = {t_ignore:.3f}s"
        )
        # Use max(t_default, small floor) so the ratio bound stays
        # meaningful when t_default itself is sub-millisecond.
        ratio = t_ignore / max(t_default, 0.01)
        assert ratio <= self.ratio_limit, (
            f"ignoreNullInGreatest=1 elapsed {t_ignore:.3f}s is "
            f"{ratio:.2f}x of default {t_default:.3f}s, "
            f"exceeds {self.ratio_limit}x tolerance"
        )

    # ------------------------------------------------------------------
    # main
    # ------------------------------------------------------------------
    def test_fun_sca_greatest_least_perf(self):
        """Fun: greatest()/least() perf regression

        1. Default ignoreNullInGreatest=0 finishes within absolute bound
        2. ignoreNullInGreatest=1 stays within configured ratio of default

        Since: v3.4.2.0

        Labels: common,ci,perf

        Jira: None
        """
        try:
            self.case_perf_001()
        finally:
            self._set_ignore_null(0)
