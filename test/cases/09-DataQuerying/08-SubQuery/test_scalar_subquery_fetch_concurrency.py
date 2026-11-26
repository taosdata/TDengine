# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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
#
# Regression for defect 7169223624.
#
# The scalar/quantified subquery fetch used to fail sporadically with
# "rpc state already dropped" (0x8000002A) instead of the expected multi-row
# error (0x800026A5) when several fetch requests of the same sub-job were in
# flight, because the request handles shared one slot in the sub-job context.
# Every query below must always report the multi-row error; any other code is
# a regression.
#
# SUBQ_FETCH_STRESS_ITERS overrides the number of iterations (default 300).
# Under sustained CPU pressure the pre-fix build reproduces the failure.

import os
import time

from new_test_framework.utils import tdLog, tdSql

DB = "db1"

SQLS = [
    (
        "quantified_all_interval",
        "select (select f1 = all (select f1 from st1) a from st1) from st1 partition by f1 interval(1d);",
    ),
    (
        "quantified_all_plain",
        "select (select f1 = all (select f1 from st1) a from st1) from st1;",
    ),
    (
        "scalar_unionall",
        "select (select f1 from st1 where f1 in ((select f1 from st1 union all select f1 from st1)) order by f1) + 1, "
        "avg((select f1 from st1 where f1 in ((select f1 from st1 union all select f1 from st1)) order by f1) + 2) "
        "from st1 a left join st1 b on a.ts = b.ts and a.f1 = "
        "(select f1 from st1 where f1 in ((select f1 from st1 union all select f1 from st1)) order by f1);",
    ),
]

ERR_STATE_DROPPED = -2147483606  # 0x8000002A
ERR_MULTIROW = -2147473755      # 0x800026A5


class TestScalarSubqueryFetchConcurrency:
    updatecfgDict = {
        "debugFlag": 131,
        "asyncLog": 1,
        "qDebugFlag": 131,
        "cDebugFlag": 131,
        "rpcDebugFlag": 131,
    }
    updatecfgDict["clientCfg"] = {
        "debugFlag": 131,
        "asyncLog": 1,
        "qDebugFlag": 131,
        "cDebugFlag": 131,
        "rpcDebugFlag": 131,
    }

    def test_scalar_subquery_fetch_state(self):
        """Scalar subquery fetch handles survive concurrent requests

        1. Repeatedly execute scalar and quantified subqueries whose fetch requests may overlap.
        2. Every execution must report the multi-row subquery error, never rpc state dropped.

        Catalog:
            - Query:SubQuery

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-25 wpan Add regression for defect 7169223624.

        """
        tdLog.info("scalar subquery fetch handle concurrency, defect 7169223624")
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB}")
        tdSql.execute(f"use {DB}")
        tdSql.executes(
            [
                "CREATE STABLE `st1` (`ts` TIMESTAMP, `f1` int, f2 int) TAGS (`tg1` int)",
                "CREATE TABLE tb1 USING st1 TAGS (1)",
                "CREATE TABLE tb2 USING st1 TAGS (2)",
                "CREATE TABLE tb3 USING st1 TAGS (3)",
                "INSERT INTO tb1 VALUES ('2025-12-01 00:00:00.000', 1, 11)",
                "INSERT INTO tb2 VALUES ('2025-12-01 00:00:00.000', 2, 21)",
                "INSERT INTO tb2 VALUES ('2025-12-02 00:00:00.000', 3, 23)",
                "INSERT INTO tb3 VALUES ('2025-12-01 00:00:00.000', 4, 31)",
                "INSERT INTO tb3 VALUES ('2025-12-02 00:00:00.000', 5, 32)",
                "INSERT INTO tb3 VALUES ('2025-12-03 00:00:00.000', 6, 33)",
            ]
        )

        iters = int(os.environ.get("SUBQ_FETCH_STRESS_ITERS", "300"))
        cur = tdSql.cursor
        stateDropped = 0
        unexpected = {}
        stAll = time.time()
        for it in range(iters):
            for name, sql in SQLS:
                st = time.time()
                try:
                    cur.execute(sql)
                    cur.fetchall()
                    code = 0
                except Exception as e:  # noqa: BLE001 - the error code is what matters here
                    code = getattr(e, "errno", None)
                    if code is None:
                        code = -99999
                if code == ERR_STATE_DROPPED:
                    stateDropped += 1
                    tdLog.info(f"STATE-DROPPED iter={it} case={name}")
                elif code != ERR_MULTIROW:
                    unexpected[(name, code)] = unexpected.get((name, code), 0) + 1
                    tdLog.info(f"unexpected error iter={it} case={name} code={code} ({time.time() - st:.3f}s)")
            if (it + 1) % 100 == 0:
                tdLog.info(
                    f"progress iter={it + 1}/{iters} elapsed={time.time() - stAll:.1f}s "
                    f"stateDropped={stateDropped} unexpected={unexpected}"
                )

        tdLog.info(
            f"done iters={iters} elapsed={time.time() - stAll:.1f}s "
            f"stateDropped={stateDropped} unexpected={unexpected}"
        )
        assert stateDropped == 0, f"fetches failed with rpc state already dropped: {stateDropped} hit(s)"
        assert not unexpected, f"queries returned unexpected error codes: {unexpected}"
