# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck
import time


class TestTime:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def do_now_returns_unix_time(self):
        for precision, divisor in (("ms", 1000000), ("us", 1000), ("ns", 1)):
            db = f"scalar_now_clock_{precision}"
            tdSql.execute(f"drop database if exists {db}")
            try:
                tdSql.execute(
                    f"create database {db} precision '{precision}'"
                )
                tdSql.execute(f"use {db}")
                tdSql.execute("create table t (ts timestamp, v int)")
                tdSql.execute(
                    "insert into t values ('2024-01-01 00:00:00', 1)"
                )
                for suffix in ("", " from t"):
                    before = time.time_ns()
                    tdSql.query(
                        "select cast(now as bigint), "
                        "cast(now() as bigint)" + suffix
                    )
                    after = time.time_ns()
                    tdSql.checkRows(1)
                    actual = tdSql.getData(0, 0)
                    tdSql.checkData(0, 1, actual)
                    # Queries without FROM use the default ms precision.
                    scale = divisor if suffix else 1000000
                    # Python and native Windows clocks differ in resolution.
                    margin = 1000000000
                    assert (before - margin) // scale <= actual <= (
                        after + margin
                    ) // scale, (precision, suffix, before, actual, after)
            finally:
                tdSql.execute(f"drop database if exists {db}")
        print("Current Unix time in now/now() ............. [ passed ]")

    def test_time(self):
        """Scalar: Time

        Test time functions, including TIMETRUNCATE, TIMEDIFF, and their combined usage.

        Catalog:
            - Function:Sclar

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/scalar/tsConvert.sim

        """

        self.do_now_returns_unix_time()
        tdLog.info(f"======== step1")
        tdSql.prepare("db1", drop=True)
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable st1 (ts timestamp, f1 int, f2 binary(30)) tags(t1 int, t2 binary(30));"
        )
        tdSql.execute(f"create table tb1 using st1 tags(1, '1');")
        tdSql.execute(f"insert into tb1 values ('2022-07-10 16:31:00', 1, '1');")
        tdSql.execute(f"insert into tb1 values ('2022-07-10 16:32:00', 2, '2');")
        tdSql.execute(f"insert into tb1 values ('2022-07-10 16:33:00', 3, '3');")
        tdSql.execute(f"insert into tb1 values ('2022-07-10 16:34:00', 4, '4');")
        tdSql.query(
            f"select * from (select ts,TIMETRUNCATE(ts,1d),TIMETRUNCATE(ts,1h), abs(timediff(TIMETRUNCATE(ts,1d),TIMETRUNCATE(ts,1h),1h)) as td from tb1 where ts >='2022-06-01 00:00:00' and ts <='2022-11-3 23:59:59' ) t where td >12;"
        )

        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2022-07-10 16:31:00.000")
        tdSql.checkData(1, 0, "2022-07-10 16:32:00.000")
        tdSql.checkData(2, 0, "2022-07-10 16:33:00.000")
        tdSql.checkData(3, 0, "2022-07-10 16:34:00.000")

        tdSql.query(f"select * from tb1 where ts > '2022-07-10 16:32:00';")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2022-07-10 16:33:00.000")
        tdSql.checkData(1, 0, "2022-07-10 16:34:00.000")

        tdSql.query(f"select * from tb1 where ts + 1 > '2022-07-10 16:32:00';")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2022-07-10 16:32:00.000")
        tdSql.checkData(1, 0, "2022-07-10 16:33:00.000")
        tdSql.checkData(2, 0, "2022-07-10 16:34:00.000")

        tdSql.query(
            f"select * from tb1 where '2022-07-10 16:32:00' > timestamp '2022-07-10 16:31:59';"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2022-07-10 16:31:00.000")
        tdSql.checkData(1, 0, "2022-07-10 16:32:00.000")
        tdSql.checkData(2, 0, "2022-07-10 16:33:00.000")
        tdSql.checkData(3, 0, "2022-07-10 16:34:00.000")

        tdSql.query(
            f"select case f1 when '1' then 1 when '2022-07-10 16:32:00' then '2' end from tb1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(
            f"select case ts when '2022-07-10 16:31:00' then 1 when '2022-07-10 16:32:00' then '2' end from tb1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(
            f"select case '2022-07-10 16:31:00' when ts then 1 when 2022 then '2' else 3 end from tb1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)
