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

import time

from taos.tmq import Consumer

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestTmqEmptySubmit:
    """Regression: TMQ must skip submits that carry no rows instead of failing the poll."""

    updatecfgDict = {"debugFlag": 135, "asynclog": 0}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ---- utils -------------------------------------------------
    def prepare(self):
        self.src_db = "tmq_empty_src"
        self.out_db = "tmq_empty_out"
        self.stream_name = "s_empty"
        self.topic = "tmq_empty_topic"
        self.group = "tmq_empty_group"

        tdSql.execute(f"drop topic if exists {self.topic}")
        tdSql.execute(f"drop database if exists {self.out_db}")
        tdSql.execute(f"drop database if exists {self.src_db}")

        tdStream.ensureSnode()
        tdSql.execute(f"create database {self.src_db} vgroups 1")
        tdSql.execute(f"create database {self.out_db} vgroups 1")
        tdSql.execute(f"create table {self.src_db}.src_t (ts timestamp, v int)")

        # HAVING filters every group, so each triggered window makes the stream write a submit
        # that carries no rows (SUBMIT_REQ_ONLY_CREATE_TABLE). Such a submit used to fail the
        # consumer poll with TSDB_CODE_OUT_OF_RANGE.
        tdSql.execute(f"use {self.out_db}")
        tdSql.execute(
            f"create stream {self.stream_name} interval(1s) sliding(1s) from {self.src_db}.src_t "
            f"into out_t as select _twstart as ts, count(*) as cnt from {self.src_db}.src_t "
            f"having count(*) > 100000"
        )

    def wait_zero_row_submit(self, timeout=90):
        """The stream creates out_t from the first zero-row submit; wait until it exists and is empty."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                tdSql.query(f"select count(*) from {self.out_db}.out_t")
                if tdSql.getData(0, 0) == 0:
                    time.sleep(2)  # let a couple more zero-row submits land in WAL
                    return
            except Exception:
                pass
            time.sleep(1)
        tdLog.exit("stream did not create the output table from a zero-row submit in time")

    def consume_until_row(self, timeout=60):
        """Poll the topic and return (rows consumed, poll errors seen)."""
        consumer = Consumer(
            {
                "group.id": self.group,
                "client.id": "tmq_empty_client",
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
            }
        )
        consumer.subscribe([self.topic])

        rows = 0
        errors = []
        deadline = time.time() + timeout
        try:
            while time.time() < deadline:
                res = consumer.poll(1)
                if res is None:
                    continue
                err = res.error()
                if err is not None:
                    errors.append(str(err))
                    break
                for block in res.value() or []:
                    for row in block.fetchall():
                        rows += 1
                        if row[1] != 42:
                            tdLog.exit(f"unexpected row consumed: {row}")
                consumer.commit(res)
                if rows > 0:
                    break
        finally:
            try:
                consumer.unsubscribe()
            except Exception:
                pass
            consumer.close()
        return rows, errors

    # ---- impl --------------------------------------------------
    def do_skip_zero_row_submit(self):
        tdLog.info("insert rows whose windows are all filtered by HAVING")
        tdStream.checkStreamStatus(self.stream_name)
        # Stream windows are triggered by the watermark, so the data has to be close to the current
        # time; this is the real-time behavior the case exercises, not a fixed-data assertion.
        base_ts = int(time.time() * 1000) - 5000
        values = " ".join(f"({base_ts + i * 1000}, {i})" for i in range(6))
        tdSql.execute(f"insert into {self.src_db}.src_t values {values}")
        self.wait_zero_row_submit()

        # The row below is written after the zero-row submits, so the consumer has to skip them
        # before it can deliver it.
        tdSql.execute(f"insert into {self.out_db}.out_t values ({base_ts + 60000}, 42)")
        tdSql.execute(f"create topic {self.topic} as select ts, cnt from {self.out_db}.out_t")

        rows, errors = self.consume_until_row()
        if errors:
            tdLog.exit(f"tmq poll failed while consuming zero-row submits: {errors}")
        if rows == 0:
            tdLog.exit("no row was delivered after the zero-row submits")

        tdSql.query(f"select count(*) from {self.out_db}.out_t")
        tdSql.checkData(0, 0, 1)

        print("tmq skip zero-row submit ................. [ passed ]")

    # ---- main --------------------------------------------------
    def test_tmq_empty_submit(self):
        """summary: tmq consumer must skip submits that carry no rows.

        A stream whose HAVING filters every group writes submits without rows
        (SUBMIT_REQ_ONLY_CREATE_TABLE). The consumer used to fail such a poll with
        TSDB_CODE_OUT_OF_RANGE and then kept failing every following poll.

        Since: 3.4.3.0

        Labels: common,ci

        Catalog:
        - DataSubscription

        Jira: 7169036562

        History:
        - 2026-11-16 mmwang Added regression for zero-row submit handling
        """
        self.prepare()
        self.do_skip_zero_row_submit()
