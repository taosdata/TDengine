
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

import time
import threading
import datetime
import subprocess
import tempfile

from new_test_framework.utils import (
    tdLog,
    tdSql,
    cluster,
    etool,
)

class Test_snode_restart_with_checkpoint:
    # updatecfgDict = {'checkpointInterval': 5}
    # print("===================: ", updatecfgDict)

    def setup_class(cls):
        tdLog.info(f"start to excute {__file__}")
        # tdSql.init(conn.cursor(), True)


    def test_case1(self):
        """OldPy: snode

        1. -

        Catalog:
            - Streams:OldPyCases

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-7-21 lvze Migrated from community/tests/system-test/8-stream/snode_restart_with_checkpoint.py -N 4

        """
        tdLog.info("========case1 start========")

        with tempfile.TemporaryFile(mode="w+b") as benchmark_log:
            benchmark = subprocess.Popen(
                [etool.benchMarkFile(), "-y", "-B", "1", "-t", "4", "-S", "1000",
                 "-n", "1000", "-i", "1000", "-v", "2"],
                stdout=benchmark_log, stderr=subprocess.STDOUT,
            )
            try:
                time.sleep(4)
                tdSql.query("use test", queryTimes=60)
                tdSql.query("create snode on dnode 4")
                tdSql.query(f"""
                            create stream test.s1 interval(2s) sliding(2s) from test.meters
                            partition by groupid
                            stream_options(fill_history| low_latency_calc)
                            into test.st1
                            tags(
                            groupid int as groupid)
                            as select
                                _twstart  ts,
                                sum(voltage) voltage
                            from
                                %%trows t1 ;
                            """)
                tdLog.info("========create stream using snode and insert data ok========")
                # time.sleep(60)
                self.checkStreamRunning()


                tdDnodes = cluster.dnodes
                tdDnodes[3].stoptaosd()
                time.sleep(2)
                if benchmark.poll() is None:
                    benchmark.kill()
                benchmark.wait(timeout=30)
                tdLog.info("========stop insert ok========")
                tdDnodes[3].starttaosd()
                tdLog.info("========snode restart ok========")

                self.checkStreamRunning()

                # A stopped writer leaves an open tail window per group. Check
                # every window whose end is covered by that group's input.
                tdSql.query("select groupid,max(ts) from meters partition by groupid")
                group_end = {row[0]: row[1] for row in tdSql.queryResult}
                tdSql.query(
                    "select _wstart,_wend,sum(voltage),groupid from meters "
                    "partition by groupid interval(2s) order by groupid,_wstart"
                )
                expected = [
                    (row[0], row[2], row[3]) for row in tdSql.queryResult
                    if row[1] <= group_end[row[3]]
                ]
                assert expected, "No closed source windows to verify after snode restart"
                deadline = time.monotonic() + 300
                while True:
                    tdSql.query("select ts,voltage,groupid from st1 order by groupid,ts")
                    actual = [
                        tuple(row) for row in tdSql.queryResult
                        if row[0] + datetime.timedelta(seconds=2) <= group_end[row[2]]
                    ]
                    if actual == expected or time.monotonic() >= deadline:
                        break
                    time.sleep(1)
                assert actual == expected, (
                    f"Closed stream windows differ after snode restart: "
                    f"expected={expected!r}, actual={actual!r}"
                )
                tdLog.info(f"Verified {len(expected)} closed windows after snode restart")
                tdLog.info("case1 end")
            finally:
                if benchmark.poll() is None:
                    benchmark.kill()
                benchmark.wait(timeout=30)


    def checkStreamRunning(self):
        tdLog.info(f"check stream running status:")

        timeout = 60
        start_time = time.time()

        while True:
            if time.time() - start_time > timeout:
                tdLog.error("Timeout waiting for all streams to be running.")
                tdLog.error(f"Final stream running status: {streamRunning}")
                raise TimeoutError(f"Stream status did not reach 'Running' within {timeout}s timeout.")

            tdSql.query(f"select status from information_schema.ins_streams order by stream_name;")
            streamRunning=tdSql.getColData(0)

            if all(status == "Running" for status in streamRunning):
                tdLog.info("All Stream running!")
                tdLog.info(f"stream running status: {streamRunning}")
                return
            else:
                tdLog.info("Stream not running! Wait stream running ...")
                tdLog.info(f"stream running status: {streamRunning}")
                time.sleep(1)

event = threading.Event()

