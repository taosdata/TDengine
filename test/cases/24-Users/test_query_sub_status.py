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

import re
import threading
import time

import taos

from new_test_framework.utils import tdLog, tdSql


class TestQuerySubStatus:
    """Regression tests for show queries.sub_status.

    sub_status lists one entry per sub task of a query. It is a VARCHAR cell
    bounded only by the type maximum (TSDB_MAX_FIELD_LEN bytes including the
    varstr header), and every task must be listed when the encoded text fits:
    the number of entries has to match sub_num, and each entry carries
    "<tid>:<type/state>:<offset>" where offset is the task start time relative
    to the query create_time, e.g. "+58ms", or "-" when it never started.
    """

    VALID_TASK_TYPES = ["scan", "merge", "modify", "compute", "partial", "task"]
    OFFSET_RE = re.compile(r"^(\+\d+ms|-)$")

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _get_index(self, col_names, name):
        return col_names.index(name) if name in col_names else -1

    @staticmethod
    def _parse_sub_status(sub_status):
        entries = str(sub_status).split(",")
        parsed = []
        for entry in entries:
            fields = entry.split(":", 2)
            parsed.append(fields)
        return parsed

    def _collect_sub_status(self, query, match, duration):
        """Run query on a second connection and poll show queries for its row.

        Returns the sub_num / sub_status samples observed while the query was
        still running.
        """
        consumer_conn = taos.connect(host=self.host, port=self.port, config=self.cfg_path)
        errors = []

        def consume():
            try:
                cursor = consumer_conn.cursor()
                cursor.execute(query)
                cursor.fetchall()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        worker = threading.Thread(target=consume, daemon=True)
        worker.start()
        sampled = []
        try:
            deadline = time.time() + duration
            while time.time() < deadline and worker.is_alive():
                tdSql.query("show queries")
                col_names = [desc[0] for desc in tdSql.cursor.description]
                sql_idx = self._get_index(col_names, "sql")
                sub_num_idx = self._get_index(col_names, "sub_num")
                sub_status_idx = self._get_index(col_names, "sub_status")
                if sql_idx < 0 or sub_num_idx < 0 or sub_status_idx < 0:
                    break
                for row in range(tdSql.getRows()):
                    if match in str(tdSql.getData(row, sql_idx)):
                        sub_status = tdSql.getData(row, sub_status_idx)
                        if sub_status:
                            sampled.append((tdSql.getData(row, sub_num_idx), str(sub_status)))
                time.sleep(0.2)
        finally:
            worker.join(timeout=60)
            consumer_conn.close()

        if errors:
            raise errors[0]
        return sampled

    def test_sub_status_lists_every_task(self):
        """SubStatus: every sub task is listed once the text fits the column

        Create a database with 86 vgroups so a single supertable query needs at
        least 86 scan tasks plus the merge task, i.e. well over the old 1000
        byte cap. While the merge is held busy, sub_status must contain one
        complete entry per sub task, the entry count must equal sub_num, and
        the encoded text must be longer than 1000 bytes (the removed limit) but
        still inside the VARCHAR cell.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-16 Yihao Deng fix defect 7159092216

        """
        db = "db_sub_status_many"
        vgroups = 86
        tdLog.info("=============== test sub status lists every task")

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups {vgroups} buffer 8")
        tdSql.execute(f"create stable {db}.stb (ts timestamp, v int) tags (g int)")

        for batch_start in range(0, vgroups, 10):
            clauses = []
            for i in range(batch_start, min(batch_start + 10, vgroups)):
                prefix = "create table" if i == batch_start else ""
                clauses.append(f"{prefix} {db}.ct{i} using {db}.stb tags ({i})")
            tdSql.execute(" ".join(clauses))

        for batch_start in range(0, vgroups, 10):
            rows = []
            for i in range(batch_start, min(batch_start + 10, vgroups)):
                prefix = "insert into" if i == batch_start else ""
                rows.append(f"{prefix} {db}.ct{i} values (now, {i})")
            tdSql.execute(" ".join(rows))

        # Aggregation over every vgroup: scan tasks in all vgroups plus the
        # merge task. sleep(6) keeps the merge alive for several heartbeats.
        query = f"select count(*), sleep(6) from {db}.stb"
        samples = self._collect_sub_status(query, "sleep(6)", 8)
        tdLog.info(f"collected {len(samples)} sub_status samples")

        assert samples, "the distributed query was never observed in show queries"
        sub_num, sub_status = samples[0]

        assert int(sub_num) >= vgroups, f"expected at least {vgroups} sub tasks, got sub_num={sub_num}"
        assert len(sub_status.encode("utf-8")) > 1000, (
            f"sub_status must exceed the removed 1000 byte cap for {sub_num} sub tasks, "
            f"got {len(sub_status.encode('utf-8'))} bytes"
        )

        entries = self._parse_sub_status(sub_status)
        tdLog.info(
            f"sub_num={sub_num}, entries={len(entries)}, sub_status bytes={len(sub_status.encode('utf-8'))}"
        )
        assert len(entries) == int(sub_num), (
            f"sub_status lists {len(entries)} tasks but sub_num is {sub_num}; "
            f"tasks must not be truncated when the text fits the VARCHAR column"
        )

        tids = set()
        types = set()
        offsets = []
        for fields in entries:
            assert len(fields) == 3, f"entry must be tid:type/state:offset, got: {fields}"
            tid_str, status, offset = fields
            assert tid_str.isdigit(), f"tid must be numeric, got: {tid_str}"
            assert "/" in status, f"status must be type/state, got: {status}"
            task_type = status.split("/", 1)[0]
            assert task_type in self.VALID_TASK_TYPES, (
                f"task type must be one of {self.VALID_TASK_TYPES}, got: {task_type}"
            )
            assert self.OFFSET_RE.match(offset), (
                f"offset must be relative to create_time with a unit ('+58ms') or '-', got: {offset}"
            )
            tids.add(tid_str)
            types.add(task_type)
            offsets.append(offset)

        assert len(tids) == int(sub_num), f"task ids must be unique, got {len(tids)} ids for {sub_num} tasks"
        assert "scan" in types and "merge" in types, (
            f"expected both scan and merge tasks in sub_status, got types: {sorted(types)}"
        )
        assert any(offset.startswith("+") for offset in offsets), (
            f"started tasks must show a relative offset like '+58ms', got: {offsets}"
        )

        tdSql.execute(f"drop database if exists {db}")
        print("test sub status lists every task ....................... [passed]")

    def cleanup_class(cls):
        tdLog.info(f"cleanup {__file__}")
        tdSql.execute("drop database if exists db_sub_status_many")
