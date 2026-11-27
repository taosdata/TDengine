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

import ctypes
import json
import threading
import time

from taos.cinterface import _libtaos
from taos.tmq import Consumer

from new_test_framework.utils import tdLog, tdSql


class TestTmqRebalanceSnapshotMeta:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def json_meta_text(self, result):
        """Return tmq_get_json_meta() text for a poll result, None for data messages."""
        _libtaos.tmq_get_json_meta.argtypes = [ctypes.c_void_p]
        _libtaos.tmq_get_json_meta.restype = ctypes.c_void_p
        _libtaos.tmq_free_json_meta.argtypes = [ctypes.c_void_p]
        _libtaos.tmq_free_json_meta.restype = None
        ptr = _libtaos.tmq_get_json_meta(result.msg)
        if not ptr:
            return None
        try:
            return ctypes.cast(ptr, ctypes.c_char_p).value.decode("utf-8", errors="replace")
        finally:
            _libtaos.tmq_free_json_meta(ptr)

    def collect_create_meta(self, consumer, collected, lock, errors, stop_event):
        """Poll one consumer and accumulate create-table metadata names."""
        try:
            while not stop_event.is_set():
                result = consumer.poll(1)
                if result is None:
                    continue
                err = result.error()
                if err is not None:
                    # Transient errors (Consumer mismatch / rebalance) are expected;
                    # keep polling and let the final table-count assertion decide.
                    errors.append(str(err))
                    continue
                text = self.json_meta_text(result)
                if text is None:
                    continue
                meta = json.loads(text)
                # Snapshot/WAL batch meta JSON: {"tmq_meta_version":"1.0","metas":[{"type":"create","tableName":"..."}, ...]}
                with lock:
                    for item in meta.get("metas") or []:
                        name = item.get("tableName")
                        if name:
                            collected.add(name)
        except Exception as e:  # pragma: no cover - defensive
            errors.append(repr(e))

    def do_rebalance_snapshot_meta(self):
        database = "db_tmq_rebalance_meta"
        topic = "topic_tmq_rebalance_meta"
        stb_num = 5
        ctb_per_stb = 30
        group_id = "g_tmq_rebalance_meta"

        # --- setup: 3 vgroups, 5 stables, 150 empty child tables ---
        tdSql.execute(f"drop database if exists {database}")
        tdSql.execute(f"create database {database} vgroups 3 keep 36500 wal_retention_period 3600")
        for i in range(stb_num):
            tdSql.execute(f"create stable {database}.stb{i} (ts timestamp, v int) tags(t int)")
        for i in range(stb_num):
            for j in range(ctb_per_stb):
                tdSql.execute(f"create table {database}.ct{i}_{j} using {database}.stb{i} tags({j})")
        tdSql.execute(f"flush database {database}")
        tdSql.execute(f"create topic {topic} with meta as database {database}")

        conf = {
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "experimental.snapshot.enable": "true",
            "msg.enable.batchmeta": "1",
        }

        consumers = []
        threads = []
        collected = set()
        lock = threading.Lock()
        errors = []
        stop_event = threading.Event()

        try:
            # A subscribes first and owns all 3 partitions; B/C then join and
            # trigger the rebalance. All consumers start polling together while
            # A still holds epoch 1, reproducing the lost-metadata race: A's
            # retained partition scans the snapshot with epoch 1, then discards
            # the create-table metadata when the epoch bumps to 2.
            a = Consumer(conf)
            a.subscribe([topic])
            consumers.append(a)

            b = Consumer(conf)
            b.subscribe([topic])
            consumers.append(b)

            c = Consumer(conf)
            c.subscribe([topic])
            consumers.append(c)

            for consumer in consumers:
                thread = threading.Thread(
                    target=self.collect_create_meta, args=(consumer, collected, lock, errors, stop_event)
                )
                thread.daemon = True
                thread.start()
                threads.append(thread)

            expected = {f"ct{i}_{j}" for i in range(stb_num) for j in range(ctb_per_stb)}
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                with lock:
                    if expected <= collected:
                        break
                time.sleep(0.2)

            stop_event.set()
            for thread in threads:
                thread.join(timeout=5)

            missing = expected - collected
            assert not missing, (
                f"missing create-table metadata for {len(missing)} child tables: "
                f"{sorted(missing)[:20]}, errors={errors[:5]}"
            )
            tdLog.info(f"rebalance snapshot meta delivered {len(expected)}/{len(expected)} child tables")
        finally:
            stop_event.set()
            for consumer in consumers:
                try:
                    consumer.close()
                except Exception:  # pragma: no cover - defensive
                    pass
            tdSql.execute(f"drop topic if exists {topic}")
            tdSql.execute(f"drop database if exists {database}")

        tdLog.info("rebalance snapshot meta ......................... [ passed ]")

    def test_rebalance_snapshot_meta(self):
        """Child-table metadata survives a consumer-group rebalance in snapshot mode.

        1. Create a database with 3 vgroups, 5 super tables and 150 empty child tables.
        2. Subscribe one consumer, then join two more consumers to trigger a rebalance.
        3. Consume with all three consumers and verify that every child table's
           create-table metadata is delivered (no table is lost during the rebalance).

        Catalog:
            - Subscribe

        Since: v3.3.6.70

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-11 Added rebalance snapshot meta regression

        """
        self.do_rebalance_snapshot_meta()
