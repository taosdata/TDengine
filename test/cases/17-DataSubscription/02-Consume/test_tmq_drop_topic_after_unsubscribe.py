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

import threading
import time

from taos.tmq import Consumer

from new_test_framework.utils import tdLog, tdSql


class TestCase:
    """Regression: `drop topic` must not be blocked by a stale tmq rebalance update.

    A rebalance result is computed from a subscription snapshot and persisted
    asynchronously (raft proposal + redo actions on all vgroups), so a
    subscribe/unsubscribe request can be committed in between. Such a request sets
    the consumer to REBALANCE and moves the changed topics into
    rebNewTopics/rebRemovedTopics, waiting for the next rebalance round to apply
    them. If the stale rebalance update overwrites the status with READY, those
    pending topics are never applied: checkConsumer() keeps reporting
    TSDB_CODE_MND_TOPIC_SUBSCRIBED because rebRemovedTopics still holds the topic,
    so `drop topic` fails until the consumer session times out.

    The race window is narrow and cannot be produced deterministically from SQL, so
    the case amplifies it: every unsubscribe is immediately followed by a
    `drop topic` retry loop, and a second consumer of the same group keeps changing
    its subscription so that rebalance rounds are in flight when the target
    consumer unsubscribes.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.db = "db_unsub_drop"
        cls.stb = "st"
        cls.vgroups = 2
        cls.base_ts = 1750000000000
        cls.session_timeout_ms = 45000
        cls.drop_timeout = 10
        # tmq_subscribe()/tmq_unsubscribe() retry synchronously for up to
        # SUBSCRIBE_RETRY_MAX_COUNT(240) * SUBSCRIBE_RETRY_INTERVAL(500ms) ~= 2 minutes
        # (clientTmq.c), so the churn worker needs a longer grace period to exit.
        cls.churn_exit_timeout = 150

    # ---- helpers ----
    def _new_consumer(self, group):
        return Consumer({
            "group.id": group,
            "client.id": "unsub_drop_client",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "enable.auto.commit": "true",
            "auto.commit.interval.ms": "1000",
            "auto.offset.reset": "earliest",
            "msg.with.table.name": "false",
            "session.timeout.ms": str(self.session_timeout_ms),
        })

    def _poll_rows(self, consumer, expect_rows, timeout):
        deadline = time.time() + timeout
        rows = 0
        while time.time() < deadline:
            res = consumer.poll(1)
            if res is None:
                continue
            err = res.error()
            if err is not None:
                # The consumer is rebalancing, the assignment is not ready yet.
                tdLog.info(f"poll skipped: {err}")
                time.sleep(0.2)
                continue
            for block in res.value():
                rows += len(block.fetchall())
            if rows >= expect_rows:
                break
        return rows

    def _wait_assignment(self, consumer, timeout):
        # The assignment is filled only after the consumer is READY and got its vgroups from
        # mnode. Received rows are not required here: the assigned vgroup may hold no row of
        # this topic, and poll() returns None on timeout for an idle consumer.
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                consumer.poll(1)
            except Exception as e:
                tdLog.info(f"poll skipped: {e}")
            if consumer.assignment():
                return True
        return False

    def _create_topic(self, topic):
        tdSql.execute(f"drop topic if exists {topic}")
        tdSql.execute(f"create topic {topic} as select ts, c1 from {self.db}.{self.stb}")

    def _drop_topic(self, topic, timeout):
        # Retry until the pending removal of the topic is applied by a rebalance round.
        deadline = time.time() + timeout
        while True:
            try:
                tdSql.execute(f"drop topic {topic}", queryTimes=1)
                return
            except Exception as e:
                if time.time() >= deadline:
                    tdLog.exit(f"drop topic {topic} still failed after {timeout}s, last error: {e}")
                tdLog.info(f"drop topic {topic} retry, error: {e}")
                time.sleep(0.2)

    def _unsubscribe(self, consumer, timeout):
        # The consumer may be temporarily in REBALANCE, which rejects the request.
        deadline = time.time() + timeout
        while True:
            try:
                consumer.unsubscribe()
                return
            except Exception as e:
                if "not exist" in str(e).lower():
                    # The consumer was already cleared by mnode, nothing left to unsubscribe.
                    tdLog.info(f"unsubscribe skipped, consumer is gone: {e}")
                    return
                if time.time() >= deadline:
                    tdLog.exit(f"unsubscribe still failed after {timeout}s, last error: {e}")
                tdLog.info(f"unsubscribe retry, error: {e}")
                time.sleep(0.2)

    def _churn(self, consumer, topic, stop, ready, cleaned):
        # The worker owns `consumer` for its whole lifetime, including the initial subscribe and
        # the final unsubscribe/close. tmq_subscribe()/tmq_unsubscribe() are synchronous and can
        # retry for up to SUBSCRIBE_RETRY_MAX_COUNT * SUBSCRIBE_RETRY_INTERVAL (~2 minutes), so
        # `stop` cannot cancel an in-flight call: the main thread must never touch this consumer.
        try:
            try:
                consumer.subscribe([topic])
            except Exception as e:
                tdLog.info(f"churn subscribe skipped: {e}")
            ready.set()

            # Keep subscription changes coming so that rebalance rounds are usually in flight.
            while not stop.is_set():
                try:
                    consumer.unsubscribe()
                except Exception as e:
                    tdLog.info(f"churn unsubscribe skipped: {e}")
                time.sleep(0.2)
                if stop.is_set():
                    break
                try:
                    consumer.subscribe([topic])
                except Exception as e:
                    tdLog.info(f"churn subscribe skipped: {e}")
                try:
                    consumer.poll(1)
                except Exception as e:
                    tdLog.info(f"churn poll skipped: {e}")
        finally:
            try:
                consumer.unsubscribe()
            except Exception as e:
                tdLog.info(f"churn final unsubscribe skipped: {e}")
            try:
                consumer.close()
            except Exception as e:
                tdLog.info(f"churn close skipped: {e}")
            # The consumer is fully released by its owner, the main thread may continue.
            cleaned.set()

    # ---- impl ----
    def prepare(self):
        tdSql.execute(f"drop database if exists {self.db}")
        tdSql.execute(f"create database {self.db} vgroups {self.vgroups} precision 'ms' wal_retention_period 3600")
        tdSql.execute(f"use {self.db}")
        tdSql.execute(f"create table {self.stb} (ts timestamp, c1 int) tags (t1 int)")
        for i in range(self.vgroups * 2):
            tdSql.execute(f"create table ct{i} using {self.stb} tags({i})")
            tdSql.execute(f"insert into ct{i} values ({self.base_ts + i}, {i})")

    def do_unsubscribe_then_drop_topic(self, rounds):
        # Sequential scenario: the pending removal must be applied by the next round.
        for r in range(rounds):
            topic = f"tp_unsub_seq_{r}"
            group = f"cg_unsub_seq_{r}"
            self._create_topic(topic)
            consumer = self._new_consumer(group)
            try:
                consumer.subscribe([topic])
                if self._poll_rows(consumer, 1, self.drop_timeout) < 1:
                    tdLog.exit(f"consumer group {group} consumed no row from topic {topic}")
                self._unsubscribe(consumer, self.drop_timeout)
                self._drop_topic(topic, self.drop_timeout)
            finally:
                try:
                    consumer.close()
                except Exception as e:
                    tdLog.info(f"consumer close skipped: {e}")

        print("unsubscribe then drop topic ......... [ passed ]")

    def do_unsubscribe_while_rebalancing(self, rounds):
        # Concurrent scenario: a rebalance round of another consumer of the same group is
        # usually in flight when the target consumer unsubscribes, which is the defect window.
        for r in range(rounds):
            topic = f"tp_unsub_race_{r}"
            group = f"cg_unsub_race_{r}"
            self._create_topic(topic)
            churn_consumer = self._new_consumer(group)
            target = self._new_consumer(group)
            stop = threading.Event()
            ready = threading.Event()
            cleaned = threading.Event()
            churn_thread = threading.Thread(target=self._churn,
                                            args=(churn_consumer, topic, stop, ready, cleaned))
            try:
                churn_thread.start()
                if not ready.wait(self.drop_timeout):
                    tdLog.exit(f"churn consumer of group {group} did not subscribe topic {topic}")
                target.subscribe([topic])
                if not self._wait_assignment(target, self.drop_timeout):
                    tdLog.exit(f"consumers of group {group} got no vgroup assignment for topic {topic}")

                # Let the churn consumer trigger at least one rebalance round, then unsubscribe
                # the target consumer while that round may still be committing.
                time.sleep(2.2)
                self._unsubscribe(target, self.drop_timeout)
                # The churn consumer still subscribes to the topic, which blocks `drop topic`.
                # Ask its owner thread to unsubscribe and close it; never touch it from here.
                stop.set()
                if not cleaned.wait(self.churn_exit_timeout):
                    tdLog.exit(f"churn worker of group {group} did not finish unsubscribe/close in "
                               f"{self.churn_exit_timeout}s")
                churn_thread.join(self.churn_exit_timeout)
                self._drop_topic(topic, self.drop_timeout)
            finally:
                stop.set()
                # The churn consumer is owned by the worker thread: wait for it, never close it here.
                churn_thread.join(self.churn_exit_timeout)
                try:
                    target.close()
                except Exception as e:
                    tdLog.info(f"consumer close skipped: {e}")

        print("unsubscribe while rebalancing ......... [ passed ]")

    # ---- main ----
    def test_tmq_drop_topic_after_unsubscribe(self):
        """Topic can be dropped right after the consumer unsubscribes

        1. Repeat subscribe -> poll -> unsubscribe -> drop topic; the drop must succeed
           well before the consumer session timeout
        2. Repeat the same sequence while another consumer of the same group keeps changing
           its subscription, so a rebalance round is usually in flight when the target
           consumer unsubscribes

        Catalog:
            - Subscription

        Since: v3.4.3.0

        Labels: common,ci

        Jira: 7141634318

        History:
            - 2026-10-22 mmwang Add regression case for drop topic blocked by a stale
              rebalance update

        """
        self.prepare()
        self.do_unsubscribe_then_drop_topic(2)
        self.do_unsubscribe_while_rebalancing(3)
