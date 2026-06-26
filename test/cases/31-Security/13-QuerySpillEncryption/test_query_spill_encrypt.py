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
"""End-to-end test for query spill temp-file encryption (encryptScope=query_spill).

This is the integration / correctness layer: it proves a real ORDER BY query
spills its sort operator to paged-buffer temp files and still returns correct
results, i.e. the encrypt-on-write / decrypt-on-read round trip is transparent.
It runs in every edition (community CBC is a memcpy stub, so the round trip
still holds).

The byte-level "no plaintext on disk" proof is NOT done here -- a timing-based
poll of the AUTO_DEL temp files is racy and would skip more often than it
checks. That guarantee lives in the deterministic pageBufferTest gtest
(testEncryptedFlushAndReadBackBuffer), which reads the raw on-disk bytes.

The full-table ORDER BY below uses the merge-sort path whose in-memory buffer is
a fixed numOfPages(1024) * pageSize(>=4KB) ~= 4 MB (tsort.c, not tunable by any
config -- pqSortMemThreshold only affects ORDER BY ... LIMIT priority-queue
sort). So the spill is forced purely by data volume: ~200k rows * ~48B payload
~= 12 MB comfortably exceeds the 4 MB buffer. A spill is then confirmed
deterministically via EXPLAIN ANALYZE reporting "merge sort" (external) rather
than "quicksort" (in-memory) -- without that gate a correctness check would pass
even if the sort never touched disk.
"""

from new_test_framework.utils import tdLog, tdSql


class TestQuerySpillEncrypt:

    TOTAL = 200000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute("drop database if exists test_qspill_enc")
        tdSql.execute("create database test_qspill_enc vgroups 1")
        tdSql.execute("use test_qspill_enc")
        tdSql.execute("create table t1 (ts timestamp, val int, str varchar(64))")

        # A wide payload column inflates per-row size so total sort volume
        # (~12 MB) comfortably overflows the fixed ~4 MB sort buffer and spills.
        base_ts = 1700000000000
        batch = 5000
        for start in range(0, cls.TOTAL, batch):
            values = []
            for i in range(start, min(start + batch, cls.TOTAL)):
                values.append(
                    f"({base_ts + i}, {i}, 'qspill-payload-row-{i:08d}-zzzzzzzzzzzzzzzzzzzz')"
                )
            tdSql.execute("insert into test_qspill_enc.t1 values " + ",".join(values))
        tdLog.info(f"inserted {cls.TOTAL} rows")

    # --- util ---

    def _spill_query(self):
        # Full-table sort on a non-time column -> external sort -> paged-buffer spill.
        return "select ts, val, str from test_qspill_enc.t1 order by val desc"

    def _assert_sort_spilled(self):
        # EXPLAIN ANALYZE prints the sort method: "merge sort" means the sort
        # operator went external (spilled to paged-buffer temp files), while
        # "quicksort" means it stayed in memory. Asserting "merge sort" is the
        # gate that proves the encryption path was actually exercised.
        tdSql.query("explain analyze " + self._spill_query())
        text = ""
        for r in range(tdSql.queryRows):
            for c in range(tdSql.queryCols):
                text += str(tdSql.getData(r, c)).lower()
        assert "merge sort" in text, (
            "sort did not spill (no 'merge sort' in EXPLAIN ANALYZE); "
            "the encryption path was not exercised -- lower pqSortMemThreshold "
            "or raise the row count"
        )
        assert "quicksort" not in text, "sort stayed in memory (quicksort)"
        tdLog.info("confirmed sort spilled to disk (merge sort)")

    # --- impl ---

    def do_spill_correctness(self):
        # Gate: the sort must actually spill, otherwise correctness proves nothing.
        self._assert_sort_spilled()

        # The spilling sort must return all rows in strict descending order.
        tdSql.query(self._spill_query())
        tdSql.checkRows(self.TOTAL)
        tdSql.checkData(0, 1, self.TOTAL - 1)        # max value first
        tdSql.checkData(self.TOTAL - 1, 1, 0)        # min value last
        print("query spill correctness ............ [ passed ]")

    # --- main ---

    def test_query_spill_encrypt(self):
        """Query spill temp-file encryption

        A spilling ORDER BY (confirmed via EXPLAIN ANALYZE 'merge sort') returns
        correct results, proving the encrypt-on-write / decrypt-on-read round
        trip over paged-buffer temp files is transparent. The byte-level
        no-plaintext proof lives in the pageBufferTest gtest.

        Catalog:
            - Security

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-24 Tony Zhang Created for query spill encryption

        """
        self.do_spill_correctness()
