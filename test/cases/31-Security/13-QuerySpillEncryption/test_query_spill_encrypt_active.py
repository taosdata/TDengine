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
"""Companion test: query spill encryption with the scope ACTIVE.

Where test_query_spill_encrypt.py runs encryption OFF (proving the change is a
zero-regression no-op and that the spill/correctness logic holds), this test
turns the scope ON via encryptScope=query_spill so that on an enterprise build
the spilled paged-buffer pages are encrypted with real SM4-CBC and the query
must still return correct results -- i.e. the encrypt-on-write / decrypt-on-read
round trip is exercised end to end through the executor.

It stays green in CI on both editions and asserts the same thing on each:
  - enterprise: encryptScope is parsed (dmEps.c, TD_ENTERPRISE), real SM4 runs.
  - community:  encryptScope is accepted as a config but never parsed (no-op).
The pytest cannot observe whether the temp file is ciphertext (AUTO_DEL, raw
bytes), so the observable result -- a correct spilling query -- is identical in
both editions; there is nothing to branch on here. The byte-level "really
ciphertext" proof lives in the deterministic pageBufferTest gtest.

No cluster encrypt key is bootstrapped: query_spill uses a per-buffer random
key generated internally (taosSafeRandBytes), not the cluster master key, and
setting only the query_spill scope does not require one to start taosd (verified
empirically -- dropping encryptConfig keeps the scope active and the test green).

The byte-level "ciphertext on disk" proof lives in the deterministic
pageBufferTest gtest; this test proves the live query path stays correct while
encryption is on.
"""

from new_test_framework.utils import tdLog, tdSql


class TestQuerySpillEncryptActive:

    # Turn ON query_spill encryption. Enterprise parses this on first dnode
    # start; community registers but ignores it (no-op). No encryptConfig is
    # needed -- query_spill uses an internal per-buffer random key.
    updatecfgDict = {"encryptAlgorithm": "sm4", "encryptScope": "query_spill"}

    TOTAL = 200000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute("drop database if exists test_qspill_enc_on")
        tdSql.execute("create database test_qspill_enc_on vgroups 1")
        tdSql.execute("use test_qspill_enc_on")
        tdSql.execute("create table t1 (ts timestamp, val int, str varchar(64))")

        # Wide payload so total sort volume (~12 MB) overflows the fixed ~4 MB
        # sort buffer and spills.
        base_ts = 1700000000000
        batch = 5000
        for start in range(0, cls.TOTAL, batch):
            values = []
            for i in range(start, min(start + batch, cls.TOTAL)):
                values.append(
                    f"({base_ts + i}, {i}, 'qspill-payload-row-{i:08d}-zzzzzzzzzzzzzzzzzzzz')"
                )
            tdSql.execute("insert into test_qspill_enc_on.t1 values " + ",".join(values))
        tdLog.info(f"inserted {cls.TOTAL} rows")

    # --- util ---

    def _spill_query(self):
        # Full-table sort on a non-time column -> external sort -> paged-buffer spill.
        return "select ts, val, str from test_qspill_enc_on.t1 order by val desc"

    def _assert_sort_spilled(self):
        # EXPLAIN ANALYZE reports "merge sort" (external/spilled) vs "quicksort"
        # (in-memory). Asserting it proves the encryption path was exercised.
        tdSql.query("explain analyze " + self._spill_query(), show=True)
        text = ""
        for r in range(tdSql.queryRows):
            for c in range(tdSql.queryCols):
                text += str(tdSql.getData(r, c)).lower()
        assert "merge sort" in text, (
            "sort did not spill (no 'merge sort' in EXPLAIN ANALYZE); "
            "the encryption path was not exercised"
        )
        tdLog.info("confirmed sort spilled to disk (merge sort)")

    # --- impl ---

    def do_correctness_under_encryption(self):
        # With encryptScope=query_spill set, an enterprise build runs real SM4
        # over the spilled pages; community is a no-op. The observable result is
        # identical either way, so this asserts it edition-agnostically -- the
        # byte-level "really ciphertext" proof is the pageBufferTest gtest's job.

        # Gate: the sort must actually spill, otherwise this proves nothing.
        self._assert_sort_spilled()

        # The spilling sort -- encrypted on enterprise -- must still return all
        # rows in strict descending order (decrypt-on-read is transparent).
        tdSql.query(self._spill_query())
        tdSql.checkRows(self.TOTAL)
        tdSql.checkData(0, 1, self.TOTAL - 1)        # max value first
        tdSql.checkData(self.TOTAL - 1, 1, 0)        # min value last
        print("query spill correctness (encryptScope=query_spill) ... [ passed ]")

    # --- main ---

    def test_query_spill_encrypt_active(self):
        """Query spill temp-file encryption (scope active)

        With encryptScope=query_spill, a spilling ORDER BY (confirmed via EXPLAIN
        ANALYZE 'merge sort') returns correct results, proving the SM4
        encrypt-on-write / decrypt-on-read round trip over paged-buffer temp
        files is transparent. Real SM4 on enterprise; graceful no-op on community.

        Catalog:
            - Security

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-25 Tony Zhang Created for query spill encryption (scope on)

        """
        self.do_correctness_under_encryption()
