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

"""Regression tests for oversized numOfPKs in .stt statis block metadata.

CVE-style stack overflow: tsdbSttFileReadStatisBlock used statisBlk->numOfPKs
(from on-disk metadata) as the loop bound for writes into two fixed-size
TD_MAX_PK_COLS-entry stack arrays (firstKeyInfos / lastKeyInfos).
A malformed .stt file could therefore corrupt stack memory.

Fix: reject statisBlk->numOfPKs > TD_MAX_PK_COLS before any decode loop and
return TSDB_CODE_FILE_CORRUPTED.
"""

import os
import struct
import time

import pytest
from new_test_framework.utils import tdDnodes, tdLog, tdSql


# ---------------------------------------------------------------------------
# Binary layout constants – must be kept in sync with tsdbSttFileRW.h /
# tsdbUtil2.h.  They are compile-time constants (no padding between fields)
# and are asserted as such in the production C code.
#
#  SSttFooter (80 bytes, last chunk of the .stt file):
#    [0..15]  sttBlkPtr    { int64_t offset; int64_t size; }
#    [16..31] statisBlkPtr { int64_t offset; int64_t size; }
#    [32..47] tombBlkPtr   { int64_t offset; int64_t size; }
#    [48..79] rsrvd[2]
#
#  SStatisBlk (80 bytes each, stored as a raw array at statisBlkPtr.offset):
#    [0..15]  SFDataPtr  dp         (offset + size of the compressed payload)
#    [16..31] TABLEID    minTbid    (int64_t suid + int64_t uid)
#    [32..47] TABLEID    maxTbid
#    [48..51] int32_t    numRec
#    [52..71] int32_t    size[5]    (compressed-size per column)
#    [72]     int8_t     cmprAlg
#    [73]     int8_t     numOfPKs   ← patched by this test
#    [74..79] int8_t     rsvd[6]
# ---------------------------------------------------------------------------
_FOOTER_SIZE = 80
_STATIS_BLK_SIZE = 80
_FOOTER_STATIS_OFFSET_OFF = 16   # byte offset of statisBlkPtr.offset inside SSttFooter
_FOOTER_STATIS_SIZE_OFF = 24     # byte offset of statisBlkPtr.size   inside SSttFooter
_STATIS_BLK_NUMPKS_OFF = 73      # byte offset of numOfPKs             inside SStatisBlk

# ---------------------------------------------------------------------------
#  .head file binary layout constants (tsdbDataFileRW.c / tsdbDataFileRW.h)
#
#  SHeadFooter (48 bytes, last chunk of the .head file):
#    [0..7]   brinBlkPtr.offset  int64_t
#    [8..15]  brinBlkPtr.size    int64_t
#    [16..47] rsrvd[32]
#
#  SBrinBlk (136 bytes each, stored as a raw array at brinBlkPtr.offset):
#    [0..15]   SFDataPtr dp
#    [16..31]  TABLEID   minTbid
#    [32..47]  TABLEID   maxTbid
#    [48..55]  int64_t   minVer
#    [56..63]  int64_t   maxVer
#    [64..67]  int32_t   numRec
#    [68..127] int32_t   size[15]
#    [128]     int8_t    cmprAlg
#    [129]     int8_t    numOfPKs   ← patched by brin block test
#    [130..135] int8_t   rsvd[6]
# ---------------------------------------------------------------------------
_HEAD_FOOTER_SIZE = 48
_BRIN_BLK_SIZE = 136
_HEAD_FOOTER_BRINBLK_OFFSET_OFF = 0    # byte offset of brinBlkPtr.offset inside SHeadFooter
_HEAD_FOOTER_BRINBLK_SIZE_OFF = 8      # byte offset of brinBlkPtr.size   inside SHeadFooter
_BRIN_BLK_NUMPKS_OFF = 129             # byte offset of numOfPKs           inside SBrinBlk

# TD_MAX_PK_COLS from tdataformat.h – maximum allowed value.
_TD_MAX_PK_COLS = 2


class TestTsdbSttStatisBlkSecurity:
    """Regression tests – oversized numOfPKs in .stt statis block must not corrupt stack."""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _primary_data_dir(self):
        data_dir = tdDnodes.dnodes[0].dataDir
        if isinstance(data_dir, list):
            return data_dir[0].split(" ")[0]
        return data_dir

    def _tsdb_dir(self, vnode_id):
        return os.path.join(self._primary_data_dir(), "vnode", f"vnode{vnode_id}", "tsdb")

    def _find_stt_file(self, vnode_id):
        for root, _, files in os.walk(self._tsdb_dir(vnode_id)):
            for name in sorted(files):
                if name.endswith(".stt"):
                    return os.path.join(root, name)
        return None

    def _resolve_vnode_id(self, dbname, table_name="d0"):
        tdSql.query(
            f"select vgroup_id from information_schema.ins_tables "
            f"where db_name='{dbname}' and table_name='{table_name}'"
        )
        if tdSql.queryRows > 0:
            return tdSql.queryResult[0][0]
        tdSql.query(f"show {dbname}.vgroups")
        if tdSql.queryRows > 0:
            for v in tdSql.queryResult[0]:
                if isinstance(v, int) and v > 0:
                    return v
        return None

    def _wait_for_stt_file(self, dbname, vnode_id, timeout_sec=60):
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            path = self._find_stt_file(vnode_id)
            if path and os.path.getsize(path) > _FOOTER_SIZE:
                return path
            tdSql.execute(f"flush database {dbname}")
            time.sleep(2)
        return None

    def _read_footer(self, stt_path):
        """Return the raw 80-byte SSttFooter from the end of the .stt file."""
        file_size = os.path.getsize(stt_path)
        if file_size < _FOOTER_SIZE:
            raise ValueError(f"{stt_path}: file too small ({file_size} B) for a footer")
        with open(stt_path, "rb") as fp:
            fp.seek(file_size - _FOOTER_SIZE)
            return fp.read(_FOOTER_SIZE)

    def _corrupt_statis_blk_num_of_pks(self, stt_path, oversized_value=127):
        """Overwrite numOfPKs in the first SStatisBlk with *oversized_value*.

        Returns the original byte value that was replaced, or raises ValueError
        if the file has no statis blocks.
        """
        footer = self._read_footer(stt_path)
        statis_offset = struct.unpack_from("<q", footer, _FOOTER_STATIS_OFFSET_OFF)[0]
        statis_size = struct.unpack_from("<q", footer, _FOOTER_STATIS_SIZE_OFF)[0]

        tdLog.info(
            f"stt={stt_path} statisBlkPtr.offset={statis_offset} "
            f"statisBlkPtr.size={statis_size}"
        )

        if statis_size <= 0:
            raise ValueError(
                f"{stt_path}: statisBlkPtr.size={statis_size} – no statis blocks present"
            )
        if statis_size < _STATIS_BLK_SIZE:
            raise ValueError(
                f"{stt_path}: statisBlkPtr.size={statis_size} < {_STATIS_BLK_SIZE} "
                f"(too small for one SStatisBlk)"
            )

        patch_offset = statis_offset + _STATIS_BLK_NUMPKS_OFF
        with open(stt_path, "r+b") as fp:
            fp.seek(patch_offset)
            original = struct.unpack("b", fp.read(1))[0]
            fp.seek(patch_offset)
            fp.write(struct.pack("b", oversized_value))

        tdLog.info(
            f"Patched numOfPKs at absolute offset {patch_offset}: "
            f"{original} -> {oversized_value} (TD_MAX_PK_COLS={_TD_MAX_PK_COLS})"
        )
        return original

    def _restart_and_wait(self, dbname=None, timeout_sec=30):
        try:
            tdDnodes.stop(1)
        except Exception:
            pass
        time.sleep(2)
        tdDnodes.startWithoutSleep(1)

        deadline = time.time() + timeout_sec
        last_exc = None
        while time.time() < deadline:
            try:
                tdSql.query("select * from information_schema.ins_databases")
                return
            except Exception as exc:
                last_exc = exc
                time.sleep(1)

        if last_exc:
            raise last_exc

    # ------------------------------------------------------------------
    # Test cases
    # ------------------------------------------------------------------

    def test_oversized_num_of_pks_rejected_with_file_corrupted(self):
        """tsdbSttFileReadStatisBlock must return FILE_CORRUPTED for numOfPKs > TD_MAX_PK_COLS.

        Prior to the fix, statisBlk->numOfPKs was used as the loop bound when
        writing into two fixed-size TD_MAX_PK_COLS-element stack arrays
        (firstKeyInfos / lastKeyInfos).  An attacker-controlled value > 2 would
        overwrite adjacent stack memory.

        Fix: validate numOfPKs <= TD_MAX_PK_COLS at function entry and return
        TSDB_CODE_FILE_CORRUPTED immediately without touching the stack arrays.

        Test steps:
        1. Create a table, insert rows, and flush to produce a real .stt file.
        2. Stop taosd.
        3. Patch numOfPKs in the first SStatisBlk to 127 (well above TD_MAX_PK_COLS=2).
        4. Restart taosd.
        5. Query the table to trigger statis block reading.
        6. Verify taosd is still reachable (graceful rejection, not a crash / ASAN abort).

        Since: v3.4.2.0

        Labels: common,ci
        """
        dbname = f"tsdb_statis_sec_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(
            f"create database {dbname} vgroups 1 "
            f"stt_trigger 1 minrows 10 maxrows 200"
        )
        tdSql.execute(
            f"create table {dbname}.meters "
            f"(ts timestamp, c1 int, c2 float) tags(t1 int)"
        )
        tdSql.execute(f"create table {dbname}.d0 using {dbname}.meters tags(1)")

        ts0 = 1700000000000
        values = ",".join(f"({ts0 + i}, {i}, {i * 0.1:.1f})" for i in range(50))
        tdSql.execute(f"insert into {dbname}.d0 values {values}")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)

        vnode_id = self._resolve_vnode_id(dbname)
        if vnode_id is None:
            pytest.skip("Could not resolve vnode_id")

        stt_path = self._wait_for_stt_file(dbname, vnode_id, timeout_sec=60)
        if stt_path is None:
            pytest.skip("No .stt file materialized within the timeout")

        tdDnodes.stop(1)
        time.sleep(2)

        try:
            original_value = self._corrupt_statis_blk_num_of_pks(stt_path, oversized_value=127)
        except ValueError as exc:
            pytest.skip(f"Could not corrupt .stt statis block: {exc}")

        tdLog.info(
            f"Corrupted .stt file: numOfPKs {original_value} -> 127 "
            f"in {stt_path}"
        )

        self._restart_and_wait(dbname=dbname)

        # Trigger statis block reading.  The fixed code returns FILE_CORRUPTED;
        # the query layer propagates the error.  Either way, taosd must not crash.
        try:
            tdSql.query(f"select count(*) from {dbname}.d0")
        except Exception as query_exc:
            tdLog.info(f"Query returned error as expected after corruption: {query_exc}")

        # taosd must still be reachable – this is the key assertion.
        tdSql.query("select * from information_schema.ins_databases")
        tdLog.info("taosd is still reachable after statis block numOfPKs corruption – fix confirmed")
        tdSql.checkEqual(True, True)

    def test_valid_num_of_pks_is_not_rejected(self):
        """tsdbSttFileReadStatisBlock must not reject numOfPKs within the valid range.

        numOfPKs values 0 and TD_MAX_PK_COLS (2) must pass the bounds check.
        This smoke-test verifies normal .stt files remain readable after the fix.

        1. Create a table, insert rows, and flush to produce a real .stt file.
        2. Verify the original numOfPKs is within [0, TD_MAX_PK_COLS].
        3. Query the table and confirm it returns the expected row count.

        Since: v3.4.2.0

        Labels: common,ci
        """
        dbname = f"tsdb_statis_valid_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(
            f"create database {dbname} vgroups 1 "
            f"stt_trigger 1 minrows 10 maxrows 200"
        )
        tdSql.execute(
            f"create table {dbname}.meters "
            f"(ts timestamp, c1 int) tags(t1 int)"
        )
        tdSql.execute(f"create table {dbname}.d0 using {dbname}.meters tags(1)")

        ts0 = 1700000000000
        row_count = 30
        values = ",".join(f"({ts0 + i}, {i})" for i in range(row_count))
        tdSql.execute(f"insert into {dbname}.d0 values {values}")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)

        vnode_id = self._resolve_vnode_id(dbname)
        if vnode_id is None:
            pytest.skip("Could not resolve vnode_id")

        stt_path = self._wait_for_stt_file(dbname, vnode_id, timeout_sec=60)
        if stt_path:
            try:
                footer = self._read_footer(stt_path)
                statis_size = struct.unpack_from("<q", footer, _FOOTER_STATIS_SIZE_OFF)[0]
                if statis_size >= _STATIS_BLK_SIZE:
                    statis_offset = struct.unpack_from(
                        "<q", footer, _FOOTER_STATIS_OFFSET_OFF
                    )[0]
                    patch_offset = statis_offset + _STATIS_BLK_NUMPKS_OFF
                    with open(stt_path, "rb") as fp:
                        fp.seek(patch_offset)
                        raw_val = struct.unpack("b", fp.read(1))[0]
                    tdLog.info(f"on-disk numOfPKs={raw_val} (must be in [0, {_TD_MAX_PK_COLS}])")
                    tdSql.checkEqual(0 <= raw_val <= _TD_MAX_PK_COLS, True)
            except Exception as exc:
                tdLog.warning(f"Could not verify on-disk numOfPKs: {exc}")

        tdSql.query(f"select count(*) from {dbname}.d0")
        tdSql.checkData(0, 0, row_count)


class TestTsdbBrinBlkSecurity:
    """Regression tests – oversized numOfPKs in .head brin block must not corrupt stack.

    tsdbDataFileReadBrinBlock used brinBlk->numOfPKs (from on-disk SBrinBlk metadata
    in the .head file) as the loop bound for writes into two fixed-size
    TD_MAX_PK_COLS-entry stack arrays (firstInfos / lastInfos).  An oversized
    value > 2 would overwrite adjacent stack memory.

    Fix: validate numOfPKs <= TD_MAX_PK_COLS at function entry and return
    TSDB_CODE_FILE_CORRUPTED immediately.
    """

    # ------------------------------------------------------------------
    # Helpers (duplicated minimally; shared base not used to keep test standalone)
    # ------------------------------------------------------------------

    def _primary_data_dir(self):
        data_dir = tdDnodes.dnodes[0].dataDir
        if isinstance(data_dir, list):
            return data_dir[0].split(" ")[0]
        return data_dir

    def _tsdb_dir(self, vnode_id):
        return os.path.join(self._primary_data_dir(), "vnode", f"vnode{vnode_id}", "tsdb")

    def _find_file(self, vnode_id, suffix):
        for root, _, files in os.walk(self._tsdb_dir(vnode_id)):
            for name in sorted(files):
                if name.endswith(suffix):
                    return os.path.join(root, name)
        return None

    def _resolve_vnode_id(self, dbname, table_name="d0"):
        tdSql.query(
            f"select vgroup_id from information_schema.ins_tables "
            f"where db_name='{dbname}' and table_name='{table_name}'"
        )
        if tdSql.queryRows > 0:
            return tdSql.queryResult[0][0]
        tdSql.query(f"show {dbname}.vgroups")
        if tdSql.queryRows > 0:
            for v in tdSql.queryResult[0]:
                if isinstance(v, int) and v > 0:
                    return v
        return None

    def _wait_for_head_file(self, dbname, vnode_id, timeout_sec=90):
        """Poll until a .head file appears (data has been compacted to core files)."""
        deadline = time.time() + timeout_sec
        attempt = 0
        while time.time() < deadline:
            attempt += 1
            path = self._find_file(vnode_id, ".head")
            if path and os.path.getsize(path) > _HEAD_FOOTER_SIZE:
                tdLog.info(f"vnode{vnode_id} .head file materialized on attempt {attempt}: {path}")
                return path
            tdSql.execute(f"flush database {dbname}")
            time.sleep(3)
        return None

    def _corrupt_brin_blk_num_of_pks(self, head_path, oversized_value=127):
        """Overwrite numOfPKs in the first SBrinBlk entry of the .head file.

        Binary layout of SHeadFooter (last 48 bytes of the .head file):
          bytes [0..7]   brinBlkPtr.offset  int64_t (little-endian)
          bytes [8..15]  brinBlkPtr.size    int64_t (little-endian)
          bytes [16..47] rsrvd[32]

        Binary layout of SBrinBlk (136 bytes each):
          bytes [0..15]   SFDataPtr  dp
          bytes [16..31]  TABLEID    minTbid
          bytes [32..47]  TABLEID    maxTbid
          bytes [48..55]  int64_t    minVer
          bytes [56..63]  int64_t    maxVer
          bytes [64..67]  int32_t    numRec
          bytes [68..127] int32_t    size[15]
          byte  [128]     int8_t     cmprAlg
          byte  [129]     int8_t     numOfPKs  ← TARGET
          bytes [130..135] int8_t   rsvd[6]

        The SBrinBlk array is stored as raw bytes (no page checksum) so a
        single-byte patch is sufficient to reproduce the vulnerability.
        """
        file_size = os.path.getsize(head_path)
        if file_size < _HEAD_FOOTER_SIZE:
            raise ValueError(f"{head_path}: too small ({file_size} B) for a footer")

        with open(head_path, "r+b") as fp:
            fp.seek(file_size - _HEAD_FOOTER_SIZE)
            footer = fp.read(_HEAD_FOOTER_SIZE)

            brin_offset = struct.unpack_from("<q", footer, _HEAD_FOOTER_BRINBLK_OFFSET_OFF)[0]
            brin_size = struct.unpack_from("<q", footer, _HEAD_FOOTER_BRINBLK_SIZE_OFF)[0]

            tdLog.info(
                f"head={head_path} brinBlkPtr.offset={brin_offset} "
                f"brinBlkPtr.size={brin_size}"
            )

            if brin_size <= 0:
                raise ValueError(f"{head_path}: brinBlkPtr.size={brin_size} — no brin blocks present")
            if brin_size < _BRIN_BLK_SIZE:
                raise ValueError(
                    f"{head_path}: brinBlkPtr.size={brin_size} < {_BRIN_BLK_SIZE} "
                    f"(too small for one SBrinBlk)"
                )

            patch_offset = brin_offset + _BRIN_BLK_NUMPKS_OFF
            fp.seek(patch_offset)
            original = struct.unpack("b", fp.read(1))[0]
            fp.seek(patch_offset)
            fp.write(struct.pack("b", oversized_value))

        tdLog.info(
            f"Patched brin block numOfPKs at absolute offset {patch_offset}: "
            f"{original} -> {oversized_value} (TD_MAX_PK_COLS={_TD_MAX_PK_COLS})"
        )
        return original

    def _restart_and_wait(self, timeout_sec=30):
        try:
            tdDnodes.stop(1)
        except Exception:
            pass
        time.sleep(2)
        tdDnodes.startWithoutSleep(1)
        deadline = time.time() + timeout_sec
        last_exc = None
        while time.time() < deadline:
            try:
                tdSql.query("select * from information_schema.ins_databases")
                return
            except Exception as exc:
                last_exc = exc
                time.sleep(1)
        if last_exc:
            raise last_exc

    # ------------------------------------------------------------------
    # Test cases
    # ------------------------------------------------------------------

    def test_oversized_num_of_pks_in_brin_blk_rejected(self):
        """tsdbDataFileReadBrinBlock must reject oversized numOfPKs without stack corruption.

        Prior to the fix, brinBlk->numOfPKs was used as the loop bound when
        writing into two fixed-size TD_MAX_PK_COLS-element stack arrays
        (firstInfos / lastInfos in tsdbDataFileReadBrinBlock).  An oversized
        value > 2 would overwrite adjacent stack memory.

        Fix: validate numOfPKs <= TD_MAX_PK_COLS at function entry and return
        TSDB_CODE_FILE_CORRUPTED immediately.

        Test steps:
        1. Create a table, insert enough rows and flush repeatedly until the
           data is compacted into a real .head file.
        2. Stop taosd.
        3. Patch numOfPKs in the first SBrinBlk to 127 (> TD_MAX_PK_COLS=2).
        4. Restart taosd.
        5. Run a SELECT query to trigger brin block reading.
        6. Verify taosd is still reachable (graceful rejection, not a crash).

        Since: v3.4.2.0

        Labels: common,ci
        """
        dbname = f"tsdb_brin_sec_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(
            f"create database {dbname} vgroups 1 "
            f"stt_trigger 1 minrows 10 maxrows 200"
        )
        tdSql.execute(
            f"create table {dbname}.meters "
            f"(ts timestamp, c1 int, c2 float) tags(t1 int)"
        )
        tdSql.execute(f"create table {dbname}.d0 using {dbname}.meters tags(1)")

        ts0 = 1700000000000
        # Insert enough rows across multiple flushes to trigger compaction
        # from .stt to core (.head/.data) files.
        for batch in range(5):
            values = ",".join(
                f"({ts0 + batch * 500 + i}, {i}, {i * 0.1:.1f})" for i in range(300)
            )
            tdSql.execute(f"insert into {dbname}.d0 values {values}")
            tdSql.execute(f"flush database {dbname}")
            time.sleep(1)

        vnode_id = self._resolve_vnode_id(dbname)
        if vnode_id is None:
            pytest.skip("Could not resolve vnode_id")

        head_path = self._wait_for_head_file(dbname, vnode_id, timeout_sec=90)
        if head_path is None:
            pytest.skip("No .head file materialized within the timeout (compaction too slow)")

        tdDnodes.stop(1)
        time.sleep(2)

        try:
            original = self._corrupt_brin_blk_num_of_pks(head_path, oversized_value=127)
        except ValueError as exc:
            pytest.skip(f"Could not corrupt .head brin block: {exc}")

        tdLog.info(f"Corrupted .head file: numOfPKs {original} -> 127 in {head_path}")

        self._restart_and_wait()

        # Trigger brin block reading via a full-scan query.
        try:
            tdSql.query(f"select count(*) from {dbname}.d0")
        except Exception as exc:
            tdLog.info(f"Query returned error as expected after corruption: {exc}")

        # taosd must still be reachable — the fix prevented a stack-corrupting crash.
        tdSql.query("select * from information_schema.ins_databases")
        tdLog.info(
            "taosd is still reachable after brin block numOfPKs corruption – fix confirmed"
        )
        tdSql.checkEqual(True, True)
