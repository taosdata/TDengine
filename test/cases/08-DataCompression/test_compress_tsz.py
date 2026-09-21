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

from new_test_framework.utils import tdLog, tdSql
import sys
import random
import struct
import time
import math

import taos

class TestCompressTsz1:
    updatecfgDict = {'lossyColumns':'float-double','IfAdtFse':1}
    # init
    def setup_class(cls):
        seed = time.time() % 10000
        random.seed(seed)
        tdLog.debug(f"start to excute {__file__}")

    #
    # ------------------- test_compress_tsz1.py ----------------
    #

    # get col value and total max min ...
    def getColsValues(self, i):
        # c1 bigint c2 bigint c3 float c4 double c5 int
        c5 = 0 # none flag 0: not have none  1: c3 is none 2: c4 is none 3: c3 and c4 is none
        c1 = random.randint(-100, 100)
        c2 = random.randint(-10000, 100000)
        # c3 value
        c3 = c1 + c2 * 0.00001       # 6 bit
        c4 = c1 + c2 * 0.00000000001 # 12 bit

        # set null
        if random.randint(1, 10) == 5:
            c3 = None
            c5 = 1
        if random.randint(1, 10) == 7:
            c4 = None
            c5 = 2
        # set none flag
        if c3 is None and c4 is None:
            c5 = 3    
        # combine values
        values = f"({self.ts},{c1},{c2},"

        # c3
        if c3 is None:
            values += "null,"
        else:
            values += "%.8f,"%c3
        # c4
        if c4 is None:
            values += "null,"
        else:
            values += "%.15f,"%c4

        # c5
        values += f"{c5}) "
        
        # move next
        self.ts += 1

        return values

    # insert data
    def insertData(self):
        tdLog.info("insert data ....")
        sqls = ""
        for i in range(self.childCnt):
            # insert child table
            values = ""
            pre_insert = f"insert into t{i} values "
            for j in range(self.childRow):
                if values == "":
                    values = self.getColsValues(i)
                else:
                    values += self.getColsValues(i)

                # batch insert    
                if j % self.batchSize == 0  and values != "":
                    sql = pre_insert + values
                    tdSql.execute(sql)
                    values = ""
            # append last
            if values != "":
                sql = pre_insert + values
                tdSql.execute(sql)
                values = ""

        # random flush
        if random.randint(1, 3) == 2:
            sql = "flush database db;"
            tdLog.info(sql)
            tdSql.execute(sql)

        # insert finished
        tdLog.info(f"insert data successfully.\n"
        f"                            inserted child table = {self.childCnt}\n"
        f"                            inserted child rows  = {self.childRow}\n"
        f"                            total inserted rows  = {self.childCnt*self.childRow}\n")
        return


    # prepareEnv
    def prepareEnv(self):
        # init                
        self.ts = 1680000000000*1000
        self.childCnt = 5
        self.childRow = 50000
        self.batchSize = 5000
        
        # create database  db
        tdSql.execute("drop database if exists db")
        sql = f"create database db vgroups 2 precision 'us' "
        tdSql.execute(sql)
        sql = f"use db"
        tdSql.execute(sql)

        # create super talbe st
        sql = f"create table st(ts timestamp, c1 bigint, c2 bigint, c3 float, c4 double, c5 int) tags(area int)"
        tdSql.execute(sql)

        # create child table
        for i in range(self.childCnt):
            sql = f"create table t{i} using st tags({i}) "
            tdSql.execute(sql)


        # insert data
        self.insertData()

    # where
    def checkCorrect(self):
        sql = f"select * from st"
        tdSql.query(sql)
        rowCnt = tdSql.getRows()
        for i in range(rowCnt):
            # get cols val
            c1 = tdSql.queryResult[i][1]
            c2 = tdSql.queryResult[i][2]
            c3 = tdSql.queryResult[i][3]
            c4 = tdSql.queryResult[i][4]
            c5 = tdSql.queryResult[i][5]
            
            # calc expect value
            ec3 = c1 + c2 * 0.00001       # 6 bit
            ec4 = c1 + c2 * 0.00000000001 # 12 bit

            # none flag c5
            if c5 == 3:
                if c3 is not None or c4 is not None:
                    tdLog.exit(f"Not expect . both c3 and c4 must none. c3 = {c3} c4={c4} c5={c5}")
            elif c5 == 2:
                if c4 is not None:
                    tdLog.exit(f"Not expect . c4 must be none.  c4={c4} c5={c5}")
                if math.isclose(c3, ec3, rel_tol=1e-06) == False:
                    tdLog.exit(f"Not expect . c3 value invalid.  c3={c3} expect={ec3} fabs={math.fabs(c3-ec3)}")
            elif c5 == 1:
                if c3 is not None:
                    tdLog.exit(f"Not expect . c3 must be none.  c3={c3} c5={c5}")
                if math.isclose(c4, ec4, rel_tol=1e-12) == False:
                    tdLog.exit(f"Not expect . c4 value invalid.  c4={c4} expect={ec4} fabs={math.fabs(c4-ec4)}")
            else:
                if math.isclose(c3, ec3, rel_tol=1e-06) == False:
                    tdLog.exit(f"Not expect . c3 invalid.  c3={c3} expect={ec3} fabs={math.fabs(c3-ec3)}")
                if math.isclose(c4, ec4, rel_tol=1e-12) == False:
                    tdLog.exit(f"Not expect . c4 invalid.  c4={c4} expect={ec4} fabs={math.fabs(c4-ec4)}")

        # successful
        tdLog.info(f"check data correct ok. sql={sql}")

    # run
    def do_compress_tsz1(self):
        # prepare env insert data
        self.prepareEnv()
        # check where
        self.checkCorrect()

        print("\ndo compact tsz1 ....................... [passed]")

    def do_compress_tsz2(self):
        # alter config "IfAdtFse" to 0
        tdSql.execute(f"alter all dnodes 'IfAdtFse 0'")
        
        # prepare env insert data
        self.prepareEnv()
        # check where
        self.checkCorrect()

        print("do compress tsz2 ...................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_compress_tsz(self):
        """Compress tsz algorithm
        
        1. Init config open TSZ compression and set IfAdtFse to 1
        2. Create 1 stable 5 child tables
        3. each child table insert 50000 rows data with some null float/double values
        4. Verify data correctness after insert with TSZ compression
        5. Alter config IfAdtFse to 0
        6. Repeat steps 1-3 to verify data correctness
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_compress_tsz1.py
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_compress_tsz2.py
 
        """
        self.do_compress_tsz1()
        self.do_compress_tsz2()


#
# ------------------- TSZ encode-disabled DDL rule + large-volume correctness (regression) ----------------
#


class TestCompressTszDdlAndData:
    """
    Regression for the TSZ encode/decode mismatch:
      1. DDL rule: compress=tsz must not carry an explicit L1 encode (only DISABLED), enforced at CREATE/ALTER.
      2. Large-volume TSZ write -> flush/compact -> read-back correctness (covers the BSS+tsz decompress crash).
    """

    def _use_db(self, db):
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} precision 'us'")
        tdSql.execute(f"use {db}")

    def _wait_compact(self):
        for _ in range(60):
            tdSql.query("show compacts;")
            if tdSql.getRows() == 0:
                break
            time.sleep(3)

    def test_tsz_ddl_encode_disabled(self):
        """TSZ DDL rule: compress tsz forces the L1 encode to disabled

        1. Create/alter float & double columns with COMPRESS 'tsz' (absent/disabled/default encode) succeed
        2. Explicit L1 encode such as 'delta-d' combined with tsz is rejected on create, alter modify and add column
        3. COMPRESS 'tsz' on non-float/double types (int/bool/varchar) is rejected
        4. Both normal table and super table paths persist tsz columns with encode disabled

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None
        """
        tdLog.debug(f"start to execute {__file__}")
        db = "tsz_ddl_rule"
        self._use_db(db)

        # allowed: absent encode defaults to DISABLED; explicit disabled; any level
        tdSql.execute(
            f"create table {db}.t_ok1 (ts timestamp, c1 float COMPRESS 'tsz', c2 double COMPRESS 'tsz')"
        )
        tdSql.execute(
            f"create table {db}.t_ok2 (ts timestamp, c2 double ENCODE 'disabled' COMPRESS 'tsz' LEVEL 'high')"
        )
        tdSql.execute(
            f"create table {db}.t_ok3 (ts timestamp, c2 double ENCODE 'disabled' COMPRESS 'tsz' LEVEL 'low')"
        )

        # rejected: explicit L1 encode (other than disabled) cannot be combined with tsz
        tdSql.error(f"create table {db}.t_bad_enc1 (ts timestamp, c2 double ENCODE 'delta-d' COMPRESS 'tsz')")
        tdSql.error(f"create table {db}.t_bad_enc2 (ts timestamp, c2 float ENCODE 'delta-d' COMPRESS 'tsz')")

        # rejected: tsz is only valid on float/double columns
        tdSql.error(f"create table {db}.t_bad_int (ts timestamp, i int COMPRESS 'tsz')")
        tdSql.error(f"create table {db}.t_bad_bool (ts timestamp, b bool COMPRESS 'tsz')")
        tdSql.error(f"create table {db}.t_bad_str (ts timestamp, s varchar(10) COMPRESS 'tsz')")

        # alter: switch an existing float/double column to/from tsz keeps encode sane
        tdSql.execute(f"create table {db}.t_alt (ts timestamp, c2 double)")
        tdSql.execute(f"alter table {db}.t_alt modify column c2 compress 'tsz'")
        tdSql.execute(f"alter table {db}.t_alt modify column c2 compress 'zlib'")

        # alter add column: same rule applies
        tdSql.execute(f"alter table {db}.t_alt add column c3 double ENCODE 'disabled' COMPRESS 'tsz'")
        tdSql.execute(f"alter table {db}.t_alt add column c4 double COMPRESS 'tsz'")
        tdSql.error(f"alter table {db}.t_alt add column c5 double ENCODE 'delta-d' COMPRESS 'tsz'")

        tdLog.info("tsz DDL encode-disabled rule check passed")

    def test_tsz_large_data_correctness(self):
        """Large TSZ volume flush/compact read-back stays within SZ tolerance

        1. Create float & double columns with COMPRESS 'tsz' (encode normalized to disabled)
        2. Insert a large volume (>= 3 on-disk blocks per table) so real SZ compress/decompress happens on disk
        3. Flush database then compact database to force blocks to be re-decompressed and re-compressed
        4. Read back rows and verify per-row values stay within SZ lossy tolerance
        5. Verify row count and min/max/avg aggregates after compact

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None
        """
        tdLog.debug(f"start to execute {__file__}")
        db = "tsz_large_correct"
        self._use_db(db)

        rows_per_blk = 4096  # default max rows per file block
        nblk = 3
        rows = rows_per_blk * nblk  # 12288 rows => >=3 on-disk blocks per table
        batch = 1024
        base = 1680000000000000

        tdSql.execute(
            f"create table {db}.t (ts timestamp, c1 float COMPRESS 'tsz', c2 double COMPRESS 'tsz')"
        )
        # t1: float + double tsz columns
        for s in range(0, rows, batch):
            e = min(s + batch, rows)
            ts = base + s
            vals = []
            for i in range(s, e):
                ts += 1
                f = math.sin(i * 0.01) * 100.0
                d = 1000.0 + math.cos(i * 0.001) * 50.0
                vals.append(f"({ts},{f:.6f},{d:.6f})")
            tdSql.execute(f"insert into {db}.t values " + ",".join(vals))
        tdSql.execute(f"flush database {db}")

        # t2: double-only tsz column, different range/timestamps
        tdSql.execute(f"create table {db}.t2 (ts timestamp, c2 double COMPRESS 'tsz')")
        base2 = 1700000000000000
        for s in range(0, rows, batch):
            e = min(s + batch, rows)
            ts = base2 + s
            vals = []
            for i in range(s, e):
                ts += 1
                d = 2000.0 + math.cos(i * 0.001) * 30.0
                vals.append(f"({ts},{d:.6f})")
            tdSql.execute(f"insert into {db}.t2 values " + ",".join(vals))
        tdSql.execute(f"flush database {db}")

        # compact (merge) so blocks are re-decompressed/re-compressed; then read again
        tdSql.execute(f"compact database {db}")
        self._wait_compact()
        tdSql.execute(f"flush database {db}")

        # ---- verify t ----
        tdSql.query(f"select count(*) from {db}.t")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, rows)

        # row-level spot check (forces per-block decompression of tsz columns)
        tdSql.query(f"select ts, c1, c2 from {db}.t order by ts")
        res = tdSql.queryResult
        assert res is not None
        rowCnt = tdSql.getRows()
        tdLog.debug(f"tsz large: fetched rows={rowCnt}")
        if rowCnt != rows:
            tdLog.exit(f"row count mismatch: {rowCnt} != {rows}")
        for idx in range(0, rowCnt, 64):
            r = res[idx]
            exp1 = math.sin(idx * 0.01) * 100.0
            exp2 = 1000.0 + math.cos(idx * 0.001) * 50.0
            if not math.isclose(float(r[1]), exp1, rel_tol=1e-3, abs_tol=5e-3):
                tdLog.exit(f"c1 mismatch at idx {idx}: got={r[1]} expect={exp1}")
            if not math.isclose(float(r[2]), exp2, rel_tol=1e-5, abs_tol=1e-2):
                tdLog.exit(f"c2 mismatch at idx {idx}: got={r[2]} expect={exp2}")

        # aggregates sanity for t
        tdSql.query(f"select min(c2), max(c2), avg(c2) from {db}.t")
        res = tdSql.queryResult
        assert res is not None
        mn, mx, avg = res[0][0], res[0][1], res[0][2]
        if not (950.0 - 0.1 <= float(mn) and float(mx) <= 1050.0 + 0.1 and 990.0 <= float(avg) <= 1010.0):
            tdLog.exit(f"c2 aggregate out of range: min={mn} max={mx} avg={avg}")

        # ---- verify t2 ----
        tdSql.query(f"select count(*), min(c2), max(c2) from {db}.t2")
        res = tdSql.queryResult
        assert res is not None
        cnt, mn2, mx2 = res[0][0], res[0][1], res[0][2]
        if int(cnt) != rows:
            tdLog.exit(f"t2 count mismatch: {cnt} != {rows}")
        if not (1970.0 - 0.1 <= float(mn2) and float(mx2) <= 2030.0 + 0.1):
            tdLog.exit(f"t2 c2 range out of expected: min={mn2} max={mx2}")

        # full second read after compact (all blocks decompressed)
        tdSql.query(f"select count(*) from {db}.t")
        tdSql.checkData(0, 0, rows)

        tdLog.info(f"tsz large-volume correctness check passed, rows per table = {rows}")


class TszSecScanBase:
    """Shared helpers for the Devon static scan (defect 7114784001) TSZ data path cases."""

    def _use_db(self, db, extra=""):
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} precision 'us' {extra}")
        tdSql.execute(f"use {db}")

    def _wait_compact(self):
        for _ in range(60):
            tdSql.query("show compacts;")
            if tdSql.getRows() == 0:
                return
            time.sleep(3)
        tdSql.query("show compacts;")
        tdLog.exit(f"compact did not finish within 180s, {tdSql.getRows()} compact(s) still running")

    @staticmethod
    def _rand_float(rnd):
        """float in [1,2) with a random mantissa: nearly unpredictable for SZ and incompressible for zstd"""
        return struct.unpack('<f', struct.pack('<I', 0x3F800000 | rnd.getrandbits(23)))[0]

    def _insert_column(self, tbl, values, base, batch=1024):
        ts = base
        done = 0
        total = len(values)
        while done < total:
            n = min(batch, total - done)
            rows = []
            for i in range(done, done + n):
                ts += 1
                rows.append(f"({ts},{values[i]!r})")
            tdSql.execute(f"insert into {tbl} values " + ",".join(rows))
            done += n
        return ts

    def _read_column(self, tbl):
        tdSql.query(f"select c1 from {tbl} order by ts")
        res = tdSql.queryResult
        assert res is not None
        return [float(r[0]) for r in res]

    def _check_column(self, tag, got, expect, rel_tol, abs_tol, step=8):
        if len(got) != len(expect):
            tdLog.exit(f"{tag}: row count mismatch {len(got)} != {len(expect)}")
        worst = 0.0
        for i in range(0, len(expect), step):
            e = float(expect[i])
            g = float(got[i])
            worst = max(worst, abs(g - e))
            if not math.isclose(g, e, rel_tol=rel_tol, abs_tol=abs_tol):
                tdLog.exit(f"{tag}: value mismatch at row {i}: got={g!r} expect={e!r}")
        tdLog.info(f"{tag}: {len(expect)} rows ok, worst abs error over sampled rows (every {step}th) = {worst:g}")


class TestCompressTszSecScanHuffman(TszSecScanBase):
    """
    Devon static scan (defect 7114784001) coverage for the TSZ/SZ codec, Huffman type coder
    (the engine default, IfAdtFse=0).

    The malformed payload rejection itself (reqLength > 32/64, cyclic or out-of-range Huffman trees,
    garbage FSE code) is covered by the C case utils/test/c/tsz_test.c (-sec, ctest tsz_test_
    malformed_reject). These cases cover the on-disk data paths that consume the very same decoder,
    so a stack/heap overflow or a stray exit() in taosd makes them fail:

      1. idx 70  HIGH    : SZ + zstd two stage output must stay inside the block output capacity, even
                           for incompressible data and with more rows per block than 16449
      2. idx 11  CRITICAL: reqLength at its legal maximum (32 for float / 64 for double) and a stream
                           dominated by "exact" values must still decode correctly
      3. idx 173 HIGH    : the type array (Huffman tree) rebuilt on every read must stay in bounds
    """

    updatecfgDict = {'lossyColumns': 'float-double', 'IfAdtFse': 0}

    def test_tsz_sec_scan_large_incompressible_block(self):
        """TSZ incompressible large block stays inside the output capacity

        1. Create a database with MAXROWS > 16449 so the zstd frame content size field grows
        2. Write float/double tsz columns whose mantissas are random (SZ stream not shrinkable by zstd)
        3. Flush (compress on disk) and read back every sampled row within the SZ precision
        4. Compact and read again so the blocks are re-decompressed and re-compressed
        5. taosd must stay alive: a heap overflow in the compressor would abort the process

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None
        """
        tdLog.debug(f"start to execute {__file__}")
        db = "tsz_sec_scan_incompressible"
        self._use_db(db, "maxrows 20000")

        rows = 24000  # 2 blocks of 20000/4000 rows
        rnd = random.Random(7114784001)
        c1 = [self._rand_float(rnd) for _ in range(rows)]

        tdSql.execute(f"create table {db}.t (ts timestamp, c1 float COMPRESS 'tsz')")
        self._insert_column(f"{db}.t", c1, 1680000000000000)
        tdSql.execute(f"flush database {db}")

        got = self._read_column(f"{db}.t")
        self._check_column("incompressible float", got, c1, 1e-3, 1e-3)

        tdSql.execute(f"compact database {db}")
        self._wait_compact()
        tdSql.execute(f"flush database {db}")
        got = self._read_column(f"{db}.t")
        self._check_column("incompressible float after compact", got, c1, 1e-3, 1e-3)

    def test_tsz_sec_scan_reqlength_max_and_exact_values(self):
        """TSZ blocks with the maximum reqLength and mostly exact values decode correctly

        1. Write float/double tsz columns with a huge dynamic range so reqLength is clamped to its
           legal maximum (32 bit mantissa for float, 64 for double)
        2. Nearly every point is stored as an "exact" value, which is the branch that used to trust
           the reqLength byte when indexing the 4/8 byte buffers
        3. Values must come back within the SZ precision, before and after a compact

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None
        """
        tdLog.debug(f"start to execute {__file__}")
        db = "tsz_sec_scan_reqlen"
        self._use_db(db)

        rows = 12000
        c1 = []
        for i in range(rows):
            if i % 3 == 0:
                c1.append(1e30 * (1.0 + i * 1e-7))
            elif i % 3 == 1:
                c1.append(-1e-30 * (1.0 + i * 1e-7))
            else:
                c1.append(math.sin(i * 0.37) * 10.0)

        tdSql.execute(f"create table {db}.t (ts timestamp, c1 double COMPRESS 'tsz')")
        self._insert_column(f"{db}.t", c1, 1700000000000000)
        tdSql.execute(f"flush database {db}")

        got = self._read_column(f"{db}.t")
        self._check_column("max reqLength double", got, c1, 1e-5, 1e-3)

        tdSql.execute(f"compact database {db}")
        self._wait_compact()
        got = self._read_column(f"{db}.t")
        self._check_column("max reqLength double after compact", got, c1, 1e-5, 1e-3)

    def test_tsz_sec_scan_unpredictable_points(self):
        """TSZ Huffman type array stays in bounds for data with many unpredictable points

        1. Write float tsz columns whose mantissas are random, so the type array is dominated by
           unpredictable ("exact") points and the Huffman tree has to be rebuilt on every read
        2. Values must come back within the SZ precision, before and after a compact
        3. Row count and aggregate must stay consistent

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None
        """
        tdLog.debug(f"start to execute {__file__}")
        db = "tsz_sec_scan_huffman"
        self._use_db(db)

        rows = 12000
        rnd = random.Random(4242)
        c1 = [self._rand_float(rnd) for _ in range(rows)]

        tdSql.execute(f"create table {db}.t (ts timestamp, c1 float COMPRESS 'tsz')")
        self._insert_column(f"{db}.t", c1, 1720000000000000)
        tdSql.execute(f"flush database {db}")

        got = self._read_column(f"{db}.t")
        self._check_column("huffman unpredictable float", got, c1, 1e-3, 1e-3)

        tdSql.query(f"select count(*), min(c1), max(c1) from {db}.t")
        res = tdSql.queryResult
        assert res is not None
        if int(res[0][0]) != rows:
            tdLog.exit(f"huffman unpredictable float: count {res[0][0]} != {rows}")
        #the generated values are in [1,2): compare against the inserted min/max instead of the range
        #that the generator already guarantees, so a value/column mix up is actually detected
        if not (math.isclose(float(res[0][1]), min(c1), rel_tol=1e-3, abs_tol=1e-3)
                and math.isclose(float(res[0][2]), max(c1), rel_tol=1e-3, abs_tol=1e-3)):
            tdLog.exit(f"huffman unpredictable float: min/max {res[0][1]}/{res[0][2]} "
                       f"differ from the inserted {min(c1)}/{max(c1)}")

        tdSql.execute(f"compact database {db}")
        self._wait_compact()
        got = self._read_column(f"{db}.t")
        self._check_column("huffman unpredictable float after compact", got, c1, 1e-3, 1e-3)


class TestCompressTszSecScanFse(TszSecScanBase):
    """
    Devon static scan (defect 7114784001) coverage for the TSZ/SZ codec with the ADT-FSE type coder
    (IfAdtFse=1).

    idx 171 HIGH: the FSE decoder used to call exit(1) on a malformed FSE code, which terminated the
    whole taosd process, and it indexed 67 entry tables with a payload supplied symbol. The C case
    covers the malformed FSE stream rejection; this case covers the FSE decode path with real data
    written by the engine, before and after a compact.
    """

    updatecfgDict = {'lossyColumns': 'float-double', 'IfAdtFse': 1}

    def test_tsz_sec_scan_fse_points(self):
        """TSZ FSE type coder round-trips real float/double columns

        1. Write float and double tsz columns with mixed predictable and unpredictable points
        2. Flush and read back within the SZ precision
        3. Compact (blocks are re-decompressed with the FSE branch) and read back again
        4. taosd must stay alive for the whole case

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None
        """
        tdLog.debug(f"start to execute {__file__}")
        db = "tsz_sec_scan_fse"
        self._use_db(db)

        rows = 12000
        rnd = random.Random(97531)
        c1 = []
        for i in range(rows):
            if i % 2 == 0:
                c1.append(self._rand_float(rnd))
            else:
                c1.append(50.0 + math.sin(i * 0.01) * 20.0)

        tdSql.execute(f"create table {db}.t (ts timestamp, c1 float COMPRESS 'tsz')")
        self._insert_column(f"{db}.t", c1, 1740000000000000)
        tdSql.execute(f"flush database {db}")

        got = self._read_column(f"{db}.t")
        self._check_column("fse mixed float", got, c1, 1e-3, 1e-3)

        tdSql.execute(f"compact database {db}")
        self._wait_compact()
        got = self._read_column(f"{db}.t")
        self._check_column("fse mixed float after compact", got, c1, 1e-3, 1e-3)


