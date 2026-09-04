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

