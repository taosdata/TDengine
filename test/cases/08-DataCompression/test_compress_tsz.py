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

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_compress_tsz1.py
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_compress_tsz2.py
 
        """
        self.do_compress_tsz1()
        self.do_compress_tsz2()
