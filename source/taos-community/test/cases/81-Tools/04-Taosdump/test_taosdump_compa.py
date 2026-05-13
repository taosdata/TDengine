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

from new_test_framework.utils import tdLog, tdSql, etool
import os

class TestTaosdumpCompa:
    def caseDescription(self):
        """
        test taosdump compatible with import data coming from v3.1.0.0
        """

    def exec(self, command):
        tdLog.info(command)
        return os.system(command)

    def findPrograme(self):
        # taosdump 
        taosdump = etool.taosDumpFile()
        if taosdump == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % taosdump)

        # tmp dir
        tmpdir = "./tmp"
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s/*" % tmpdir)

        return taosdump, tmpdir


    def dumpIn(self, taosdump, indir):
        # dump in
        self.exec(f'{taosdump} -i {indir}')

    def check_same(self, db, stb, aggfun, expect):
        # sum pk db
        sql = f"select {aggfun} from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkData(0, 0, expect, show=True)

    def verifyResult(self, db):
        
        #  compare sum(pk)
        stb = "meters"
        self.check_same(db, stb, "count(ts)", 5000)
        self.check_same(db, stb, "last(ts)", "2023-11-15 07:36:39")
        self.check_same(db, stb, "last(bc)", False)
        self.check_same(db, stb, "sum(fc)", 2468.910999777726829)
        self.check_same(db, stb, "sum(dc)", 24811.172123999996984)
        self.check_same(db, stb, "sum(ti)", -411)
        self.check_same(db, stb, "sum(si)", 117073)
        self.check_same(db, stb, "sum(ic)", -39181)
        self.check_same(db, stb, "sum(bi)", -2231976)
        self.check_same(db, stb, "sum(uti)", 248825)
        self.check_same(db, stb, "sum(usi)", 248333)
        self.check_same(db, stb, "sum(ui)", 2484501)
        self.check_same(db, stb, "sum(ubi)", 25051956)
        self.check_same(db, stb, "last(bin)", "kwax")
        self.check_same(db, stb, "last(nch)", "0cYzPVcV")

        self.check_same(db, stb, "sum(tfc)", 3420.000076293945312)
        self.check_same(db, stb, "sum(tdc)", 3020.234999999780030)
        self.check_same(db, stb, "sum(tti)", -100000)
        self.check_same(db, stb, "sum(tsi)", -85000)
        self.check_same(db, stb, "sum(tic)", -4795000)
        self.check_same(db, stb, "sum(tbi)", -1125000)
        self.check_same(db, stb, "sum(tuti)", 475000)
        self.check_same(db, stb, "sum(tusi)", 460000)
        self.check_same(db, stb, "sum(tui)", 520000)
        self.check_same(db, stb, "sum(tubi)", 43155000)
        self.check_same(db, stb, "last(tbin)", "ywkc")
        self.check_same(db, stb, "last(tnch)", "kEoWzCBj")

    def test_taosdump_compa(self):
        """taosdump compatible 

        1. Backup data come from v3.1.0.0
        2. Restore data with taosdump
        3. Verify data correctness
            - compare sum value for numeric type 
            - compare last value for string/boolean type
            

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_compa.py

        """
        # database
        db = "test"
        
        # find
        taosdump, tmpdir = self.findPrograme()
        data = f"{os.path.dirname(os.path.abspath(__file__))}/compa"

        # dump in
        self.dumpIn(taosdump, data)

        # verify db
        self.verifyResult(db)


