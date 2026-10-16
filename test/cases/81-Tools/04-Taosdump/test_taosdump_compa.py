# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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
        taosdump = etool.taosOldDumpFile()
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

        Labels: common,ci,integration,functional,compatibility
        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_compa.py

        """
        # database
        db = "test"
        
        newTaosdump = etool.taosDumpFile()
        data = f"{os.path.dirname(os.path.abspath(__file__))}/compa"

        # import and verify with taosBackup (pre-generated AVRO backup from v3.1.0.0)
        for tool_name, tool in [("taosBackup", newTaosdump)]:
            tdSql.execute(f"drop database if exists {db}")
            self.dumpIn(tool, data)
            self.verifyResult(db)


