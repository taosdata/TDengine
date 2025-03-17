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

import os
import json
import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
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

    def checkSame(self, db, stb, aggfun, expect):
        # sum pk db
        sql = f"select {aggfun} from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkData(0, 0, expect, show=True)

    def verifyResult(self, db):
        
        #  compare sum(pk)
        stb = "meters"
        self.checkSame(db, stb, "count(ts)", 5000)
        self.checkSame(db, stb, "last(ts)", "2023-11-15 07:36:39")
        self.checkSame(db, stb, "last(bc)", False)
        self.checkSame(db, stb, "sum(fc)", 2468.910999777726829)
        self.checkSame(db, stb, "sum(dc)", 24811.172123999996984)
        self.checkSame(db, stb, "sum(ti)", -411)
        self.checkSame(db, stb, "sum(si)", 117073)
        self.checkSame(db, stb, "sum(ic)", -39181)
        self.checkSame(db, stb, "sum(bi)", -2231976)
        self.checkSame(db, stb, "sum(uti)", 248825)
        self.checkSame(db, stb, "sum(usi)", 248333)
        self.checkSame(db, stb, "sum(ui)", 2484501)
        self.checkSame(db, stb, "sum(ubi)", 25051956)
        self.checkSame(db, stb, "last(bin)", "kwax")
        self.checkSame(db, stb, "last(nch)", "0cYzPVcV")

        self.checkSame(db, stb, "sum(tfc)", 3420.000076293945312)
        self.checkSame(db, stb, "sum(tdc)", 3020.234999999780030)
        self.checkSame(db, stb, "sum(tti)", -100000)
        self.checkSame(db, stb, "sum(tsi)", -85000)
        self.checkSame(db, stb, "sum(tic)", -4795000)
        self.checkSame(db, stb, "sum(tbi)", -1125000)
        self.checkSame(db, stb, "sum(tuti)", 475000)
        self.checkSame(db, stb, "sum(tusi)", 460000)
        self.checkSame(db, stb, "sum(tui)", 520000)
        self.checkSame(db, stb, "sum(tubi)", 43155000)
        self.checkSame(db, stb, "last(tbin)", "ywkc")
        self.checkSame(db, stb, "last(tnch)", "kEoWzCBj")

    def run(self):
        # database
        db = "test"
        
        # find
        taosdump, tmpdir = self.findPrograme()
        data = "./tools/taosdump/native/compa"

        # dump in
        self.dumpIn(taosdump, data)

        # verify db
        self.verifyResult(db)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
