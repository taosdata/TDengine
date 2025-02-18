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
        value = tdSql.getData(0,0)

        if value == expect:
            tdLog.info(f"{aggfun} not equal. real={value} expect={expect}")
        else:
            tdLog.info(f"{aggfun} equal. real={value} expect={expect}")


    def verifyResult(self, db):
        
        #  compare sum(pk)
        stb = "meters"
        self.checkSame(db, stb, "count(ts)", 2000)
        self.checkSame(db, stb, "sum(current)", 20241.627464294433594)
        self.checkSame(db, stb, "avg(voltage)", 209.538000000000011)

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
