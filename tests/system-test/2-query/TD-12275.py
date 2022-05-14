###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from posixpath import split
import sys
import os 

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import subprocess

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath


    def caseDescription(self):

        '''
        case1 <wenzhouwww>: [TD-12275] : 
            this test case is an long query crash for elapsed function  .
        ''' 
        return 
   
    def run(self):
        tdSql.prepare()
        build_path = self.getBuildPath()+"/build/bin/"
        prepare_cmd = "%staosBenchmark -t 100 -n 100000 -S 10000 -y " % (build_path)

        # only taos -s for shell can generate this issue 
        print(prepare_cmd)
        _ = subprocess.check_output(prepare_cmd, shell=True).decode("utf-8")
        cmd1 = "taos -s 'select elapsed(ts) from test.meters interval(10s) sliding(5s) group by tbname' "
        print(cmd1)
        _ = subprocess.check_output(cmd1, shell=True).decode("utf-8")
        
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
