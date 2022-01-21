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

import sys
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import subprocess


class TDTestCase:
    def caseDescription(self):
        '''
        case1<pxiao>: [TD-11977] start taosdump without taosd   
        case1<pxiao>: [TD-11977] start taosBenchmark without taosd
        case1<pxiao>: [TD-11977] start taosAdaptor without taosd
        ''' 
        return
    
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()        
        
        tools = ["taosdump", "taosBenchmark", "taosAdaptor"]                
        tdDnodes.stop(1)

        for tool in tools:            
            path = tdDnodes.dnodes[1].getBuildPath(tool)

            try:
                path += "/build/bin/" 
                print(f"{path}{tool}")
                if tool == "taosBenchmark":
                    os.system(f"{path}{tool} -y")            
                else:
                    os.system(f"{path}{tool}")
            except:
                pass

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
