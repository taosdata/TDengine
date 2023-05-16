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


from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
from util.dnodes import *

import random
import os
import subprocess
    

class TDTestCase:
    # init
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())


    #     


    # run
    def run(self):
        # get from global 
        tdDnodes =  tdDnodes_Get()
        dnodes = tdDnodes.dnodes
        num = len(dnodes)
        print(f" start dnode num={num} !")
        for i in range(num):
            dnode = dnodes[i]
            print(f"  dnode{i} deploay={dnode.deployed} ip={dnode.remoteIP} path={dnode.path}")

        print(" end !")

    # stop
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
