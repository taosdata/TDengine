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

from basic import *

class TDTestCase:

    def init(self):
        # tdLog.debug("start to execute %s" % __file__)

        self.numOfNodes = 5
        self.dockerDir = "/data"
        cluster.init(self.numOfNodes, self.dockerDir)        
        cluster.prepardBuild()
        for i in range(self.numOfNodes):
            if i == 0:
                cluster.cfg("role", "1", i + 1)
            else:
                cluster.cfg("role", "2", i + 1)
        cluster.run()

td = TDTestCase()
td.init()


## usage:  python3 OneMnodeMultipleVnodesTest.py 


