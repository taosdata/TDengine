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
import sys
sys.path.insert(0, os.getcwd())
from util.log import *
from util.sql import *
from util.dnodes import *
import taos
import threading

 
class TwoClients:
    def initConnection(self):
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/home/chr/taosdata/TDengine/sim/dnode1/cfg "        
    
    def newCloseCon(times):
        newConList = []
        for times in range(0,times) :
            newConList.append(taos.connect(self.host, self.user, self.password, self.config))
        for times in range(0,times) :
            newConList[times].close()        

    def run(self):
        tdDnodes.init("")
        tdDnodes.setTestCluster(False)
        tdDnodes.setValgrind(False) 

        tdDnodes.stopAll()
        tdDnodes.deploy(1)
        tdDnodes.start(1)
        
        # multiple new and cloes connection
        for m in range(1,101) :
            t= threading.Thread(target=newCloseCon,args=(10,))
            t.start()

        
clients = TwoClients()
clients.initConnection()
clients.run()