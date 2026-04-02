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
import taos
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

class TDTestCase:
	def init(self):
		tdLog.debug("start to execute %s" % __file__)
		tdLog.info("prepare cluster")	
		tdDnodes.stopAll()
		tdDnodes.deploy(1)
		tdDnodes.cfg(1, "maxVnodeConnections", "300")
		tdDnodes.cfg(1, "maxMgmtConnections", "300")
		tdDnodes.cfg(1, "maxMeterConnections", "300")
		tdDnodes.cfg(1, "maxShellConns", "300")
		tdDnodes.start(1)
		
	def run(self):
		connNum = 100
		repeatNum = 20
		for r in range (repeatNum) :
			tdLog.info("repeat:%d of %d" % (r, repeatNum))	
			connList = []
			for c in range (connNum) :
				connList.append(taos.connect(config=tdDnodes.getSimCfgPath()))
			tdLog.sleep(3)
			for c in range (connNum) :
				connList[c].close()	
				
	def stop(self):
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addCluster(__file__, TDTestCase())