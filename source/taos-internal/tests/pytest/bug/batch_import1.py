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
		tdDnodes.start(1)
		
		self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
		tdSql.init(self.conn.cursor())
		
	def run(self):
		tdLog.info("================= step1")	
		threadNum = 10
		insertRows = 200000
		replica = 1
		cmd = "c/batch_import %d %d %d" % (threadNum, insertRows, replica)
		if os.system(cmd) != 0 :
			tdLog.exit("failed to run %s" % (cmd))
		
		tdLog.info("================= step2")
		for t in range (threadNum) : 
			sql = 'select count(*) from db.t%d' % (t)
			tdSql.query(sql)
			tdSql.checkRows(1)
			queryRows = tdSql.getData(0, 0)
			expect = insertRows * 0.9
			if queryRows < expect:
				tdLog.exit("sql:%s, queryRows:%d < expect:%d" % (sql, queryRows, expect))
			
		tdLog.info("================= step3")
		sql = 'select count(*) from db.mt'
		tdSql.query(sql)
		queryRows = tdSql.getData(0, 0)
		expect = insertRows * threadNum * 0.9
		if queryRows < expect:
			tdLog.exit("sql:%s, queryRows:%d < expect:%d" % (sql, queryRows, expect))
				
	def stop(self):
		tdSql.close()
		self.conn.close()
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addCluster(__file__, TDTestCase())