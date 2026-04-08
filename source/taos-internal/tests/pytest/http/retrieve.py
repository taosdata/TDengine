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
		tdSql.prepare()
		tdSql.execute('create table if not exists db.st (ts timestamp, speed int) tags(i int)')

		tdLog.info("================= step1")

		tables = 100
		queryTimes = 100
		tdLog.info("create %d tables" % (tables))
		for i in range(0, tables):
			tdSql.execute("create table if not exists db.t%d using db.st tags(%d)" % (i, i))

		rowsPerBatch = 1000
		insertBatchs = 10
		tdLog.info("insert %d batchs(%d rows per batch) per table" % (insertBatchs, rowsPerBatch))	
		tdSql.execute("alter dnode 192.168.0.1 debugflag 131")
		for t in range (0, tables):
			tdLog.info("insert table:db.t%d" % (t))
			startTs = 1538408992959
			for b in range (0, insertBatchs):
				sql = "insert into db.t%d values " % (t)
				batchTs = startTs + b * 1000
				for r in range(0, rowsPerBatch):
					rowsTs = batchTs + r
					sql += "(%d,%d)" % (rowsTs, t)
				# tdLog.exit(sql)
				tdSql.execute(sql)
		tdSql.execute("alter dnode 192.168.0.1 debugflag 135")
		
		tdLog.info("================= step2")	
		tdLog.info("query %d tables" % (tables))	
		insertRows = insertBatchs * rowsPerBatch
		for t in range (0, tables):
			sql = 'select count(*) from db.t%d' % (t)
			tdSql.query(sql)
			tdSql.checkRows(1)
			tdSql.checkData(0, 0, insertRows)

		tdLog.info("================= step3")
		for loop in range (queryTimes) :
			tdLog.info("retrieve table loop:%d" % (loop))
			cmd = "curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d '%s' 127.0.0.1:6020/rest/sql > /dev/null 2>&1" % ('select * from db.st')
			if os.system(cmd) != 0 :
				tdLog.exit(cmd)

	def stop(self):
		tdSql.close()
		tdLog.success("%s successfully executed" % __file__)

tdCases.addCluster(__file__, TDTestCase())
