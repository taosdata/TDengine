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
import threading
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
		tdSql.execute('reset query cache')
		
	def Loop(self):
		# tdLog.debug('thread %s is running...' % threading.current_thread().name)
		threadIndex = threading.current_thread().name
		queryTimes = 20
		expectRows = self.insertBatchs * self.rowsPerBatch * self.tables
		sql = "select * from db.st"
		
		conn = taos.connect(config=tdDnodes.getSimCfgPath())
		cursor = conn.cursor()
		for q in range (queryTimes) :
			if threadIndex == "0":
				tdLog.info("thread:%s run query:%d of %d" % (threadIndex, q, queryTimes))
			cursor.execute(sql)
			queryResult = cursor.fetchall()
			queryRows = len(queryResult)
			#tdLog.debug(queryRows)
			if queryRows != expectRows:
				tdLog.exit("sql:%s, queryRows:%d != expect:%d" % (sql, queryRows, expectRows))
				break
		
		cursor.close()
		conn.close()
		# tdLog.debug('thread %s ended.' % threading.current_thread().name)

	def run(self):
	
		self.tables = 10
		self.insertBatchs = 10
		startTs = 1530374400000L;
		self.rowsPerBatch = 1000;
		
		tdLog.info("================= step1")	
		tdSql.execute('create database db')
		tdSql.execute('use db')
		tdSql.execute('create table st (ts timestamp, value double) tags(a int, b binary(60))')
		for i in range(0, self.tables):
			tdSql.execute("create table if not exists db.t%d using db.st tags(%d, '%d')" % (i, i, i))
		
		tdLog.info("insert %d batchs(%d rows per batch) per table" % (self.insertBatchs, self.rowsPerBatch))	
		for t in range (0, self.tables):
			tdLog.info("insert table:db.t%d" % (t))	
			for b in range (0, self.insertBatchs):
				sql = "insert into db.t%d values " % (t)
				for r in range(0, self.rowsPerBatch):
					val = b * self.rowsPerBatch + r
					rowsTs = startTs + 60000 * val
					sql += "(%d,%d)" % (rowsTs, val)	
				tdSql.execute(sql)
		
		tdLog.info("================= step2")	
		sql = "select * from db.st"
		insertRows = self.insertBatchs * self.rowsPerBatch * self.tables
		tdSql.query(sql)
		tdSql.checkRows(insertRows)
		
		tdLog.info("================= step3")
		threadNum = 10
		threads = []
		for t in range (threadNum) :
			threadName = "%d" % (t)
			thread = threading.Thread(target=self.Loop, name=threadName)
			thread.start()
			threads.append(thread)
		
		for t in range (threadNum) :
			threads[t].join()
							
	def stop(self):
		tdSql.close()
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addCluster(__file__, TDTestCase())
