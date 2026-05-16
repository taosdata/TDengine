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
		tdSql.execute('create dnode 192.168.0.2')
		tdDnodes.deploy(2)
		tdDnodes.start(2)

	def run(self):
	
		self.tablesPerVnodes = 1000
		self.replica = 1
		self.sleep = 10
		
		tdLog.info("================= step0")	
		tdSql.execute('create database if not exists db replica %d days 5 keep 3650 rows 1000 cache 200 ablocks 1.100000 tblocks 2 tables %d precision us' % (self.replica, self.tablesPerVnodes))
		tdSql.execute('use db')
		tdSql.execute('create table st (ts timestamp, i int, j float, k double) tags(a int)')
		
		tdLog.info("================= step1")	
		for i in range(0, self.tablesPerVnodes * 2):
			tdSql.execute("create table if not exists db.t%d using db.st tags(%d)" % (i, i))
			if i == self.tablesPerVnodes:
				tdLog.info("drop table db.t%d" % (i - self.tablesPerVnodes))
				tdSql.execute("drop table db.t%d" % (i - self.tablesPerVnodes))
				tdDnodes.stop(2)
		
		tdLog.info("================= step2")
		sql = "show db.tables"
		tdSql.query(sql)
		tdSql.checkRows(self.tablesPerVnodes * 2 - 1)
		
		tdLog.info("================= step3")	
		tdDnodes.start(2)
		tdLog.sleep(self.sleep)
		for i in range(self.tablesPerVnodes * 2, self.tablesPerVnodes * 4):
			tdSql.execute("create table if not exists db.t%d using db.st tags(%d)" % (i, i))
			if i == self.tablesPerVnodes * 3:
				tdLog.info("drop table db.t%d" % (i - self.tablesPerVnodes))
				tdSql.execute("drop table db.t%d" % (i - self.tablesPerVnodes))
				tdDnodes.stop(1)
				tdLog.sleep(self.sleep)
		
		tdLog.info("================= step4")
		sql = "show db.tables"
		tdSql.query(sql)
		tdSql.checkRows(self.tablesPerVnodes * 4 - 2)
		
		tdLog.info("================= step5")
		tdDnodes.start(1)
		tdLog.sleep(self.sleep)
		sql = "show db.tables"
		tdSql.query(sql)
		tdSql.checkRows(self.tablesPerVnodes * 4 - 2)
		
		tdLog.info("================= step6")
		timestamp = 1530374400000
		for i in range (self.tablesPerVnodes * 4) :
			val = i
			if i == 0 or i == self.tablesPerVnodes * 2 : 
				continue;
			sql = "insert into db.t%d values(%d, %d, %d, %d)" % (i, timestamp, val, val, val)
			tdSql.execute(sql)
		
		tdLog.info("================= step7")
		sql = "select * from db.st"
		tdSql.query(sql)
		tdSql.checkRows(self.tablesPerVnodes * 4 - 2)
							
	def stop(self):
		tdSql.close()
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addCluster(__file__, TDTestCase())