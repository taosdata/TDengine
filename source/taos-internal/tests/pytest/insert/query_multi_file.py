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

class TDTestCase:
	def init(self, conn):
		tdLog.debug("start to execute %s" % __file__)
		self.conn = conn
		self.cursor = self.conn.cursor()
		
	def run(self):
		tdLog.info("prepare database")	
		self.cursor.execute('reset query cache')
		self.cursor.execute('drop database db')
		self.cursor.execute('create database db')
		self.cursor.execute('use db')
		self.cursor.execute('create table tb (ts timestamp, speed int)')

		insertRows = 20000
		tdLog.info("insert %d rows" % (insertRows))	
		for i in range(0, insertRows):
			self.cursor.execute('insert into tb values (now + %ds, %d)' % (i, i))
		
		self.cursor.execute('select * from tb')
		data = self.cursor.fetchall()
		queryRows = len(data)
		tdLog.info("query data, rows:%d" % (insertRows))		
		if queryRows != insertRows:
			tdLog.exit("queryRows:%d != insertRows:%d" % (queryRows, insertRows))
		
	def stop(self):
		self.cursor.close()
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())