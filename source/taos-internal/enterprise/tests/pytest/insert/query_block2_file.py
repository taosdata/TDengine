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

		N = 82
		
		tdLog.info("=============== step 1")	
		x = N * 2
		y = N 
		while x > y :
			self.cursor.execute('insert into tb values (now - %dm, -%d)' % (x, x))
			x -= 1
		
		self.cursor.execute('select * from tb')
		data = self.cursor.fetchall()
		expect = N
		queryRows = len(data)
		tdLog.info("query data, rows:%d" % (queryRows))		
		if queryRows != expect:
			tdLog.exit("queryRows:%d != expect:%d" % (queryRows, expect))
			
		x = N 
		y = N * 2
		while x < y :
			self.cursor.execute('insert into tb values (now + %dm, %d)' % (x, x))
			x += 1
			
		self.cursor.execute('select * from tb')
		data = self.cursor.fetchall()
		expect = N * 2
		queryRows = len(data)
		tdLog.info("query data, rows:%d" % (queryRows))		
		if queryRows != expect:
			tdLog.exit("queryRows:%d != expect:%d" % (queryRows, expect))
		
		tdLog.info("=============== step 2")	
		R = 4
		y = N * R
		expect = y + N
		expect = expect + N
		x = N * 3
		y = y + x 
		while x < y:
			self.cursor.execute('insert into tb values (now + %dm, %d)' % (x, x))
			x += 1
		
		self.cursor.execute('select * from tb')
		data = self.cursor.fetchall()
		queryRows = len(data)
		tdLog.info("query data, rows:%d" % (queryRows))		
		if queryRows != expect:
			tdLog.exit("queryRows:%d != expect:%d" % (queryRows, expect))
			
		tdLog.info("=============== step 3")		
		N2 = N
		result1 = N
		result2 = N * 2
		N1 = result2 + 1
		step = "%dm" % (N1)
		start1 = "now-" + step
		start2 = "now"
		start3 = "now+" + step
		end1 = "now-" + step
		end2 = "now"
		end3 = "now+" + step
		
		tdLog.info("=============== step 3.1")		
		sql = 'select * from tb where ts < %s and ts > %s' % (start1, end1)
		expectErrNotOccured = True
		try:
			self.cursor.execute(sql)
		except:
			expectErrNotOccured = False
		if expectErrNotOccured :
			tdLog.debug("expect error: %s" % (sql))
			
		sql = 'select * from tb where ts < %s and ts > %s' % (start1, end2)
		expectErrNotOccured = True
		try:
			self.cursor.execute(sql)
		except:
			expectErrNotOccured = False
		if expectErrNotOccured :
			tdLog.debug("expect error: %s" % (sql))
		
		sql = 'select * from tb where ts < %s and ts > %s' % (start1, end3)
		expectErrNotOccured = True
		try:
			self.cursor.execute(sql)
		except:
			expectErrNotOccured = False
		if expectErrNotOccured :
			tdLog.debug("expect error: %s" % (sql))
			
		tdLog.info("=============== step 3.2")					
		sql = 'select * from tb where ts < %s and ts > %s' % (start2, end1)
		self.cursor.execute(sql)
		data = self.cursor.fetchall()
		expect = result1
		queryRows = len(data)
		tdLog.info("query data, rows:%d" % (queryRows))		
		if queryRows != expect:
			tdLog.exit("queryRows:%d != expect:%d" % (queryRows, expect))
			
		sql = 'select * from tb where ts < %s and ts > %s' % (start2, end2)
		expectErrNotOccured = True
		try:
			self.cursor.execute(sql)
		except:
			expectErrNotOccured = False
		if expectErrNotOccured :
			tdLog.debug("expect error: %s" % (sql))
		
		sql = 'select * from tb where ts < %s and ts > %s' % (start2, end3)
		expectErrNotOccured = True
		try:
			self.cursor.execute(sql)
		except:
			expectErrNotOccured = False
		if expectErrNotOccured :
			tdLog.debug("expect error: %s" % (sql))
			
		tdLog.info("=============== step 3.3")		
		sql = 'select * from tb where ts < %s and ts > %s' % (start3, end1)
		self.cursor.execute(sql)
		data = self.cursor.fetchall()
		expect = result2
		queryRows = len(data)
		tdLog.info("query data, rows:%d" % (queryRows))		
		if queryRows != expect:
			tdLog.exit("queryRows:%d != expect:%d" % (queryRows, expect))
			
		sql = 'select * from tb where ts < %s and ts > %s' % (start3, end2)
		self.cursor.execute(sql)
		data = self.cursor.fetchall()
		expect = result1
		queryRows = len(data)
		tdLog.info("query data, rows:%d" % (queryRows))		
		if queryRows != expect:
			tdLog.exit("queryRows:%d != expect:%d" % (queryRows, expect))
		
		sql = 'select * from tb where ts < %s and ts > %s' % (start3, end3)
		expectErrNotOccured = True
		try:
			self.cursor.execute(sql)
		except:
			expectErrNotOccured = False
		if expectErrNotOccured :
			tdLog.debug("expect error: %s" % (sql))		
		
		tdLog.info("=============== step 4")	
		tdLog.info("=============== step 4.1")		
		sql = 'select * from tb where ts < %s and ts > %s order by ts desc' % (start1, end1)
		expectErrNotOccured = True
		try:
			self.cursor.execute(sql)
		except:
			expectErrNotOccured = False
		if expectErrNotOccured :
			tdLog.debug("expect error: %s" % (sql))
			
		sql = 'select * from tb where ts < %s and ts > %s order by ts desc' % (start1, end2)
		expectErrNotOccured = True
		try:
			self.cursor.execute(sql)
		except:
			expectErrNotOccured = False
		if expectErrNotOccured :
			tdLog.debug("expect error: %s" % (sql))
		
		sql = 'select * from tb where ts < %s and ts > %s order by ts desc' % (start1, end3)
		expectErrNotOccured = True
		try:
			self.cursor.execute(sql)
		except:
			expectErrNotOccured = False
		if expectErrNotOccured :
			tdLog.debug("expect error: %s" % (sql))
			
		tdLog.info("=============== step 4.2")					
		sql = 'select * from tb where ts < %s and ts > %s order by ts desc' % (start2, end1)
		self.cursor.execute(sql)
		data = self.cursor.fetchall()
		expect = result1
		queryRows = len(data)
		tdLog.info("query data, rows:%d" % (queryRows))		
		if queryRows != expect:
			tdLog.exit("queryRows:%d != expect:%d" % (queryRows, expect))
			
		sql = 'select * from tb where ts < %s and ts > %s order by ts desc' % (start2, end2)
		expectErrNotOccured = True
		try:
			self.cursor.execute(sql)
		except:
			expectErrNotOccured = False
		if expectErrNotOccured :
			tdLog.debug("expect error: %s" % (sql))
		
		sql = 'select * from tb where ts < %s and ts > %s order by ts desc' % (start2, end3)
		expectErrNotOccured = True
		try:
			self.cursor.execute(sql)
		except:
			expectErrNotOccured = False
		if expectErrNotOccured :
			tdLog.debug("expect error: %s" % (sql))
			
		tdLog.info("=============== step 4.3")		
		sql = 'select * from tb where ts < %s and ts > %s order by ts desc' % (start3, end1)
		self.cursor.execute(sql)
		data = self.cursor.fetchall()
		expect = result2
		queryRows = len(data)
		tdLog.info("query data, rows:%d" % (queryRows))		
		if queryRows != expect:
			tdLog.exit("queryRows:%d != expect:%d" % (queryRows, expect))
			
		sql = 'select * from tb where ts < %s and ts > %s order by ts desc' % (start3, end2)
		self.cursor.execute(sql)
		data = self.cursor.fetchall()
		expect = result1
		queryRows = len(data)
		tdLog.info("query data, rows:%d" % (queryRows))		
		if queryRows != expect:
			tdLog.exit("queryRows:%d != expect:%d" % (queryRows, expect))
		
		sql = 'select * from tb where ts < %s and ts > %s order by ts desc' % (start3, end3)
		expectErrNotOccured = True
		try:
			self.cursor.execute(sql)
		except:
			expectErrNotOccured = False
		if expectErrNotOccured :
			tdLog.debug("expect error: %s" % (sql))		
		
	def stop(self):
		self.cursor.close()
		tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())