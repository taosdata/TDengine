###################################################################
 #		   Copyright (c) 2016 by TAOS Technologies, Inc.
 #				     All rights reserved.
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

class TDTestCase:
	def init(self, conn):
		tdLog.debug("start to execute %s" % __file__)
		tdSql.init(conn.cursor())
		
	def run(self):
		tdSql.prepare()
		tdSql.execute('create table tb(ts timestamp, i int, j float, k double)')

		tdLog.info("================= step1")	
		
		i = 0
		timestamp = 1564641700000
		while i < 1000 :
			val = 10 * i
			val = val + 2
			ts = timestamp + 200
			tdSql.execute("insert into tb values(%d, %d, %d, %d )" % (ts, val, val, val))

			val = 10 * i
			val = val + 3
			ts = timestamp + 300
			tdSql.execute("insert into tb values(%d, %d, %d, %d )" % (ts, val, val, val))

			val = 10 * i
			val = val + 4
			ts = timestamp + 400
			tdSql.execute("insert into tb values(%d, %d, %d, %d )" % (ts, val, val, val))

			val = 10 * i
			val = val + 5
			ts = timestamp + 500
			tdSql.execute("insert into tb values(%d, %d, %d, %d )" % (ts, val, val, val))

			val = 10 * i
			val = val + 6
			ts = timestamp + 600
			tdSql.execute("insert into tb values(%d, %d, %d, %d )" % (ts, val, val, val))

			val = 10 * i
			val = val + 7
			ts = timestamp + 700
			tdSql.execute("insert into tb values(%d, %d, %d, %d )" % (ts, val, val, val))

			val = 10 * i
			val = val + 8
			ts = timestamp + 800
			tdSql.execute("insert into tb values(%d, %d, %d, %d )" % (ts, val, val, val))

			val = 10 * i
			val = val + 9
			ts = timestamp + 900
			tdSql.execute("insert into tb values(%d, %d, %d, %d )" % (ts, val, val, val))

			val = 10 * i
			val = val + 1
			ts = timestamp + 100
			tdSql.execute("import into tb values(%d, %d, %d, %d )" % (ts, val, val, val))

			val = 10 * i
			val = val + 0
			ts = timestamp + 0
			tdSql.execute("import into tb values(%d, %d, %d, %d )" % (ts, val, val, val))

			timestamp = timestamp + 1000
			i = i + 1

		tdLog.info("================= step2")	
		tdSql.query("select * from tb")
		tdSql.checkRows(10000)
		
	def stop(self):
		tdSql.close()
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())