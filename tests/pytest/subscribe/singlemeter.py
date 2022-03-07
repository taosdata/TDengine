###################################################################
 #		   Copyright (c) 2020 by TAOS Technologies, Inc.
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
import time
from util.log import *
from util.cases import *
from util.sql import *
from util.sub import *

class TDTestCase:
	def init(self, conn, logSql):
		tdLog.debug("start to execute %s" % __file__)
		tdSql.init(conn.cursor(), logSql)
		self.conn = conn

	def run(self):
		sqlstr = "select * from t0"
		topic = "test"
		now = int(time.time() * 1000)
		tdSql.prepare()

		tdLog.info("create a table and insert 10 rows.")
		tdSql.execute("create table t0(ts timestamp, a int, b int);")
		for i in range(0, 10):
			tdSql.execute("insert into t0 values (%d, %d, %d);" % (now + i, i, i))

		tdLog.info("consumption 01.")
		tdSub.init(self.conn.subscribe(True, topic, sqlstr, 0))
		tdSub.consume()
		tdSub.checkRows(10)

		tdLog.info("consumption 02: no new rows inserted")
		tdSub.consume()
		tdSub.checkRows(0)

		tdLog.info("consumption 03: after one new rows inserted")
		tdSql.execute("insert into t0 values (%d, 10, 10);" % (now + 10))
		tdSub.consume()
		tdSub.checkRows(1)

		tdLog.info("consumption 04: keep progress and continue previous subscription")
		tdSub.close(True)
		tdSub.init(self.conn.subscribe(False, topic, sqlstr, 0))
		tdSub.consume()
		tdSub.checkRows(0)

		tdLog.info("consumption 05: remove progress and continue previous subscription")
		tdSub.close(False)
		tdSub.init(self.conn.subscribe(False, topic, sqlstr, 0))
		tdSub.consume()
		tdSub.checkRows(11)

		tdLog.info("consumption 06: keep progress and restart the subscription")
		tdSub.close(True)
		tdSub.init(self.conn.subscribe(True, topic, sqlstr, 0))
		tdSub.consume()
		tdSub.checkRows(11)

		tdSub.close(True)

	def stop(self):
		tdSub.close(False)
		tdSql.close()
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
