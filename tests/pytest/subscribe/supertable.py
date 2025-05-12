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
		sqlstr = "select * from meters"
		topic = "test"
		now = int(time.time() * 1000)
		tdSql.prepare()

		numTables = 2000
		rowsPerTable = 5
		totalRows = numTables * rowsPerTable
		tdLog.info("create a super table and %d sub-tables, then insert %d rows into each sub-table." % (numTables, rowsPerTable))
		tdSql.execute("create table meters(ts timestamp, a int, b int) tags(area int, loc binary(20));")
		for i in range(0, numTables):
			for j in range(0, rowsPerTable):
				tdSql.execute("insert into t%d using meters tags(%d, 'area%d') values (%d, %d, %d);" % (i, i, i, now + j, j, j))

		tdLog.info("consumption 01.")
		tdSub.init(self.conn.subscribe(True, topic, sqlstr, 0))
		tdSub.consume()
		tdSub.checkRows(totalRows)

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
		tdSub.checkRows(totalRows + 1)

		tdLog.info("consumption 06: keep progress and restart the subscription")
		tdSub.close(True)
		tdSub.init(self.conn.subscribe(True, topic, sqlstr, 0))
		tdSub.consume()
		tdSub.checkRows(totalRows + 1)

		tdLog.info("consumption 07: insert one row to two table then remove one table")
		tdSql.execute("insert into t0 values (%d, 11, 11);" % (now + 11))
		tdSql.execute("insert into t%d values (%d, 11, 11);" % ((numTables-1), (now + 11)))
		tdSql.execute("drop table t0")
		tdSub.consume()
		tdSub.checkRows(1)

		tdLog.info("consumption 08: check timestamp criteria")
		tdSub.close(False)
		tdSub.init(self.conn.subscribe(True, topic, sqlstr + " where ts > %d" % now, 0))
		tdSub.consume()
		tdSub.checkRows((numTables-1) * (rowsPerTable-1) + 1)

		tdLog.info("consumption 09: insert large timestamp to t2 then insert smaller timestamp to t1")
		tdSql.execute("insert into t2 values (%d, 100, 100);" % (now + 100))
		tdSub.consume()
		tdSub.checkRows(1)
		tdSql.execute("insert into t1 values (%d, 12, 12);" % (now + 12))
		tdSub.consume()
		tdSub.checkRows(1)

		tdLog.info("consumption 10: field criteria")
		tdSub.close(True)
		tdSub.init(self.conn.subscribe(False, topic, sqlstr + " where a > 100", 0))
		tdSql.execute("insert into t2 values (%d, 101, 100);" % (now + 101))
		tdSql.execute("insert into t2 values (%d, 100, 100);" % (now + 102))
		tdSql.execute("insert into t2 values (%d, 102, 100);" % (now + 103))
		tdSub.consume()
		tdSub.checkRows(2)

		tdLog.info("consumption 11: two vnodes")
		tdSql.execute("insert into t2 values (%d, 102, 100);" % (now + 104))
		tdSql.execute("insert into t1299 values (%d, 102, 100);" % (now + 104))
		tdSub.consume()
		tdSub.checkRows(2)

		tdLog.info("consumption 12: create a new table")
		tdSql.execute("insert into t%d using meters tags(%d, 'area%d') values (%d, 102, 100);" % (numTables, numTables, numTables, now + 105))
		tdSub.consume()
		tdSub.checkRows(1)

	def stop(self):
		tdSub.close(False)
		tdSql.close()
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
