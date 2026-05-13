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
from util.dnodes import *

class TDTestCase:
	def init(self, conn):
		tdLog.debug("start to execute %s" % __file__)
		tdSql.init(conn.cursor())
		
	def run(self):
		tdLog.info("prepare database")	
		tdSql.execute('reset query cache')
		tdSql.execute('drop database db')
		
		tdLog.info("================= step1")	
		tdSql.execute('create database ic1db days 7')
		tdSql.execute('create table ic1db.tb(ts timestamp, s int)')
		tdSql.execute('insert into ic1db.tb values(now-30d, -30)')
		tdSql.execute('insert into ic1db.tb values(now-20d, -20)')
		tdSql.execute('insert into ic1db.tb values(now-10d, -10)')
		tdSql.execute('insert into ic1db.tb values(now-5d, -5)')
		tdSql.execute('insert into ic1db.tb values(now+1d, 1)')
		tdSql.execute('insert into ic1db.tb values(now+2d, 2)')
		tdSql.execute('insert into ic1db.tb values(now+6d, 6)')
		tdSql.execute('insert into ic1db.tb values(now+8d, 8)')
		tdSql.execute('insert into ic1db.tb values(now+10d, 10)')
		tdSql.execute('insert into ic1db.tb values(now+12d, 12)')
		tdSql.execute('insert into ic1db.tb values(now+14d, 14)')
		tdSql.execute('insert into ic1db.tb values(now+16d, 16)')
		tdSql.query("select * from ic1db.tb")
		tdSql.checkRows(12)
		
		tdLog.info("================= step2")	
		tdSql.execute('create database ic2db days 7')
		tdSql.execute('create table ic2db.tb(ts timestamp, s int)')
		tdSql.execute('insert into ic2db.tb values(now, 0)')
		tdSql.execute('import into ic2db.tb values(now-30d, -30)')
		tdSql.execute('import into ic2db.tb values(now-20d, -20)')
		tdSql.execute('import into ic2db.tb values(now-10d, -10)')
		tdSql.execute('import into ic2db.tb values(now-5d, -5)')
		tdSql.execute('import into ic2db.tb values(now+1d, 1)')
		tdSql.execute('import into ic2db.tb values(now+2d, 2)')
		tdSql.execute('import into ic2db.tb values(now+6d, 6)')
		tdSql.execute('import into ic2db.tb values(now+8d, 8)')
		tdSql.execute('import into ic2db.tb values(now+10d, 10)')
		tdSql.execute('import into ic2db.tb values(now+12d, 12)')
		tdSql.execute('import into ic2db.tb values(now+14d, 14)')
		tdSql.execute('import into ic2db.tb values(now+16d, 16)')
		tdSql.query("select * from ic2db.tb")
		tdSql.checkRows(13)
		
		tdLog.info("================= step3")	
		tdDnodes.stop(1)
		time.sleep(5)
		tdDnodes.start(1)
		time.sleep(5)
		
		tdLog.info("================= step4")	
		tdSql.query("select * from ic1db.tb")
		tdSql.checkRows(12)
		tdSql.query("select * from ic2db.tb")
		tdSql.checkRows(13)
		
	def stop(self):
		tdSql.execute('drop database ic1db')
		tdSql.execute('drop database ic2db')
		tdSql.close()
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addLinux(__file__, TDTestCase())