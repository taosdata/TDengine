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
		tdSql.execute('create table tb (ts timestamp, i int)')

		tdLog.info("================= step1")	
		tdSql.execute('import into tb values(1564641710000, 10000)')
		tdSql.query("select * from tb")
		tdSql.checkRows(1)
		
		tdLog.info("================= step2")	
		tdSql.execute('insert into tb values(1564641708000, 8000)')
		tdSql.query("select * from tb")
		tdSql.checkRows(1)
		
		tdLog.info("================= step3")	
		tdSql.execute('insert into tb values(1564641720000, 20000)')
		tdSql.query("select * from tb")
		tdSql.checkRows(2)
		
		tdLog.info("================= step4")	
		tdSql.execute('import into tb values(1564641708000, 8000)')
		tdSql.execute('import into tb values(1564641715000, 15000)')
		tdSql.execute('import into tb values(1564641730000, 30000)')
		tdSql.query("select * from tb")
		tdSql.checkRows(5)
		
		tdLog.info("================= step5")	
		tdSql.execute('insert into tb values(1564641708000, 8000)')
		tdSql.execute('insert into tb values(1564641714000, 14000)')
		tdSql.execute('insert into tb values(1564641740000, 40000)')
		tdSql.query("select * from tb")
		tdSql.checkRows(6)
		
		tdLog.info("================= step6")	
		tdSql.execute('import into tb values(1564641707000, 7000)')
		tdSql.execute('import into tb values(1564641712000, 12000)')
		tdSql.execute('import into tb values(1564641723000, 23000)')
		tdSql.execute('import into tb values(1564641734000, 34000)')
		tdSql.execute('import into tb values(1564641750000, 50000)')
		tdSql.query("select * from tb")
		tdSql.checkRows(11)
		
		tdLog.info("================= step7")	
		tdSql.execute('import into tb values(1564641707001, 7001)')
		tdSql.execute('import into tb values(1564641712001, 12001)')
		tdSql.execute('import into tb values(1564641723001, 23001)')
		tdSql.execute('import into tb values(1564641734001, 34001)')
		tdSql.execute('import into tb values(1564641750001, 50001)')
		tdSql.query("select * from tb")
		tdSql.checkRows(16)
		
		tdLog.info("================= step8")	
		tdSql.execute('insert into tb values(1564641708002, 8002)')
		tdSql.execute('insert into tb values(1564641714002, 14002)')
		tdSql.execute('insert into tb values(1564641725002, 25002)')
		tdSql.execute('insert into tb values(1564641900000, 200000)')
		tdSql.query("select * from tb")
		tdSql.checkRows(17)
		
		tdLog.info("================= step9 only insert last one")	
		tdSql.execute('import into tb values(1564641705000, 5000)(1564641718000, 18000)(1564642400000, 700001)')
		tdSql.query("select * from tb")
		tdSql.checkRows(18)
		
		tdLog.info("================= step10")	
		tdSql.execute('import into tb values(1564641705000, 5000)(1564641718000, 18003)(1564642400000, 700002)')
		tdSql.query("select * from tb")
		tdSql.checkRows(19)
		
		tdLog.info("================= step11")	
		tdSql.execute('import into tb values(1564642400000, 70000)')
		tdSql.query("select * from tb")
		tdSql.checkRows(19)
		
		tdLog.info("================= step12")	
		tdSql.execute('import into tb values(1564641709527, 9527)(1564641709527, 9528)')
		tdSql.query("select * from tb")
		tdSql.checkRows(20)
		
		tdLog.info("================= step13")	
		tdSql.execute('import into tb values(1564641709898, 9898)(1564641709897, 9897)')
		tdSql.query("select * from tb")
		tdSql.checkRows(22)
		
	def stop(self):
		tdSql.close()
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
