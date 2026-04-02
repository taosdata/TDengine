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
		tdSql.execute('create database db days 7')
		tdSql.execute('use db')
		tdSql.execute('create table tb(ts timestamp, i int)')
		
		tdLog.info("================= step1")	
		tdSql.execute('import into tb values(now+10000a, 10000)')
		tdSql.query('select * from tb')
		tdSql.checkRows(1)
		
		tdLog.info("================= step2")	
		tdSql.execute('insert into tb values(now+8000a, 8000)')
		tdSql.query('select * from tb')
		tdSql.checkRows(1)
		
		tdLog.info("================= step3")	
		tdSql.execute('insert into tb values(now+20000a, 20000)')
		tdSql.query('select * from tb')
		tdSql.checkRows(2)
		
		tdLog.info("================= step4")	
		tdSql.execute('import into tb values(now+8000a, 8000)')
		tdSql.execute('import into tb values(now+15000a, 15000)')
		tdSql.execute('import into tb values(now+30000a, 30000)')
		tdSql.query('select * from tb')
		tdSql.checkRows(5)
		
		tdLog.info("================= step5")	
		tdSql.execute('insert into tb values(now+8000a, 8000)')
		tdSql.execute('insert into tb values(now+14000a, 14000)')
		tdSql.execute('insert into tb values(now+25000a, 25000)')
		tdSql.execute('insert into tb values(now+40000a, 40000)')
		tdSql.query('select * from tb')
		tdSql.checkRows(6)
		
		tdLog.info("================= step6")	
		tdSql.execute('import into tb values(now+7000a, 7000)')
		tdSql.execute('import into tb values(now+12000a, 12000)')
		tdSql.execute('import into tb values(now+23000a, 23000)')
		tdSql.execute('import into tb values(now+34000a, 34000)')
		tdSql.execute('import into tb values(now+50000a, 50000)')
		tdSql.query('select * from tb')
		tdSql.checkRows(11)
		
		tdLog.info("================= dnode restart")	
		tdDnodes.stop(1)
		tdLog.sleep(5)
		tdDnodes.start(1)
		tdLog.sleep(3)
		tdSql.query('select * from tb')
		tdSql.checkRows(11)
		
		tdLog.info("================= step7")	
		tdSql.execute('import into tb values(now+7001a, 7001)')
		tdSql.execute('import into tb values(now+12001a, 12001)')
		tdSql.execute('import into tb values(now+23001a, 23001)')
		tdSql.execute('import into tb values(now+34001a, 34001)')
		tdSql.execute('import into tb values(now+50001a, 50001)')
		tdSql.query('select * from tb')
		tdSql.checkRows(16)
		
		tdLog.info("================= step8")	
		tdSql.execute('insert into tb values(now+8002a, 8002)')
		tdSql.execute('insert into tb values(now+14002a, 14002)')
		tdSql.execute('insert into tb values(now+25002a, 25002)')
		tdSql.execute('insert into tb values(now+200000a, 200000)')
		tdSql.query('select * from tb')
		tdSql.checkRows(17)
		
		tdLog.info("================= step9")	
		tdSql.execute('import into tb values(now-30d, 7003)')
		tdSql.execute('import into tb values(now-20d, 34003)')
		tdSql.execute('import into tb values(now-10d, 34003)')
		tdSql.execute('import into tb values(now-5d, 34003)')
		tdSql.execute('import into tb values(now+1m, 50001)')
		tdSql.execute('import into tb values(now+2m, 50001)')
		tdSql.execute('import into tb values(now+3m, 50001)')
		tdSql.execute('import into tb values(now+4m, 50002)')
		tdSql.execute('import into tb values(now+5m, 50003)')
		tdSql.execute('import into tb values(now+6m, 50004)')
		tdSql.execute('import into tb values(now+7m, 50001)')
		tdSql.execute('import into tb values(now+8m, 500051)')
		tdSql.query('select * from tb')
		tdSql.checkRows(29)
		
		tdLog.info("================= step10")	
		tdDnodes.stop(1)
		tdLog.sleep(5)
		tdDnodes.start(1)
		tdLog.sleep(3)
		tdSql.query('select * from tb')
		tdSql.checkRows(29)
		
		tdLog.info("================= step11")	
		tdSql.execute('import into tb values(now-50d, 7003) (now-48d, 7003) (now-46d, 7003) (now-44d, 7003) (now-42d, 7003)')
		tdSql.query('select * from tb')
		tdSql.checkRows(34)
		
		tdLog.info("================= step12")	
		tdSql.execute('import into tb values(now-19d, 7003) (now-18d, 7003) (now-17d, 7003) (now-16d, 7003) (now-15d, 7003) (now-14d, 7003) (now-13d, 7003) (now-12d, 7003) (now-11d, 7003)')
		tdSql.query('select * from tb')
		tdSql.checkRows(43)
		
	def stop(self):
		tdSql.close()
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addLinux(__file__, TDTestCase())