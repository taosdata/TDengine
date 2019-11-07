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
		tdSql.execute('create dnode 192.168.0.3')
		tdDnodes.deploy(3)
		tdDnodes.start(3)
		tdLog.sleep(10)

	def run(self):
		tdSql.execute('create database db replica 3 days 7')
		tdSql.execute('use db')
		tdSql.execute('create table tb(ts timestamp, i int)')
		tdLog.sleep(10)

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
		tdSql.execute('import into tb values(now-30m, 7003)')
		tdSql.execute('import into tb values(now-20m, 34003)')
		tdSql.execute('import into tb values(now-10m, 34003)')
		tdSql.execute('import into tb values(now-5m, 34003)')
		tdSql.execute('import into tb values(now+1m, 50001)')
		tdSql.execute('import into tb values(now+2m, 50001)')
		tdSql.execute('import into tb values(now+6m, 50001)')
		tdSql.execute('import into tb values(now+8m, 50002)')
		tdSql.execute('import into tb values(now+10m, 50003)')
		tdSql.execute('import into tb values(now+12m, 50004)')
		tdSql.execute('import into tb values(now+14m, 50001)')
		tdSql.execute('import into tb values(now+16m, 500051)')
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
		tdSql.execute('import into tb values(now-50m, 7003) (now-48m, 7003) (now-46m, 7003) (now-44m, 7003) (now-42m, 7003)')
		tdSql.query('select * from tb')
		tdSql.checkRows(34)
		
		tdLog.info("================= step12")	
		tdSql.execute('import into tb values(now-19m, 7003) (now-18m, 7003) (now-17m, 7003) (now-16m, 7003) (now-15m, 7003) (now-14m, 7003) (now-13m, 7003) (now-12m, 7003) (now-11m, 7003)')
		tdSql.query('select * from tb')
		tdSql.checkRows(43)
		
		tdLog.info("================= step13")	
		tdDnodes.stop(2)
		tdLog.sleep(5)
		
		tdLog.info("================= step14")	
		tdSql.execute('import into tb values(now-48m, 34003)')
		tdSql.execute('import into tb values(now-38m, 50001)')
		tdSql.execute('import into tb values(now-28m, 50001)')
		tdSql.query('select * from tb')
		tdSql.checkRows(46)
		
		tdLog.info("================= step15")	
		tdDnodes.start(2)
		tdLog.sleep(8)
		tdSql.query('select * from tb')
		tdSql.checkRows(46)
		
		tdLog.info("================= step16")	
		tdDnodes.stop(3)
		tdLog.sleep(5)
		tdSql.query('select * from tb')
		tdSql.checkRows(46)
		
	def stop(self):
		tdSql.close()
		self.conn.close()
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addCluster(__file__, TDTestCase())