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
		
	def run(self):
		tdSql.execute('create database db replica 2 days 7')
		tdSql.execute('use db')
		tdSql.execute('create table tb(ts timestamp, i int)')
		tdLog.sleep(10)
		
		tdLog.info("================= step1")	
		tdSql.execute('import into tb values(1520000010000, 27)')
		tdSql.query('select * from tb')
		tdSql.checkRows(1)
		
		tdLog.info("================= step2")	
		tdSql.execute('insert into tb values(1520000008000, 100)')
		tdSql.query('select * from tb')
		tdSql.checkRows(1)
		
		tdLog.info("================= step3")	
		tdSql.execute('insert into tb values(1520000020000, 31)')
		tdSql.query('select * from tb')
		tdSql.checkRows(2)
		
		tdLog.info("================= step4")	
		tdSql.execute('import into tb values(1520000009000, 26)')
		tdSql.execute('import into tb values(1520000015000, 30)')
		tdSql.execute('import into tb values(1520000030000, 34)')
		tdSql.query('select * from tb')
		tdSql.checkRows(5)
		
		tdLog.info("================= step5")	
		tdSql.execute('insert into tb values(1520000008000, 101)')
		tdSql.execute('insert into tb values(1520000014000, 102)')
		tdSql.execute('insert into tb values(1520000025000, 103)')
		tdSql.execute('insert into tb values(1520000040000, 37)')
		tdSql.query('select * from tb')
		tdSql.checkRows(6)
		
		tdLog.info("================= step6")	
		tdSql.execute('import into tb values(1520000007000, 24)')
		tdSql.execute('import into tb values(1520000012000, 28)')
		tdSql.execute('import into tb values(1520000023000, 32)')
		tdSql.execute('import into tb values(1520000034000, 35)')
		tdSql.execute('import into tb values(1520000050000, 38)')
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
		tdSql.execute('import into tb values(1520000007001, 25)')
		tdSql.execute('import into tb values(1520000012001, 29)')
		tdSql.execute('import into tb values(1520000023001, 33)')
		tdSql.execute('import into tb values(1520000034001, 36)')
		tdSql.execute('import into tb values(1520000050001, 39)')
		tdSql.query('select * from tb')
		tdSql.checkRows(16)
		
		tdLog.info("================= step8")	
		tdSql.execute('insert into tb values(1520000008002, 104)')
		tdSql.execute('insert into tb values(1520000014002, 105)')
		tdSql.execute('insert into tb values(1520000025002, 106)')
		tdSql.execute('insert into tb values(1520000060000, 40)')
		tdSql.query('select * from tb')
		tdSql.checkRows(17)
		
		tdLog.info("================= step9")	
		tdSql.execute('import into tb values(1517408000000, 8)')
		tdSql.execute('import into tb values(1518272000000, 10)')
		tdSql.execute('import into tb values(1519136000000, 20)')
		tdSql.execute('import into tb values(1519568000000, 21)')
		tdSql.execute('import into tb values(1519654400000, 22)')
		tdSql.execute('import into tb values(1519827200000, 23)')
		tdSql.execute('import into tb values(1520345600000, 41)')
		tdSql.execute('import into tb values(1520691200000, 42)')
		tdSql.execute('import into tb values(1520864000000, 43)')
		tdSql.execute('import into tb values(1521900800000, 45)')
		tdSql.execute('import into tb values(1523110400000, 46)')
		tdSql.execute('import into tb values(1521382400000, 44)')
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
		tdSql.execute('import into tb values(1515680000000, 1) (1515852800000, 2) (1516025600000, 4) (1516198400000, 5) (1516371200000, 6)')
		tdSql.query('select * from tb')
		tdSql.checkRows(34)
		
		tdLog.info("================= step12")	
		tdSql.execute('import into tb values(1518358400000, 11) (1518444800000, 12) (1518531200000, 13) (1518617600000, 14) (1518704000000, 15) (1518790400000, 16) (1518876800000, 17) (1518963200000, 18) (1519049600000, 19)')
		tdSql.query('select * from tb')
		tdSql.checkRows(43)
		
		tdLog.info("================= step13")	
		tdDnodes.stop(2)
		tdLog.sleep(5)
		
		tdLog.info("================= step14")	
		tdSql.execute('import into tb values(1515852800001, 3)')
		tdSql.execute('import into tb values(1516716800000, 7)')
		tdSql.execute('import into tb values(1517580800000, 9)')
		tdSql.query('select * from tb')
		tdSql.checkRows(46)
		
		tdLog.info("================= step15")	
		tdDnodes.start(2)
		tdLog.sleep(8)
		tdSql.query('select * from tb')
		tdSql.checkRows(46)
		
		tdLog.info("================= step16")	
		tdDnodes.stop(1)
		tdLog.sleep(5)
		tdSql.query('select * from tb')
		tdSql.checkRows(46)
		
	def stop(self):
		tdSql.close()
		self.conn.close()
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addCluster(__file__, TDTestCase())
