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
		self.cursor.execute("create database if not exists db replica 1 days 5 keep 3650 rows 1000 cache 1024000 ablocks 1.100000 tblocks 100 tables 100 precision 'us' ")
		self.cursor.execute('create table if not exists db.st (ts timestamp, SOURCE_TYPE binary(2), BIZ_TYPE binary(2), FID binary(100), RR_FLAG binary(1), FILE_NAME binary(50), DEAL_TIME binary(14), RECORD_TYPE binary(2), NI_PDP binary(1), MSISDN bigint, IMSI_NUMBER binary(20), SGSN binary(15), MSNC binary(1), LAC binary(5), RA binary(5), CELL_ID binary(10), CHARGING_ID binary(12), GGSN binary(15), APNNI binary(63), APNOI binary(37), PDP_TYPE binary(4), SPA binary(16), SGSN_CHANGE binary(2), SGSN_PLMN_ID binary(6), CAUSE_CLOSE binary(2), RESULT binary(1), HOME_AREA_CODE binary(10), VISIT_AREA_CODE binary(10), CITY_CODE binary(8), VISIT_AREA_HOMETYPE binary(1), USER_TYPE binary(5), FEE_TYPE binary(2), ROAM_TYPE binary(1), SERVICE_TYPE binary(3), IMEI binary(20), start_DATE binary(8), start_TIME binary(6), CALL_DURATION bigint, SERV_ID binary(32), SERV_GROUP binary(4), SERV_DURATION bigint, DATA_UP1 bigint, DATA_DOWN1 bigint, DATA_UP2 bigint, DATA_DOWN2 bigint, CHARGED_ITEM binary(2), CHARGED_OPERATION binary(2), CHARGED_UNITS bigint, FREE_CODE binary(512), BILL_ITEM binary(512), CFEE_ORG bigint, CFEE bigint, DIS_CFEE bigint, DFEE_ORG bigint, DFEE bigint, DIS_DFEE bigint, RECORDSEQNUM binary(8), FILE_NO binary(50), ERROR_CODE bigint, CUST_ID bigint, USER_ID bigint, A_PRODUCT_ID bigint, A_SERV_TYPE binary(5), CHANNEL_NO bigint, OFFICE_CODE binary(8), DOUBLEMODE binary(1), OPEN_DATETIME binary(14), A_USER_STAT binary(1), INTER_GPRSGROUP binary(3), APN_GROUP binary(3), APN_TYPE binary(3), TARIFF_FEE bigint, RATE_TIMES bigint, INDB_TIME bigint, RESERVER1 binary(100), RESERVER2 binary(100), RESERVER3 binary(100), RESERVER4 binary(100), RESERVER5 binary(100), RESERVER6 binary(100), RESERVER7 binary(100), RESERVER8 binary(100), PROVINCE_CODE binary(4), RATE_TYPE bigint, RESOURCELIST binary(500), CJ_INTIME binary(50)) tags(TAG_MSISDN bigint, TAG_PROVINCE_CODE binary(4))')

		tables = 1
		tdLog.info("create %d tables" % (tables))	
		for i in range(0, tables):
			self.cursor.execute("create table if not exists db.t%d using db.st tags(%d,'%d')" % (i, i, i))
		
		rowsPerBatch = 65
		insertBatchs = 10000
		tdLog.info("insert %d batchs(%d rows per batch) per table" % (insertBatchs, rowsPerBatch))	
		for t in range (0, tables):
			tdLog.info("insert table:db.t%d" % (t))	
			startTs = 1538408992959000
			for b in range (0, insertBatchs):
				sql = "import into "
				batchTs = startTs + b * 1000
				for r in range(0, rowsPerBatch):
					rowsTs = batchTs + r
					sql += "db.t%d values(%d,'31','3','HQ1538410793.366956267.1157632036.315754969','0','0','1002014031','11','0',13007599959,'46001','220.206.175.139','0','14182','0','72533297','730662547','220.206.175.137','3GNET','3GNET','0','0','0','46001','0','0','0372','0371','0371','1','2011','0','0','0','8664489025680140','20181001','234952',608,'1157632036','1157632036',608,1452,1199,0,0,'X','X',3072,'1|0|3070000|3071101|30;1|7617092290268708|47618390|5592026|0','0|1|201810|0|0|99997|2651;0|1|201810|7617092290268708|0|40001|2651:10058|0',0,30,0,0,0,0,'0','DCC_76_20181002001958_1946606061_1946606859',0,7617092225186828,7617092226230767,90215349,'4G00',51001,'762055','0','20040621180059','20040621180059','20040621180059','002','002',0,1,1002014031,'DCCEAF8B','DCCEAF8B','1','6','6','1','1','0','76',76,'76','76')" % (t, rowsTs)					
				affectedRows = self.cursor.execute(sql)
				if affectedRows != rowsPerBatch:
					tdLog.exit("batch:%d insertedRows:%d != affectedRows:%d" % (b, rowsPerBatch, affectedRows))
								
		tdLog.info("query %d tables" % (tables))	
		insertRows = insertBatchs * rowsPerBatch
		for t in range (0, tables):
			sql = 'select count(*) from db.t%d' % (t)
			self.cursor.execute(sql)
			data = self.cursor.fetchall()
			if len(data) != 1:
				tdLog.exit("no rows return from %s" % sql)
			if insertRows != data[0][0]:
				tdLog.exit("db.t%d insertRows:%d != queryRows:%d" % (t, insertRows, data[0][0]))
			tdLog.info("query table:db.t%d, rows:%d" % (t, data[0][0]))	
		
			
	def stop(self):
		self.cursor.close()
		tdLog.success("%s successfully executed" % __file__)
	
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())