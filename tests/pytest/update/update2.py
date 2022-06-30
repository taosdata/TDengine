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
from util.sql import *
from util.dnodes import *
import random
import datetime
import threading


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        
        self.lock = threading.RLock()
        self.ts = []
    
    def restartTaosd(self):
        tdDnodes.stop(1)
        tdDnodes.startWithoutSleep(1)
        tdSql.execute("use db")
    
    def insertData(self):
        self.lock.acquire()
        try:
            sql = "insert into test2.warning_1 values "
            for i in range(10):
                ct = datetime.datetime.now()
                t = int(ct.timestamp() * 1000)
                self.ts.append(t)
                wait = random.randint(1, 9)
                time.sleep(0.0001 * wait)
                sql += "(%d, %d, 0, 0, 0, %d, 0, %f, %f, 0, 0, %d, %d, False, 0, '', '', 0, False, %d)" % (t, random.randint(0, 20), random.randint(1, 10000), random.uniform(0, 1), random.uniform(0, 1), random.randint(1, 10000), random.randint(1, 10000), t)                                
            tdSql.execute(sql)            
        finally:
            self.lock.release()
    
    def updateData(self):
        self.lock.acquire()
        try:
            sql = "insert into test2.warning_1(ts,endtime,maxspeed,endlongitude,endlatitude,drivercard_id,status,endmileage) values "
            for t in self.ts:
                sql += "(%d, %d, 0, %f, %f, 0, False, %d)" % (t, t, random.uniform(0, 1), random.uniform(0, 1), random.randint(1, 10000))                
            tdSql.execute(sql)
            self.ts.clear()       
        finally:
            self.lock.release()
        
    def run(self):

        tdSql.execute("CREATE DATABASE test2 CACHE 1 BLOCKS 3 UPDATE 2")
        tdSql.execute("use test2")
        tdSql.execute('''CREATE TABLE test2.fx_car_warning (ts TIMESTAMP, type TINYINT, level TINYINT, origin TINYINT, endtime BIGINT, mileage INT, maxspeed DOUBLE, 
            longitude DOUBLE, latitude DOUBLE, endlongitude DOUBLE, endlatitude DOUBLE, drivercard_id BIGINT, infoid INT, status BOOL, endmileage INT, 
            duty_officer NCHAR(10), content NCHAR(100), cltime BIGINT, clstatus BOOL, starttime BIGINT) TAGS (catid BIGINT, car_id BIGINT, mytype TINYINT)''')        
        tdSql.execute("create table test2.warning_1 using test2.fx_car_warning tags(1, 1, 0)")         
        tdLog.sleep(1)
        
        for i in range(1000):
            t1 = threading.Thread(target=self.insertData, args=( ))
            t2 = threading.Thread(target=self.updateData, args=( ))
            t1.start()
            t2.start()
            t1.join()
            t2.join()
        
        tdSql.query("select * from test2.fx_car_warning where type is null")
        tdSql.checkRows(0)
                
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())