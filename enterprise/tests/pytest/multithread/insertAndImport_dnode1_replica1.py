# -*- coding: utf-8 -*-  

import sys
import taos
import threading
import traceback
import random
import datetime
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
        # tdSql.execute('create dnode 192.168.0.2')
        # tdDnodes.deploy(2)
        # tdDnodes.start(2)
        # tdSql.execute('create dnode 192.168.0.3')
        # tdDnodes.deploy(3)
        # tdDnodes.start(3)
        # tdSql.execute('create dnode 192.168.0.4')
        # tdDnodes.deploy(4)
        # tdDnodes.start(4)
        # tdSql.execute('create dnode 192.168.0.5')
        # tdDnodes.deploy(5)
        # tdDnodes.start(5)
        # tdSql.execute('create dnode 192.168.0.6')
        # tdDnodes.deploy(6)
        # tdDnodes.start(6)
        time.sleep(3)
        
        self.db = "db"
        self.stb = "stb"
        self.tbPrefix = "tb"
        self.rowNum = 10000
        self.tbNum = 1000
        self.step = 1000
        # self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
        self.insertThreadNum = 4
        self.totalRowNum = self.insertThreadNum * self.rowNum
        # global counter for number of rows inserted
        self.global_counter = 0

        tdSql.init(self.conn.cursor())

    def _insertAndImport(self, threadId):
        print "Insert-thread%d starts inserting" %(threadId)
        conn = taos.connect(config=tdDnodes.getSimCfgPath())
        cursor = conn.cursor()
        cursor.execute("use %s" %(self.db))
        ts0 = 1546272000000 # 2019-01-01 00:00:00.000
        for i in range(self.rowNum):
            # ts = ts0 + i * self.step
            # insert one row into each table that the current thread is responsible for
            for tbId in range(self.tbNum):
                if tbId%self.insertThreadNum == threadId:
                    cursor.execute("insert into %s%d values (%d, %d, %d)" %(self.tbPrefix, tbId, (ts0 + i * self.step), i, i))
                    cursor.execute("import into %s%d values (%d, %d, %d)" %(self.tbPrefix, tbId, (ts0 - i * self.step), -i, -i))
                    with threading.Lock():
                        self.global_counter += 1
        cursor.close()
        conn.close()

    def run (self):
        tdLog.info("================= kill dnode while concurrently inserting and importing data") 
        try:
            tdSql.execute("drop database if exists %s" %(self.db))  
            tdSql.execute("create database %s replica 1 tables 200" %(self.db))
            tdLog.sleep(3)
            tdSql.execute("use %s" %(self.db))
            tdSql.execute("create table %s (ts timestamp, c1 float, c2 float) tags(t1 int)" %(self.stb))
            tdLog.info("Start to create tables")
            # tdSql.execute("create table strm as select count(*), avg(c1) from car interval(10s) sliding(5s)")
            tdLog.sleep(3)
            ts = 1546272000000 # 2019-01-01 00:00:00.000
            t1 = 1546272000000 # 2019-01-01 00:00:00.000
            sql = ""
            for i in range(self.tbNum):
                tbName = "%s%d" %(self.tbPrefix, i)
                tdSql.execute("create table %s using %s tags(%d)" %(tbName, self.stb, i))
            
        except Exception as e:
            tdLog.info("Failed to execute: %s, exception: %s" %(sql, str(e)))
            # tdDnodes.stopAll()
        finally:
            time.sleep(1)

        threads = []
        for t in range (self.insertThreadNum):
            threadName = "Insert-thread-%d" % (t)
            thread = threading.Thread(target=self._insertAndImport, name=threadName, args=(t,))
            thread.start()
            threads.append(thread)

        for t in range (len(threads)):
                tdLog.info("Join threads")
                # threads[t].start()
                threads[t].join()

        tdSql.query("select count(*) from %s" %(self.stb))
        tdSql.checkData(0, 0, self.totalRowNum)
            
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
    
tdCases.addCluster(__file__, TDTestCase())