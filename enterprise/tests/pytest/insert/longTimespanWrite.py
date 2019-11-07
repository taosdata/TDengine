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
        time.sleep(3)
        
        self.db = "db"
        self.stb = "stb"
        self.tbPrefix = "tb"
        self.rowNum = 100000
        self.count = 0
        # self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
        self.threadNum = 1
        # threadLock = threading.Lock()
        # global counter for number of tables created by all threads
        self.global_counter = 0

        tdSql.init(self.conn.cursor())

    def _interfereDnodes(self, threadId, dnodeId):
        ## interfere dnode while creating table
        print "Thread%d to interfere dnode%d" %(threadId, dnodeId)
        while self.global_counter < self.tbNum * 0.05:
            time.sleep(0.2)
        tdDnodes.forcestop(dnodeId)
        while self.global_counter < self.tbNum * 0.15:
            time.sleep(0.2)
        tdDnodes.start(dnodeId)
        while self.global_counter < self.tbNum * 0.35:
            time.sleep(0.2)
        tdDnodes.forcestop(dnodeId)
        while self.global_counter < self.tbNum * 0.45:
            time.sleep(0.2)
        tdDnodes.start(dnodeId)
        while self.global_counter < self.tbNum * 0.65:
            time.sleep(0.2)
        tdDnodes.forcestop(dnodeId)
        while self.global_counter < self.tbNum * 0.85:
            time.sleep(0.2)
        tdDnodes.start(dnodeId)

    def run (self):
        tdLog.info("================= write long timespan data") 
        threadId = 0
        threads = []
        try:
            tdSql.execute("drop database if exists %s" %(self.db))  
            tdSql.execute("create database %s keep 1095" %(self.db))
            tdLog.sleep(3)
            tdSql.execute("use %s" %(self.db))
            tdSql.execute("create table car (ts timestamp, c1 float, c2 float) tags(t1 int)")
            tdLog.info("Start to auto create tables")
            tdSql.execute("create table strm as select count(*), avg(c1) from car interval(10s) sliding(5s)")
            tdLog.sleep(3)
            ts = 1199116800000 # 2009-01-01 00:00:00.000
            t1 = 1519833600000 # 2018-03-01 00:00:00.000
            sql = ""
            for i in range(2):
                tbName = "car%d" %(i)
                for j in range(1, self.rowNum + 1):
                    ts = ts + j
                    sql = "insert into %s using car tags (%d) values (%d, %d, %d)" %(tbName, i, ts, j, j)
                    tdSql.execute(sql)
                    if j % 10 == 0:
                        sql = "import into %s values (%d, %d, %d)" %(tbName, t1 - 3 * j, -j, -j)
                        tdSql.execute(sql)
                    
            tdSql.query("select count(*) from car")
            tdSql.checkData(0, 0, int(self.rowNum / 10 * 2 + 20))
            tdLog.info("Restart service to commit data to file")
            tdDnodes.stop(1)
            tdLog.sleep(5)
            tdDnodes.start(1)
            tdSql.query("select count(*) from car")
            tdSql.checkData(0, 0, int(self.rowNum /10 * 2))

        except Exception as e:
            tdLog.info("Failed to execute: %s, exception: %s" %(sql, str(e)))
            # tdDnodes.stopAll()
        finally:
            time.sleep(1)

        # threading.Thread(target=self._interfereDnodes, name="thread-interfereDnode%d" %(3), args=(1, 3,)).start()
        # for t in range (len(threads)):
        #         tdLog.info("Join threads")
        #         # threads[t].start()
        #         threads[t].join()

        # tdSql.query("show stables")
        # tdSql.checkData(0, 4, self.tbNum)
            
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
    
tdCases.addCluster(__file__, TDTestCase())