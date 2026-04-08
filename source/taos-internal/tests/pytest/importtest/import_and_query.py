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
        
        self.db = "db"
        self.stb = "stb"
        self.tbPrefix = "tb"
        self.tbNum = 10
        self.rowsToImportPerTable = 2000
        self.rowsToInsertPerTable = 10000
        self.ts0 = 1537146000000L
        self.step = 1000L
        self.tsMax = self.ts0 + (self.rowsToImportPerTable + 1) * self.step
        self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
        self.importThreadNum = 2
        tdSql.init(self.conn.cursor())

    def _insert(self):
        conn = taos.connect(config=tdDnodes.getSimCfgPath())
        cursor = conn.cursor()
        insertLog = open("%s/dnode1/log/insert.log" %(tdDnodes.getDnodesRootDir()), "a")
        try:
            sql = "use %s" %(self.db)
            insertLog.write("%s %s\n" %(datetime.datetime.now(), sql))
            cursor.execute(sql)
            # for r in range (1, self.rowsToInsertPerTable + 1) :
            for r in range (self.rowsToInsertPerTable) :
                ## insert one row into tb with the maximum timestamp in this timeline
                for tbId in range (self.tbNum):
                    ts = self.tsMax + r * self.step
                    sql = "insert into tb%d values (%d, %d, now)" %(tbId, ts, r)
                    insertLog.write("%s %s\n" %(datetime.datetime.now(), sql))
                    cursor.execute(sql)
        except Exception,e:
            tdLog.exit("Failure when executing %s, exception: %s" %(sql, str(e)))
        finally:
            insertLog.close()
        tdLog.info("insert-thread-1 completed insert records")
        cursor.close()
        conn.close()

    def _import(self, threadId):
        conn = taos.connect(config=tdDnodes.getSimCfgPath())
        cursor = conn.cursor()
        importLog = open("%s/dnode1/log/import%d.log" %(tdDnodes.getDnodesRootDir(), threadId), "a")
        try:
            sql = "use %s" %(self.db)
            importLog.write("%s %s\n" %(datetime.datetime.now(), sql))
            cursor.execute(sql)
            for tbId in range (self.tbNum) :
                if tbId%threadId == 0:
                    ## insert one row into tb with the maximum timestamp in this timeline
                    sql = "insert into tb%d values (%d, %d, now)" %(tbId, self.tsMax, - threadId)
                    importLog.write("%s %s\n" %(datetime.datetime.now(), sql))
                    # tdSql.execute(sql)
                    cursor.execute(sql)
                    for r in range (self.rowsToImportPerTable / 2):
                        ts = self.ts0 + r * self.step
                        sql = "import into tb%d values (%d, %d, now)" %(tbId, ts, r)
                        importLog.write("%s %s\n" %(datetime.datetime.now(), sql))
                        # tdSql.execute(sql)
                        cursor.execute(sql)
                        ts = self.tsMax - r * self.step
                        sql = "import into tb%d values (%d, %d, now)" %(tbId, ts, r)
                        importLog.write("%s %s\n" %(datetime.datetime.now(), sql))
                        cursor.execute(sql)
        except Exception,e:
            tdLog.exit("Failure when importing, exception: %s" %(str(e)))
        finally:
            importLog.close()
            cursor.close()
            conn.close()
        tdLog.info("Thread-%d completed importing records" %(threadId))

    def _selectAllFromTable(self):
        conn = taos.connect(config=tdDnodes.getSimCfgPath())
        cursor = conn.cursor()
        loop = 10000
        lastQueryRows = [0 for i in range(self.tbNum)]
        l = 1
        queryLog = open("%s/selectAllFromTable.log" %(tdDnodes.getDnodesRootDir()), "a")
        sql = "use %s" %(self.db)
        try:
            queryLog.write("%s %s\n" %(datetime.datetime.now(), sql))
            cursor.execute(sql)
            while l <= loop:
                tbId = random.randint(0, self.tbNum - 1)
                sql = "select * from tb%d" %(tbId)
                cursor.execute(sql)
                queryLog.write("%s %s\n" %(datetime.datetime.now(), sql))
                queryResult = cursor.fetchall()
                queryRows = len(queryResult)
                if queryRows < lastQueryRows[tbId - 1]:
                    tdLog.exit("sql:%s, queryRows:%d < lastQueryRows:%d" % (sql, queryRows, lastQueryRows[tbId - 1]))
                else:
                    lastQueryRows[tbId - 1] = queryRows
                l += 1
        except Exception,e:
            tdLog.exit("Failure during querying all data from a single table %s, loop %d, sql: %s" %(str(e), l, sql))
        finally:
            cursor.close()
            conn.close()

    def _selectAggFromTable(self):
        conn = taos.connect(config=tdDnodes.getSimCfgPath())
        cursor = conn.cursor()
        loop = 10000
        lastQueryRows = [0 for i in range(self.tbNum)]
        l = 1
        queryLog = open("%s/selectAggFromTable.log" %(tdDnodes.getDnodesRootDir()), "a")
        sql = "use %s" %(self.db)
        try:
            queryLog.write("%s %s\n" %(datetime.datetime.now(), sql))
            cursor.execute(sql)
            while l <= loop:
                tbId = random.randint(0, self.tbNum - 1)
                sql = "select last_row(*) from tb%d" %(tbId)
                queryLog.write("%s %s\n" %(datetime.datetime.now(), sql))
                cursor.execute(sql)
                cursor.fetchall()
                sql = "select count(*), max(c1), min(c1), spread(c1), first(*), last(*) from tb%d"
                queryLog.write("%s %s\n" %(datetime.datetime.now(), sql))
                cursor.execute(sql)
                queryResult = cursor.fetchall()
                queryRows = queryResult[0][0]
                if (queryResult[0][1] - queryResult[0][2] != queryResult[0][3]):
                    tdLog.exit("sql:%s, max(c1): %d, min(c1): %d, spread(c1): %d, max(c1) - min(c1) != spread(c1)" %(sql, queryResult[0][1], queryResult[0][2], queryResult[0][3]))
                if queryRows < lastQueryRows[tbId - 1]:
                    tdLog.exit("sql:%s, queryRows:%d < lastQueryRows:%d" % (sql, queryRows, lastQueryRows[tbId - 1]))
                else:
                    lastQueryRows[tbId - 1] = queryRows
                l += 1
        except Exception,e:
            tdLog.exit("Failure during querying all data from a single table %s, loop %d, sql: %s" %(str(e), l, sql))
        finally:
            cursor.close()
            conn.close()

    def _selectFromSTable(self):
        conn = taos.connect(config=tdDnodes.getSimCfgPath())
        cursor = conn.cursor()
        loop = 10000
        lastQueryRows = [0 for i in range(self.tbNum)]
        l = 1
        queryLog = open("%s/selectFromSTable.log" %(tdDnodes.getDnodesRootDir()), "a")
        sql = "use %s" %(self.db)
        try:
            cursor.execute(sql)
            while l <= loop:
                tbId = random.randint(0, self.tbNum - 1)
                # sql = "select count(*), first(c1), last(c1), avg(c1), spread(c1) from %s%d" %(self.tbPrefix, tbId)
                sql = "select last_row(*) from %s" %(self.stb)
                cursor.execute(sql)
                sql = "select count(*), max(c1), min(c1), spread(c1), first(*), last(*) from %s" %(self.stb)
                cursor.execute(sql)
                queryResult = cursor.fetchall()
                queryRows = queryResult[0][0]
                if (queryResult[0][1] - queryResult[0][2] != queryResult[0][3]):
                    tdLog.exit("sql:%s, max(c1): %d, min(c1): %d, spread(c1): %d, max(c1) - min(c1) != spread(c1)" %(sql, queryResult[0][1], queryResult[0][2], queryResult[0][3]))
                if queryRows < lastQueryRows[tbId - 1]:
                    tdLog.exit("sql:%s, queryRows:%d < lastQueryRows:%d" % (sql, queryRows, lastQueryRows[tbId - 1]))
                else:
                    lastQueryRows[tbId - 1] = queryRows
                l += 1
        except Exception,e:
            tdLog.exit("Failure during querying all data from STable %s, loop %d, sql: %s" %(str(e), l, sql))
        finally:
            cursor.close()
            conn.close()

    def run (self):
        tdLog.info("================= creating schema") 
        i = 0
        try:
            tdSql.execute("drop database if exists %s" %(self.db))  
            tdSql.execute("create database %s tables 4" %(self.db))
            tdSql.execute("use %s" %(self.db))
            tdSql.execute("create table %s (ts timestamp, c1 bigint, stime timestamp) tags(tg1 bigint)" %(self.stb))
            while i < self.tbNum:
                tdSql.execute("create table tb%d using stb tags(%d)" % (i, i))
                i += 1
        except Exception as e:
            tdLog.info("Failed to create tb%d, exception: %s" %(i, str(e)))
            # tdDnodes.stopAll()
        finally:
            time.sleep(1)

        threads = []
        tdLog.info("================= continously insert records into %d tables, %d rows in each table" % (self.tbNum, self.rowsToImportPerTable)) 
        insertThread = threading.Thread(target=self._insert, name="insert-thread-1")
        insertThread.start()
        threads.append(insertThread)

        tdLog.info("================= import records into %d tables, %d rows in each table" % (self.tbNum, self.rowsToImportPerTable))    
        for t in range (1, self.importThreadNum + 1):
            threadName = "import-thread-%d" % (t)
            thread = threading.Thread(target=self._import, name=threadName, args=(t,))
            thread.start()
            threads.append(thread)

        queryThread1 = threading.Thread(target=self._selectFromSTable, name="query-thread-1")
        queryThread1.start()
        threads.append(queryThread1)
        queryThread2 = threading.Thread(target=self._selectAllFromTable, name="query-thread-2")
        queryThread2.start()
        threads.append(queryThread2)

        for t in range (len(threads)):
            threads[t].join()
            
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
    
tdCases.addCluster(__file__, TDTestCase())