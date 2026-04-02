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
        tdSql.execute('create dnode 192.168.0.2')
        tdDnodes.deploy(2)
        tdDnodes.start(2)
        tdSql.execute('create dnode 192.168.0.3')
        tdDnodes.deploy(3)
        tdDnodes.start(3)
        time.sleep(3)

        self.db = "db"
        self.stb = "stb"
        self.tbPrefix = "tb"
        self.tbNum = 100000
        self.count = 0
        # self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
        self.threadNum = 1
        # threadLock = threading.Lock()
        # global counter for number of tables created by all threads
        self.global_counter = 0

        tdSql.init(self.conn.cursor())

    def _createTable(self, threadId):
        print("Thread%d : createTable" % (threadId))
        conn = taos.connect(config=tdDnodes.getSimCfgPath())
        cursor = conn.cursor()
        i = 0
        try:
            sql = "use %s" % (self.db)
            cursor.execute(sql)
            while i < self.tbNum:
                if (i % self.threadNum == threadId):
                    cursor.execute(
                        "create table tb%d using %s tags(%d)" %
                        (i + 1, self.stb, i + 1))
                    with threading.Lock():
                        self.global_counter += 1
                    time.sleep(0.01)
                i += 1
        except Exception as e:
            tdLog.info(
                "Failure when creating table tb%d, exception: %s" %
                (i + 1, str(e)))
        finally:
            cursor.close()
            conn.close()

    def _interfereDnodes(self, threadId, dnodeId):
        # interfere dnode while creating table
        print("Thread%d to interfere dnode%d" % (threadId, dnodeId))
        percent = 0.05
        loop = int(1 / (2 * percent))
        for t in range(1, loop):
            while self.global_counter < self.tbNum * (t * percent):
                time.sleep(0.2)
            tdDnodes.forcestop(dnodeId)
            while self.global_counter < self.tbNum * ((t + 1) * percent):
                time.sleep(0.2)
            tdDnodes.start(dnodeId)

        # while self.global_counter < self.tbNum * 0.05:
        #     time.sleep(0.2)
        # tdDnodes.forcestop(dnodeId)
        # while self.global_counter < self.tbNum * 0.10:
        #     time.sleep(0.2)
        # tdDnodes.start(dnodeId)
        # while self.global_counter < self.tbNum * 0.15:
        #     time.sleep(0.2)
        # tdDnodes.forcestop(dnodeId)
        # while self.global_counter < self.tbNum * 0.20:
        #     time.sleep(0.2)
        # tdDnodes.start(dnodeId)
        # while self.global_counter < self.tbNum * 0.25:
        #     time.sleep(0.2)
        # tdDnodes.forcestop(dnodeId)
        # while self.global_counter < self.tbNum * 0.30:
        #     time.sleep(0.2)
        # tdDnodes.start(dnodeId)
        # while self.global_counter < self.tbNum * 0.35:
        #     time.sleep(0.2)
        # tdDnodes.forcestop(dnodeId)
        # while self.global_counter < self.tbNum * 0.40:
        #     time.sleep(0.2)
        # tdDnodes.start(dnodeId)
        # while self.global_counter < self.tbNum * 0.45:
        #     time.sleep(0.2)
        # tdDnodes.forcestop(dnodeId)
        # while self.global_counter < self.tbNum * 0.50:
        #     time.sleep(0.2)
        # tdDnodes.start(dnodeId)

    def run(self):
        tdLog.info("================= creating database with replica 2")
        threadId = 0
        threads = []
        try:
            tdSql.execute("drop database if exists %s" % (self.db))
            tdSql.execute(
                "create database %s replica 2 cache 1024 ablocks 2.0 tblocks 4 tables 1000" %
                (self.db))
            tdLog.sleep(3)
            tdSql.execute("use %s" % (self.db))
            tdSql.execute(
                "create table %s (ts timestamp, c1 bigint, stime timestamp) tags(tg1 bigint)" %
                (self.stb))
            tdLog.info("Start to create tables")
            while threadId < self.threadNum:
                tdLog.info("Thread-%d starts to create tables" % (threadId))
                cThread = threading.Thread(
                    target=self._createTable,
                    name="thread-%d" %
                    (threadId),
                    args=(
                        threadId,
                    ))
                cThread.start()
                threads.append(cThread)
                threadId += 1

        except Exception as e:
            tdLog.info("Failed to create tb%d, exception: %s" % (i, str(e)))
            # tdDnodes.stopAll()
        finally:
            time.sleep(1)

        threading.Thread(
            target=self._interfereDnodes,
            name="thread-interfereDnode%d" %
            (3),
            args=(
                1,
                3,
            )).start()
        for t in range(len(threads)):
            tdLog.info("Join threads")
            # threads[t].start()
            threads[t].join()

        tdSql.query("show stables")
        tdSql.checkData(0, 4, self.tbNum)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addCluster(__file__, TDTestCase())
