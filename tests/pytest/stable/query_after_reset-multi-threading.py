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
import random
import threading
import queue

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

current_tb = ""
last_tb = ""
written = 0


class Test (threading.Thread):
    def __init__(self, threadId, name, sleepTime, q):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.name = name
        self.sleepTime = sleepTime
        self.q = q

        self.threadLock = threading.Lock()

    def create_table(self):
        global current_tb
        global last_tb
        global written

        tdLog.info("create a table")
        current_tb = "tb%d" % int(round(time.time() * 1000))
        tdLog.info("current table %s" % current_tb)

        if (current_tb == last_tb):
            return
        else:
            tdSql.execute(
                'create table %s (ts timestamp, speed int)' %
                current_tb)
            last_tb = current_tb
            written = 0

    def insert_data(self):
        global current_tb
        global last_tb
        global written

        tdLog.info("will insert data to table")
        if (current_tb == ""):
            tdLog.info("no table, create first")
            self.create_table()

        tdLog.info("insert data to table")
        for i in range(0, 10):
            self.threadLock.acquire()
            insertRows = 1000
            tdLog.info("insert %d rows to %s" % (insertRows, current_tb))

            for j in range(0, insertRows):
                ret = tdSql.execute(
                    'insert into %s values (now + %dm, %d)' %
                    (current_tb, j, j))
                written = written + 1
            self.threadLock.release()

    def query_data(self):
        global current_tb
        global last_tb
        global written

        if (written > 0):
            tdLog.info("query data from table")
            tdSql.query("select * from %s" % last_tb)
            tdSql.checkRows(written)

    def query_stable(self):
        tdLog.sleep(10)
        self.threadLock.acquire()
        tdLog.info("query super table")

        try:
            tdSql.query("select * from st")
        except Exception as e:
            tdLog.info(repr(e))
            self.q.put(-1)
            tdLog.exit("failed")
        tdSql.checkRows(1)
        self.threadLock.release()

    def create_stable(self):
        global current_tb
        global last_tb
        global written

        self.threadLock.acquire()

        tdLog.info("create a super table and sub-table and insert data")

        try:
            tdSql.execute(
                "create table if not exists st (ts timestamp, tagtype int) tags(dev nchar(50))")
            tdSql.execute(
                'CREATE TABLE if not exists dev_001 using st tags("dev_01")')
            tdSql.execute(
                "INSERT INTO dev_001(ts, tagtype) VALUES('2020-05-13 10:00:00.000', 1)")
        except Exception as e:
            tdLog.info(repr(e))
            self.q.put(-1)
            tdLog.exit("failed")
        self.stop_database()
        self.delete_datafiles()
        self.restart_database()
        self.threadLock.release()

    def stop_database(self):
        tdLog.info("stop databae")
        tdDnodes.stop(1)

    def restart_database(self):
        global current_tb
        global last_tb
        global written

        tdLog.info("restart databae")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdLog.sleep(5)

    def force_restart(self):
        global current_tb
        global last_tb
        global written

        tdLog.info("force restart database")
        tdDnodes.forcestop(1)
        tdDnodes.start(1)
        tdLog.sleep(5)

    def drop_table(self):
        global current_tb
        global last_tb
        global written

        for i in range(0, 10):
            self.threadLock.acquire()

            tdLog.info("current_tb %s" % current_tb)

            if (current_tb != ""):
                tdLog.info("drop current tb %s" % current_tb)
                tdSql.execute("drop table %s" % current_tb)
                current_tb = ""
                last_tb = ""
                written = 0
            tdLog.sleep(self.sleepTime)
            self.threadLock.release()

    def reset_query_cache(self):
        global current_tb
        global last_tb
        global written

        tdLog.info("reset query cache")
        tdSql.execute("reset query cache")
        tdLog.sleep(1)

    def reset_database(self):
        global current_tb
        global last_tb
        global written

        tdLog.info("reset database")
        tdDnodes.forcestop(1)
        tdDnodes.deploy(1)
        current_tb = ""
        last_tb = ""
        written = 0
        tdDnodes.start(1)
        tdSql.prepare()

    def delete_datafiles(self):
        global current_tb
        global last_tb
        global written

        tdLog.info("delete data files")
        dnodesDir = tdDnodes.getDnodesRootDir()
        dataDir = dnodesDir + '/dnode1/*'
        deleteCmd = 'rm -rf %s' % dataDir
        os.system(deleteCmd)

        current_tb = ""
        last_tb = ""
        written = 0
        tdDnodes.start(1)
        tdSql.prepare()

    def run(self):
        switch = {
            1: self.create_table,
            2: self.insert_data,
            3: self.query_data,
            4: self.create_stable,
            5: self.restart_database,
            6: self.force_restart,
            7: self.drop_table,
            8: self.reset_query_cache,
            9: self.reset_database,
            10: self.delete_datafiles,
            11: self.query_stable,
        }

        switch.get(self.threadId, lambda: "ERROR")()


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        q = queue.Queue()
        test1 = Test(4, "create_stable", 1, q)
        test2 = Test(11, "query_stable", 2, q)

        test1.start()
        test2.start()
        test1.join()
        test2.join()

        while not q.empty():
            if (q.get() == -1):
                tdLog.exit("failed to end of test")

        tdLog.info("end of test")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
