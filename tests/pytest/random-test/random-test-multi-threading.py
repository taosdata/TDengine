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

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

last_tb = ""
last_stb = ""
written = 0
last_timestamp = 0


class Test (threading.Thread):
    def __init__(self, threadId, name):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.name = name

        self.threadLock = threading.Lock()

    def create_table(self):
        tdLog.info("create_table")
        global last_tb
        global written

        current_tb = "tb%d" % int(round(time.time() * 1000))

        if (current_tb == last_tb):
            return
        else:
            tdLog.info("will create table %s" % current_tb)

            try:
                tdSql.execute(
                    'create table %s (ts timestamp, speed int, c1 nchar(10))' %
                    current_tb)
                last_tb = current_tb
                written = 0
            except Exception as e:
                tdLog.info(repr(e))

    def insert_data(self):
        tdLog.info("insert_data")
        global last_tb
        global written
        global last_timestamp

        if (last_tb == ""):
            tdLog.info("no table, create first")
            self.create_table()

        start_time = 1500000000000

        tdLog.info("will insert data to table")
        for i in range(0, 10):
            insertRows = 1000
            tdLog.info("insert %d rows to %s" % (insertRows, last_tb))

            for j in range(0, insertRows):
                if (last_tb == ""):
                    tdLog.info("no table, return")
                    return
                tdSql.execute(
                    'insert into %s values (%d + %da, %d, "test")' %
                    (last_tb, start_time, last_timestamp, last_timestamp))
                written = written + 1
                last_timestamp = last_timestamp + 1

    def query_data(self):
        tdLog.info("query_data")
        global last_tb
        global written

        if (written > 0):
            tdLog.info("query data from table")
            tdSql.query("select * from %s" % last_tb)
            tdSql.checkRows(written)

    def create_stable(self):
        tdLog.info("create_stable")
        global last_tb
        global last_stb
        global written
        global last_timestamp

        current_stb = "stb%d" % int(round(time.time() * 1000))

        if (current_stb == last_stb):
            return
        else:
            tdLog.info("will create stable %s" % current_stb)
            tdSql.execute(
                'create table %s(ts timestamp, c1 int, c2 nchar(10)) tags (t1 int, t2 nchar(10))' %
                current_stb)
            last_stb = current_stb

            current_tb = "tb%d" % int(round(time.time() * 1000))
            tdSql.execute(
                "create table %s using %s tags (1, '表1')" %
                (current_tb, last_stb))
            last_tb = current_tb
            written = 0

            start_time = 1500000000000

            tdSql.execute(
                "insert into %s values (%d+%da, 27, '我是nchar字符串')" %
                (last_tb, start_time, last_timestamp))
            written = written + 1
            last_timestamp = last_timestamp + 1

    def drop_stable(self):
        tdLog.info("drop_stable")
        global last_stb

        if (last_stb == ""):
            tdLog.info("no super table")
            return
        else:
            tdLog.info("will drop last super table")
            tdSql.execute('drop table %s' % last_stb)
            last_stb = ""

    def restart_database(self):
        tdLog.info("restart_database")
        global last_tb
        global written

        tdDnodes.stop(1)
        tdDnodes.start(1)
#        tdLog.sleep(5)

    def force_restart_database(self):
        tdLog.info("force_restart_database")
        global last_tb
        global written

        tdDnodes.forcestop(1)
        tdDnodes.start(1)
#        tdLog.sleep(10)

    def drop_table(self):
        tdLog.info("drop_table")
        global last_tb
        global written

        for i in range(0, 10):
            if (last_tb != ""):
                tdLog.info("drop last_tb %s" % last_tb)
                tdSql.execute("drop table %s" % last_tb)
                last_tb = ""
                written = 0

    def query_data_from_stable(self):
        tdLog.info("query_data_from_stable")
        global last_stb

        if (last_stb == ""):
            tdLog.info("no super table")
            return
        else:
            tdLog.info("will query data from super table")
            tdSql.execute('select * from %s' % last_stb)

    def reset_query_cache(self):
        tdLog.info("reset_query_cache")
        global last_tb
        global written

        tdLog.info("reset query cache")
        tdSql.execute("reset query cache")
#        tdLog.sleep(1)

    def reset_database(self):
        tdLog.info("reset_database")
        global last_tb
        global last_stb
        global written

        tdDnodes.forcestop(1)
        tdDnodes.deploy(1)
        tdDnodes.start(1)
        tdSql.prepare()
        last_tb = ""
        last_stb = ""
        written = 0

    def delete_datafiles(self):
        tdLog.info("delete_data_files")
        global last_tb
        global last_stb
        global written

        dnodesDir = tdDnodes.getDnodesRootDir()
        dataDir = dnodesDir + '/dnode1/*'
        deleteCmd = 'rm -rf %s' % dataDir
        os.system(deleteCmd)

        tdDnodes.start(1)
#        tdLog.sleep(10)
        tdSql.prepare()
        last_tb = ""
        last_stb = ""
        written = 0

    def run(self):
        dataOp = {
            1: self.insert_data,
            2: self.query_data,
            3: self.query_data_from_stable,
        }

        dbOp = {
            1: self.create_table,
            2: self.create_stable,
            3: self.restart_database,
            4: self.force_restart_database,
            5: self.drop_table,
            6: self.reset_query_cache,
            7: self.reset_database,
            8: self.delete_datafiles,
            9: self.drop_stable,
        }

        if (self.threadId == 1):
            while True:
                self.threadLock.acquire()
                tdLog.notice("first thread")
                randDataOp = random.randint(1, 3)
                dataOp.get(randDataOp, lambda: "ERROR")()
                self.threadLock.release()

        elif (self.threadId == 2):
            while True:
                tdLog.notice("second thread")
                self.threadLock.acquire()
                randDbOp = random.randint(1, 9)
                dbOp.get(randDbOp, lambda: "ERROR")()
                self.threadLock.release()


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        test1 = Test(1, "data operation")
        test2 = Test(2, "db operation")

        test1.start()
        test2.start()
        test1.join()
        test2.join()

        tdLog.info("end of test")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
