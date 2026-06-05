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
from threading import Thread, Event

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

last_tb = ""
last_stb = ""
written = 0
last_timestamp = 0
colAdded = False
killed = False


class Test (Thread):
    def __init__(self, threadId, name, events, q):
        Thread.__init__(self)
        self.threadId = threadId
        self.name = name
        self.dataEvent, self.dbEvent, self.queryEvent = events
        self.q = q

    def create_table(self):
        tdLog.info("create_table")
        global last_tb
        global written
        global killed

        current_tb = "tb%d" % int(round(time.time() * 1000))

        if (current_tb == last_tb):
            return
        else:
            tdLog.info("will create table %s" % current_tb)

            try:
                tdSql.execute(
                    'create table %s (ts timestamp, speed int, c2 nchar(10))' %
                    current_tb)
                last_tb = current_tb
                written = 0
                killed = False
            except Exception as e:
                tdLog.info("killed: %d error: %s" % (killed, e.args[0]))
                if killed and (e.args[0] == 'network unavailable'):
                    tdLog.info("database killed, expect failed")
                    return 0
                return -1
        return 0

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
                    return 0
                try:
                    tdSql.execute(
                        'insert into %s values (%d + %da, %d, "test")' %
                        (last_tb, start_time, last_timestamp, last_timestamp))
                    written = written + 1
                    last_timestamp = last_timestamp + 1
                except Exception as e:
                    if killed:
                        tdLog.info(
                            "database killed, expect failed %s" %
                            e.args[0])
                        return 0
                    tdLog.info(repr(e))
                    return -1
        return 0

    def query_data(self):
        tdLog.info("query_data")
        global last_tb
        global killed

        if not killed and last_tb != "":
            tdLog.info("query data from table")
            tdSql.query("select * from %s" % last_tb)
            tdSql.checkRows(written)
        return 0

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
        return 0

    def drop_stable(self):
        tdLog.info("drop_stable")
        global last_stb
        global last_tb
        global written

        if (last_stb == ""):
            tdLog.info("no super table")
            return
        else:
            tdLog.info("will drop last super table")
            tdSql.execute('drop table %s' % last_stb)
            last_stb = ""
            last_tb = ""
            written = 0
        return 0

    def alter_table_to_add_col(self):
        tdLog.info("alter_table_to_add_col")
        global last_stb
        global colAdded

        if last_stb != "" and colAdded == False:
            tdSql.execute(
                "alter table %s add column col binary(20)" %
                last_stb)
            colAdded = True
        return 0

    def alter_table_to_drop_col(self):
        tdLog.info("alter_table_to_drop_col")
        global last_stb
        global colAdded

        if last_stb != "" and colAdded:
            tdSql.execute("alter table %s drop column col" % last_stb)
            colAdded = False
        return 0

    def restart_database(self):
        tdLog.info("restart_database")
        global last_tb
        global written
        global killed

        tdDnodes.stop(1)
        killed = True
        tdDnodes.start(1)
        tdLog.sleep(10)
        killed = False
        return 0

    def force_restart_database(self):
        tdLog.info("force_restart_database")
        global last_tb
        global written
        global killed

        tdDnodes.forcestop(1)
        last_tb = ""
        written = 0
        killed = True
        tdDnodes.start(1)
#        tdLog.sleep(10)
        killed = False
        return 0

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
        return 0

    def query_data_from_stable(self):
        tdLog.info("query_data_from_stable")
        global last_stb

        if (last_stb == ""):
            tdLog.info("no super table")
            return
        else:
            tdLog.info("will query data from super table")
            tdSql.execute('select * from %s' % last_stb)
        return 0

    def reset_query_cache(self):
        tdLog.info("reset_query_cache")
        global last_tb
        global written

        tdLog.info("reset query cache")
        tdSql.execute("reset query cache")
        tdLog.sleep(1)
        return 0

    def reset_database(self):
        tdLog.info("reset_database")
        global last_tb
        global last_stb
        global written
        global killed

        tdDnodes.forcestop(1)
        killed = True
        tdDnodes.deploy(1)
        tdDnodes.start(1)
        tdSql.prepare()
        killed = False
        return 0

    def delete_datafiles(self):
        tdLog.info("delete_data_files")
        global last_tb
        global last_stb
        global written
        global killed

        dnodesDir = tdDnodes.getDnodesRootDir()
        tdDnodes.forcestop(1)
        killed = True
        dataDir = dnodesDir + '/dnode1/data/*'
        deleteCmd = 'rm -rf %s' % dataDir
        os.system(deleteCmd)
        last_tb = ""
        last_stb = ""
        written = 0

        tdDnodes.start(1)
        tdSql.prepare()
        killed = False
        return 0

    def run(self):
        dataOp = {
            1: self.insert_data,
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
            10: self.alter_table_to_add_col,
            11: self.alter_table_to_drop_col,
        }

        queryOp = {
            1: self.query_data,
            2: self.query_data_from_stable,
        }

        if (self.threadId == 1):
            while True:
                self.dataEvent.wait()
                tdLog.notice("first thread")
                randDataOp = random.randint(1, 1)
                ret1 = dataOp.get(randDataOp, lambda: "ERROR")()

                if ret1 == -1:
                    self.q.put(-1)
                    tdLog.exit("first thread failed")
                else:
                    self.q.put(1)

                if (self.q.get() != -2):
                    self.dataEvent.clear()
                    self.queryEvent.clear()
                    self.dbEvent.set()
                else:
                    self.q.put(-1)
                    tdLog.exit("second thread failed, first thread exit too")

        elif (self.threadId == 2):
            while True:
                self.dbEvent.wait()
                tdLog.notice("second thread")
                randDbOp = random.randint(1, 11)
                dbOp.get(randDbOp, lambda: "ERROR")()
                self.dbEvent.clear()
                self.dataEvent.clear()
                self.queryEvent.set()

        elif (self.threadId == 3):
            while True:
                self.queryEvent.wait()
                tdLog.notice("third thread")
                randQueryOp = random.randint(1, 2)
                queryOp.get(randQueryOp, lambda: "ERROR")()
                self.queryEvent.clear()
                self.dbEvent.clear()
                self.dataEvent.set()


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        events = [Event() for _ in range(3)]
        events[0].set()
        events[1].clear()
        events[1].clear()

        test1 = Test(1, "data operation", events)
        test2 = Test(2, "db operation", events)
        test3 = Test(3, "query operation", events)

        test1.start()
        test2.start()
        test3.start()

        test1.join()
        test2.join()
        test3.join()

        while not q.empty():
            if (q.get() != 0):
                tdLog.exit("failed to end of test")

        tdLog.info("end of test")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
