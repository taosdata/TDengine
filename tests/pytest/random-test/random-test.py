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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class Test:
    def __init__(self):
        self.last_tb = ""
        self.last_stb = ""
        self.written = 0

    def create_table(self):
        tdLog.info("create_table")
        current_tb = "tb%d" % int(round(time.time() * 1000))

        if (current_tb == self.last_tb):
            return
        else:
            tdLog.info("will create table %s" % current_tb)
            tdSql.execute(
                'create table %s (ts timestamp, c1 int, c2 nchar(10))' %
                current_tb)
            self.last_tb = current_tb
            self.written = 0

    def insert_data(self):
        tdLog.info("insert_data")
        if (self.last_tb == ""):
            tdLog.info("no table, create first")
            self.create_table()

        tdLog.info("will insert data to table")
        insertRows = 10
        tdLog.info("insert %d rows to %s" % (insertRows, self.last_tb))
        for i in range(0, insertRows):
            ret = tdSql.execute(
                'insert into %s values (now + %dm, %d, "%s")' %
                (self.last_tb, i, i, "->" + str(i)))
            self.written = self.written + 1

        tdLog.info("insert earlier data")
        tdSql.execute(
            'insert into %s values (now - 5m , 10, " - 5m")' %
            self.last_tb)
        self.written = self.written + 1
        tdSql.execute(
            'insert into %s values (now - 6m , 10, " - 6m")' %
            self.last_tb)
        self.written = self.written + 1
        tdSql.execute(
            'insert into %s values (now - 7m , 10, " - 7m")' %
            self.last_tb)
        self.written = self.written + 1
        tdSql.execute(
            'insert into %s values (now - 8m , 10, " - 8m")' %
            self.last_tb)
        self.written = self.written + 1

    def query_data(self):
        tdLog.info("query_data")
        if (self.written > 0):
            tdLog.info("query data from table")
            tdSql.query("select * from %s" % self.last_tb)
            tdSql.checkRows(self.written)

    def create_stable(self):
        tdLog.info("create_stable")
        current_stb = "stb%d" % int(round(time.time() * 1000))

        if (current_stb == self.last_stb):
            return
        else:
            tdLog.info("will create stable %s" % current_stb)
            tdSql.execute(
                'create table %s(ts timestamp, c1 int, c2 nchar(10)) tags (t1 int, t2 nchar(10))' %
                current_stb)
            self.last_stb = current_stb

            current_tb = "tb%d" % int(round(time.time() * 1000))
            tdSql.execute(
                "create table %s using %s tags (1, '表1')" %
                (current_tb, self.last_stb))
            self.last_tb = current_tb
            tdSql.execute(
                "insert into %s values (now, 27, '我是nchar字符串')" %
                self.last_tb)
            self.written = self.written + 1

    def drop_stable(self):
        tdLog.info("drop_stable")
        if (self.last_stb == ""):
            tdLog.info("no super table")
            return
        else:
            tdLog.info("will drop last super table")
            tdSql.execute('drop table %s' % self.last_stb)
            self.last_stb = ""

    def query_data_from_stable(self):
        tdLog.info("query_data_from_stable")
        if (self.last_stb == ""):
            tdLog.info("no super table")
            return
        else:
            tdLog.info("will query data from super table")
            tdSql.execute('select * from %s' % self.last_stb)

    def restart_database(self):
        tdLog.info("restart_databae")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdLog.sleep(5)

    def force_restart_database(self):
        tdLog.info("force_restart_database")
        tdDnodes.forcestop(1)
        tdDnodes.start(1)
        tdLog.sleep(5)
        tdSql.prepare()
        self.last_tb = ""
        self.last_stb = ""
        self.written = 0

    def drop_table(self):
        tdLog.info("drop_table")
        if (self.last_tb != ""):
            tdLog.info("drop last tb %s" % self.last_tb)
            tdSql.execute("drop table %s" % self.last_tb)
            self.last_tb = ""
            self.written = 0

    def reset_query_cache(self):
        tdLog.info("reset_query_cache")
        tdSql.execute("reset query cache")
        tdLog.sleep(1)

    def reset_database(self):
        tdLog.info("reset_database")
        tdDnodes.forcestop(1)
        tdDnodes.deploy(1)
        self.last_tb = ""
        self.written = 0
        tdDnodes.start(1)
        tdLog.sleep(5)
        tdSql.prepare()
        self.last_tb = ""
        self.last_stb = ""
        self.written = 0

    def delete_datafiles(self):
        tdLog.info("delete_datafiles")
        dnodesDir = tdDnodes.getDnodesRootDir()
        dataDir = dnodesDir + '/dnode1/*'
        deleteCmd = 'rm -rf %s' % dataDir
        os.system(deleteCmd)

        self.last_tb = ""
        self.last_stb = ""
        self.written = 0
        tdDnodes.start(1)
        tdLog.sleep(10)
        tdSql.prepare()
        self.last_tb = ""
        self.last_stb = ""
        self.written = 0


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        test = Test()

        switch = {
            1: test.create_table,
            2: test.insert_data,
            3: test.query_data,
            4: test.create_stable,
            5: test.restart_database,
            6: test.force_restart_database,
            7: test.drop_table,
            8: test.reset_query_cache,
            9: test.reset_database,
            10: test.delete_datafiles,
            11: test.query_data_from_stable,
            12: test.drop_stable,
        }

        for x in range(1, 1000):
            r = random.randint(1, 12)
            tdLog.notice("iteration %d run func %d" % (x, r))
            switch.get(r, lambda: "ERROR")()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
