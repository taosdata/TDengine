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
        self.current_tb = ""
        self.last_tb = ""
        self.written = 0

    def create_table(self):
        tdLog.info("create a table")
        self.current_tb = "tb%d" % int(round(time.time() * 1000))
        tdLog.info("current table %s" % self.current_tb)

        if (self.current_tb == self.last_tb):
            return
        else:
            tdSql.execute(
                'create table %s (ts timestamp, speed int)' %
                self.current_tb)
            self.last_tb = self.current_tb
            self.written = 0

    def insert_data(self):
        tdLog.info("will insert data to table")
        if (self.current_tb == ""):
            tdLog.info("no table, create first")
            self.create_table()

        tdLog.info("insert data to table")
        insertRows = 10
        tdLog.info("insert %d rows to %s" % (insertRows, self.last_tb))
        for i in range(0, insertRows):
            ret = tdSql.execute(
                'insert into %s values (now + %dm, %d)' %
                (self.last_tb, i, i))
            self.written = self.written + 1

        tdLog.info("insert earlier data")
        tdSql.execute('insert into %s values (now - 5m , 10)' % self.last_tb)
        self.written = self.written + 1
        tdSql.execute('insert into %s values (now - 6m , 10)' % self.last_tb)
        self.written = self.written + 1
        tdSql.execute('insert into %s values (now - 7m , 10)' % self.last_tb)
        self.written = self.written + 1
        tdSql.execute('insert into %s values (now - 8m , 10)' % self.last_tb)
        self.written = self.written + 1

    def query_data(self):
        if (self.written > 0):
            tdLog.info("query data from table")
            tdSql.query("select * from %s" % self.last_tb)
            tdSql.checkRows(self.written)

    def query_stable(self):
        tdLog.info("query super table")
        try:
            tdSql.query("select * from st")
        except Exception as e:
            tdLog.info("Exception catched: %s" % repr(e))
            if ('Table does not exist' not in repr(e)):
                raise Exception(repr(e))

    def create_stable(self):
        tdLog.info("create a super table and sub-table and insert data")
        tdSql.execute(
            "create table if not exists st (ts timestamp, tagtype int) tags(dev nchar(50))")
        tdSql.execute(
            'CREATE TABLE if not exists dev_001 using st tags("dev_01")')
        tdSql.execute(
            "INSERT INTO dev_001(ts, tagtype) VALUES('2020-05-13 10:00:00.000', 1)")

    def stop_database(self):
        tdLog.info("stop databae")
        tdDnodes.stop(1)

    def restart_database(self):
        tdLog.info("restart databae")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdLog.sleep(5)

    def force_restart(self):
        tdLog.info("force restart database")
        tdDnodes.forcestop(1)
        tdDnodes.start(1)
        tdLog.sleep(5)

    def drop_table(self):
        if (self.current_tb != ""):
            tdLog.info("drop current tb %s" % self.current_tb)
            tdSql.execute("drop table %s" % self.current_tb)
            self.current_tb = ""
            self.last_tb = ""
            self.written = 0

    def reset_query_cache(self):
        tdLog.info("reset query cache")
        tdSql.execute("reset query cache")
        tdLog.sleep(1)

    def reset_database(self):
        tdLog.info("reset database")
        tdDnodes.forcestop(1)
        tdDnodes.deploy(1)
        self.current_tb = ""
        self.last_tb = ""
        self.written = 0
        tdDnodes.start(1)
        tdSql.prepare()

    def delete_datafiles(self):
        tdLog.info("delete data files")
        dnodesDir = tdDnodes.getDnodesRootDir()
        dataDir = dnodesDir + '/dnode1/data/*'
        deleteCmd = 'rm -rf %s' % dataDir
        os.system(deleteCmd)

        self.current_tb = ""
        self.last_tb = ""
        self.written = 0
        tdDnodes.start(1)
        tdSql.prepare()


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
            6: test.force_restart,
            7: test.drop_table,
            8: test.reset_query_cache,
            9: test.reset_database,
            10: test.delete_datafiles,
            11: test.query_stable,
            12: test.stop_database,
        }

        tdLog.info("create stable")
        switch.get(4, lambda: "ERROR")()
        tdLog.info("stop database")
        switch.get(12, lambda: "ERROR")()
        tdLog.info("delete datafiles")
        switch.get(10, lambda: "ERROR")()
        tdLog.info("restart database")
        switch.get(5, lambda: "ERROR")()
        tdLog.info("query stable")
        switch.get(11, lambda: "ERROR")()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
