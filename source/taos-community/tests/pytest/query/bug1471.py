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
import time
import threading


class myThread(threading.Thread):
    def __init__(self, conn):
        threading.Thread.__init__(self)
        self.event = threading.Event()
        self.conn = taos.connect(conn._host, port=conn._port, config=conn._config)

    def run(self):
        cur = self.conn.cursor()
        self.event.wait()
        cur.execute("drop database db")
        cur.close()
        self.conn.close()


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):
        for i in range(50):
            print("round", i)
            thread = myThread(tdSql.cursor._connection)
            thread.start()

            tdSql.execute('reset query cache')
            tdSql.execute('drop database if exists db')
            tdSql.execute('create database db')
            tdSql.execute('use db')
            tdSql.execute("create table car (ts timestamp, s int)")
            tdSql.execute("insert into car values('2020-10-19 17:00:00', 123)")

            thread.event.set()
            try:
                tdSql.query("select s from car where ts = '2020-10-19 17:00:00'")
            except Exception as e:
                pass
            else:
                tdSql.checkData(0, 0, 123)

            thread.join()
            time.sleep(0.2)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
