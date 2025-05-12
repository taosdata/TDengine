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
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
import os
import threading
import time


class TDTestCase:
    """
    kill query
    """
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)


    def query(self):
        conn = taos.connect(host='127.0.0.1', user='root', password='taosdata', config='/etc/config')
        cursor = conn.cursor()
        while True:
            cursor.execute('show queries;')
            print('show queries!')
            temp = cursor.fetchall()
            if temp:
                print(temp[0][0])
                cursor.execute('kill query %s ;' % temp[0][0])
                print('kill query success')
                break
            time.sleep(0.5)
        cursor.close()
        conn.close()

    def run(self):
        tdSql.prepare()

        print("==============step1")
        os.system('yes | sudo taosdemo -n 100')
        print('insert into test.meters 10000000 rows')


        t1 = threading.Thread(target=self.query)
        t1.setDaemon(True)
        t1.start()

        print("==============step2")
        tdSql.execute('use test;')
        try:
            print('============begin select * from 10000000 rows')
            tdSql.query('select * from test.meters;')
            # print(tdSql.queryResult)
        except Exception as e:
            if not "ProgrammingError('Query terminated'" in str(e):
                raise Exception('fail')

        print('success')
        print('kill query success')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

