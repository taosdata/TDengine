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
from multiprocessing import  Process

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1538548685000

    def updateMetadata(self):
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = tdDnodes.getSimCfgPath()

        self.conn = taos.connect(host = self.host, user = self.user, password = self.password, config = self.config)
        self.cursor = self.conn.cursor()    
        self.cursor.execute("alter table db.tb add column col2 int")
        print("alter table done")

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute("create table if not exists tb (ts timestamp, col1 int)")
        tdSql.execute("insert into tb values(%d, 1)" % self.ts)

        print("==============step2")
        tdSql.query("select * from tb")
        tdSql.checkRows(1)

        p = Process(target=self.updateMetadata, args=())
        p.start()        
        p.join()
        p.terminate()
        
        tdSql.execute("insert into tb values(%d, 1, 2)" % (self.ts + 1))
        tdSql.execute("insert into tb(ts, col1, col2) values(%d, 1, 2)" % (self.ts + 2))

        print("==============step2")
        tdSql.query("select * from tb")
        tdSql.checkRows(3)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
