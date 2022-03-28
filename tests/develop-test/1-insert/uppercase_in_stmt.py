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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
from taos import *


class TDTestCase:
    def caseDescription(self):
        '''
        case1<slzhou>: [TD-12977] fix invalid upper case table name of stmt api
        ''' 
        return
    
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn
        self._dbname = "TD12977"

    def run(self):
        tdSql.prepare()

        self._conn.execute("drop database if exists %s" % self._dbname)
        self._conn.execute("create database if not exists %s" % self._dbname)
        self._conn.select_db(self._dbname)

        self._conn.execute("create stable STB(ts timestamp, n int) tags(b int)")

        stmt = self._conn.statement("insert into ? using STB tags(?) values(?, ?)")
        params = new_bind_params(1)
        params[0].int(4);
        stmt.set_tbname_tags("ct", params);

        multi_params = new_multi_binds(2);
        multi_params[0].timestamp([1626861392589, 1626861392590])
        multi_params[1].int([123,456])
        stmt.bind_param_batch(multi_params)
        
        stmt.execute()

        tdSql.query("select * from stb")
        tdSql.checkRows(2)
        stmt.close()
                
    
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
