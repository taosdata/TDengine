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

from util.log import *
from util.cases import *
from util.sql import *
from util.common import *

class TDTestCase:
    updatecfgDict = {'ttlUnit':5,'ttlPushInterval':3}
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())      
        self.ntbname = 'ntb'
        self.stbname = 'stb'
        self.tbnum = 10
        self.ttl_param = 1
        self.default_ttl = 100
        self.modify_ttl = 1
    def ttl_check_ntb(self):
        tdSql.prepare()

        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.ntbname}_{i} (ts timestamp,c0 int) ttl {self.ttl_param}')
        tdSql.query(f'show tables')
        tdSql.checkRows(self.tbnum)
        sleep(self.updatecfgDict['ttlUnit']*self.ttl_param+self.updatecfgDict['ttlPushInterval'])
        tdSql.query(f'show tables')
        tdSql.checkRows(0)
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.ntbname}_{i} (ts timestamp,c0 int) ttl {self.default_ttl}')
        for i in range(int(self.tbnum/2)):
            tdSql.execute(f'alter table {self.ntbname}_{i} ttl {self.modify_ttl}')
        sleep(self.updatecfgDict['ttlUnit']*self.modify_ttl+self.updatecfgDict['ttlPushInterval'])
        tdSql.query(f'show tables')
        tdSql.checkRows(self.tbnum - int(self.tbnum/2))
        tdSql.execute('drop database db')
    def ttl_check_ctb(self):
        tdSql.prepare()
        tdSql.execute(f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int)')

        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({i}) ttl {self.ttl_param}')
        tdSql.query(f'show tables')
        tdSql.checkRows(self.tbnum)
        sleep(self.updatecfgDict['ttlUnit']*self.ttl_param+self.updatecfgDict['ttlPushInterval'])
        tdSql.query(f'show tables')
        tdSql.checkRows(0)
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({i}) ttl {self.default_ttl}')
        tdSql.query(f'show tables')
        tdSql.checkRows(self.tbnum)
        for i in range(int(self.tbnum/2)):
            tdSql.execute(f'alter table {self.stbname}_{i} ttl {self.modify_ttl}')
        sleep(self.updatecfgDict['ttlUnit']*self.modify_ttl+self.updatecfgDict['ttlPushInterval'])
        tdSql.query(f'show tables')
        tdSql.checkRows(self.tbnum - int(self.tbnum/2))
        tdSql.execute('drop database db')

    def run(self):
        self.ttl_check_ntb()
        self.ttl_check_ctb()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
