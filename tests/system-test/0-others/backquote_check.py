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
from util.sqlset import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self.dbname = 'db'
        self.setsql = TDSetSql()
        self.stbname = 'stb'
        self.streamname = 'stm'
        self.streamtb = 'stm_stb'
    def topic_name_check(self):
        tdSql.execute(f'create database if not exists {self.dbname} wal_retention_period 3600')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create stable {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
        for name in [self.dbname,self.stbname]:
            type = ''
            if name == self.dbname:
                type = 'database'
            elif name == self.stbname:
                type = 'stable'
            tdSql.execute(f'create topic if not exists {name}  as {type} {name}')
            tdSql.query('show topics')
            tdSql.checkEqual(tdSql.queryResult[0][0],name)
            tdSql.execute(f'drop topic {name}')
            tdSql.execute(f'create topic if not exists `{name}` as {type} {name}')
            tdSql.query('show topics')
            tdSql.checkEqual(tdSql.queryResult[0][0],name)
            tdSql.execute(f'drop topic {name}')
            tdSql.execute(f'create topic if not exists `{name}` as {type} `{name}`')
            tdSql.query('show topics')
            tdSql.checkEqual(tdSql.queryResult[0][0],name)
            tdSql.execute(f'drop topic {name}')
            tdSql.execute(f'create topic if not exists `{name}` as {type} `{name}`')
            tdSql.query('show topics')
            tdSql.checkEqual(tdSql.queryResult[0][0],name)
            tdSql.execute(f'drop topic `{name}`')

    def db_name_check(self):
        tdSql.execute(f'create database if not exists `{self.dbname}` wal_retention_period 3600')
        tdSql.execute(f'use `{self.dbname}`')
        tdSql.execute(f'drop database {self.dbname}')
    
    def stream_name_check(self):
        tdSql.execute(f'create database if not exists {self.dbname} wal_retention_period 3600')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create stable {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute(f'create stream `{self.streamname}` into `{self.streamtb}` as select count(*) from {self.stbname} interval(10s);')
        tdSql.query('show streams')
        tdSql.checkEqual(tdSql.queryResult[0][0],self.streamname)
        tdSql.execute(f'drop stream {self.streamname}')
        tdSql.execute(f'drop stable {self.streamtb}')
        tdSql.execute(f'create stream {self.streamname} into `{self.streamtb}` as select count(*) from {self.stbname} interval(10s);')
        tdSql.query('show streams')
        tdSql.checkEqual(tdSql.queryResult[0][0],self.streamname)
        tdSql.execute(f'drop stream `{self.streamname}`')
        tdSql.execute(f'drop database {self.dbname}')
        
    def run(self):
        self.topic_name_check()
        self.db_name_check()
        self.stream_name_check()
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
