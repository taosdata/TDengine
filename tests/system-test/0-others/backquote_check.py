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
        self.ntbname1 = 'ntb1'
        self.ntbname2 = 'ntb2'
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
            tdSql.checkData(0, 0, name)
            tdSql.execute(f'drop topic {name}')
            tdSql.execute(f'create topic if not exists `{name}` as {type} {name}')
            tdSql.query('show topics')
            tdSql.checkData(0, 0, name)
            tdSql.execute(f'drop topic {name}')
            tdSql.execute(f'create topic if not exists `{name}` as {type} `{name}`')
            tdSql.query('show topics')
            tdSql.checkData(0, 0, name)
            tdSql.execute(f'drop topic {name}')
            tdSql.execute(f'create topic if not exists `{name}` as {type} `{name}`')
            tdSql.query('show topics')
            tdSql.checkData(0, 0, name)
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

        time.sleep(15)
        tdSql.query('show streams')
        tdSql.checkData(0, 0, self.streamname)
        tdSql.execute(f'drop stream {self.streamname}')
        tdSql.execute(f'drop stable {self.streamtb}')
        tdSql.execute(f'create stream {self.streamname} into `{self.streamtb}` as select count(*) from {self.stbname} interval(10s);')

        time.sleep(10)
        tdSql.query('show streams')
        tdSql.checkData(0, 0, self.streamname)
        tdSql.execute(f'drop stream `{self.streamname}`')
        tdSql.execute(f'drop database {self.dbname}')

    def table_name_check(self):
        tdSql.execute(f'create database if not exists {self.dbname} wal_retention_period 3600')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table {self.ntbname1} (ts timestamp,c0 int,c1 int)')
        tdSql.execute(f'create table {self.ntbname2} (ts timestamp,c0 int,c1 int)')
        tdSql.execute(f'insert into {self.ntbname1} values(now(),1,1)')
        tdSql.execute(f'insert into {self.ntbname2} values(now(),2,2)')
        tdSql.query(f'select `{self.ntbname1}`.`c0`, `{self.ntbname1}`.`c1` from `{self.ntbname1}`')
        tdSql.checkData(0, 0, 1)
        tdSql.query(f'select `{self.ntbname1}`.`c0`, `{self.ntbname1}`.`c1` from `{self.dbname}`.`{self.ntbname1}`')
        tdSql.checkData(0, 0, 1)
        tdSql.query(f'select `{self.ntbname1}`.`c0` from `{self.ntbname2}` `{self.ntbname1}`')
        tdSql.checkData(0, 0, 2)
        tdSql.query(f'select `{self.ntbname1}`.`c0` from (select * from `{self.ntbname2}`) `{self.ntbname1}`')
        tdSql.checkData(0, 0, 2)
        # select `t1`.`col1`, `col2`, `col3` from (select ts `col1`, 123 `col2`, c0 + c1 as `col3` from t2) `t1`;
        tdSql.query(f'select `{self.ntbname1}`.`col1`, `col2`, `col3` from (select ts `col1`, 123 `col2`, c0 + c1 as `col3` from {self.ntbname2}) `{self.ntbname1}`')
        tdSql.checkData(0, 1, 123)
        tdSql.checkData(0, 2, 4)

        # tdSql.execute(f'drop database {self.dbname}')

    def view_name_check(self):
        """Cover the view name with backquote"""
        tdSql.execute(f'create view `v1` as select * from (select `ts`, `c0` from ntb1) `t1`;')
        tdSql.query('select * from `v1`;')
        tdSql.checkRows(1)
        tdSql.execute(f'drop view `v1`;')

    def query_check(self):
        """Cover the table name, column name with backquote in query statement"""
        tdSql.query(f'select `t1`.`ts`, `t1`.`c0` + 2 as `c1` from `{self.ntbname1}` `t1` union select `t2`.`ts`, `t2`.`c0` from `{self.ntbname2}` `t2`')
        tdSql.checkRows(2)

        tdSql.query(f'select `t1`.`ts`, `t1`.`c0` + `t2`.`c0` as `c0`, `t1`.`c1` * `t2`.`c1` as `c1` from `{self.ntbname1}` `t1` join `{self.ntbname2}` `t2` on timetruncate(`t1`.`ts`, 1s) = timetruncate(`t2`.`ts`, 1s);')
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        tdSql.query(f'select `t1`.`ts`, `t1`.`c1`, `t1`.`c2` from (select `ts`, `c0` + 1 as `c1`, `c1` + 2 as `c2` from `{self.ntbname1}`) `t1`;')
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 3)
        
        tdSql.query(f'select `t`.`ts`, cast(`t`.`v1` as int) + `t`.`c0` as `v` from (select `ts`, "12" as `v1`, `c0`, `c1` from `ntb1`) `t`;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 13)

        tdSql.query(f'select count(`t1`.`ts`) from (select `t`.`ts` from `{self.ntbname1}` `t`) `t1`;')
        tdSql.checkRows(1)

    def run(self):
        self.topic_name_check()
        self.db_name_check()
        if platform.system().lower() == 'windows':
            self.stream_name_check()
        self.table_name_check()
        self.view_name_check()
        self.query_check()

    def stop(self):
        tdSql.execute(f'drop database {self.dbname}')
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
