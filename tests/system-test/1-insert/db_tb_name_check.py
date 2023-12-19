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
    
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.special_name = ['!','@','#','$','%','^','&','*','(',')','[',']','{','}',\
                            ':',';','\'','\"',',','<','>','/','?','-','_','+','=','~']
    def db_name_check(self):
        dbname = tdCom.getLongName(10)
        for j in self.special_name:
            for i in range(len(list(dbname))+1):
                new_dbname = list(dbname)
                new_dbname.insert(i,j)
                dbname_1 = ''.join(new_dbname)
                tdSql.execute(f'create database if not exists `{dbname_1}`  vgroups 1 replica 1')
                tdSql.query('select * from information_schema.ins_databases')
                tdSql.checkEqual(tdSql.queryResult[2][0],str(dbname_1))
                tdSql.execute(f'drop database `{dbname_1}`')
        for i in range(len(list(dbname))+1):
            new_dbname = list(dbname)
            new_dbname.insert(i,'.')
            dbname_1 = ''.join(new_dbname)
            tdSql.error(f'create database if not exists `{dbname_1}`')

    def tb_name_check(self):
        dbname = tdCom.getLongName(10)
        tdSql.execute(f'create database if not exists `{dbname}` vgroups 1 replica 1')
        tdSql.execute(f'use `{dbname}`')
        tbname = tdCom.getLongName(5)
        for i in self.special_name:
            for j in range(len(list(tbname))+1):
                tbname1 = list(tbname)
                tbname1.insert(j,i)
                new_tbname = ''.join(tbname1)
                for sql in [f'`{dbname}`.`{new_tbname}`',f'`{new_tbname}`']:
                    tdSql.execute(f'create table {sql} (ts timestamp,c0 int)')
                    tdSql.execute(f'insert into {sql} values(now,1)')
                    tdSql.query(f'select * from {sql}')
                    tdSql.checkRows(1)
                    tdSql.execute(f'drop table {sql}')
        for i in range(len(list(tbname))+1):
            tbname1 = list(tbname)
            tbname1.insert(i,'.')
            new_tbname = ''.join(tbname1)
            for sql in [f'`{dbname}`.`{new_tbname}`',f'`{new_tbname}`']:
                tdSql.error(f'create table {sql} (ts timestamp,c0 int)')
        tdSql.execute(f'trim database `{dbname}`')
        tdSql.execute(f'drop database `{dbname}`')
    def run(self):
        self.db_name_check()
        self.tb_name_check()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
