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


from new_test_framework.utils import tdLog, tdSql
from new_test_framework.utils.common import tdCom
import os
import time

class TestDbTbNameCheck:
    
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql))
        cls.special_name = ['!','@','#','$','%','^','&','*','(',')','[',']','{','}',\
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

    def tb_name_len_check(self):
        dbname = tdCom.getLongName(10)
        tdSql.execute(f'create database if not exists `{dbname}` vgroups 1 replica 1')
        tdSql.execute(f'use `{dbname}`')
        tdSql.execute(f'CREATE STABLE `test_csv` (`ts` TIMESTAMP, `c1` VARCHAR(2000), `c2` VARCHAR(2000)) TAGS (`c3` VARCHAR(2000))')
        tbname = "test_csv_a12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012"
        tdSql.execute(f"INSERT INTO `{tbname}`\
                using `test_csv` (`c3`) tags('a12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890')\
                (`ts`,`c1`,`c2`) values(1591060628000,'a','1');")
        tdSql.query(f'select * from {tbname}')
        tdSql.checkRows(1)
        tdSql.execute(f'drop table {tbname}')

        tdSql.execute(f"INSERT INTO `{dbname}`.`{tbname}`\
                using `{dbname}`.`test_csv` (`c3`) tags('a12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890')\
                (`ts`,`c1`,`c2`) values(1591060628000,'a','1');")
        tdSql.query(f'select * from {tbname}')
        tdSql.checkRows(1)
        tdSql.execute(f'drop table {tbname}')
       
        tdSql.execute(f'trim database `{dbname}`')
        tdSql.execute(f'drop database `{dbname}`')

    def test_db_tb_name_check(self):
        """Name table

        1. Database name validation
        2. Table name validation

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_db_tb_name_check.py

        """
        self.db_name_check()
        self.tb_name_check()
        self.tb_name_len_check()
        
        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
