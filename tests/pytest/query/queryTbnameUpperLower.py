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
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.common import tdCom

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def checkStbWhereIn(self):
        '''
            where in ---> upper lower mixed
        '''
        tdCom.cleanTb()
        table_name = tdCom.getLongName(8, "letters_mixed")
        table_name_sub = f'{table_name}_sub'
        tb_name_lower = table_name_sub.lower()
        tb_name_upper = table_name_sub.upper()

        ## create stb and tb
        tdSql.execute(f'CREATE TABLE {table_name} (ts timestamp, id int, bi1 binary(20)) tags (si1 binary(20))')
        tdSql.execute(f'create table {table_name_sub}1 using {table_name} tags ("{table_name_sub}1")')
        tdSql.execute(f'create table {tb_name_lower}2 using {table_name} tags ("{tb_name_lower}2")')
        tdSql.execute(f'create table {tb_name_upper}3 using {table_name} tags ("{tb_name_upper}3")')

        ## insert values
        tdSql.execute(f'insert into {table_name_sub}1 values (now-1s, 1, "{table_name_sub}1")')
        tdSql.execute(f'insert into {tb_name_lower}2 values (now-2s, 2, "{tb_name_lower}21")')
        tdSql.execute(f'insert into {tb_name_lower}2 values (now-3s, 3, "{tb_name_lower}22")')
        tdSql.execute(f'insert into {tb_name_upper}3 values (now-4s, 4, "{tb_name_upper}31")')
        tdSql.execute(f'insert into {tb_name_upper}3 values (now-5s, 5, "{tb_name_upper}32")')
        tdSql.execute(f'insert into {tb_name_upper}3 values (now-6s, 6, "{tb_name_upper}33")')

        ## query where tbname in single
        tdSql.query(f'select * from {table_name} where tbname in ("{table_name_sub}1")')
        tdSql.checkRows(1)
        tdSql.query(f'select * from {table_name} where tbname in ("{table_name_sub.upper()}1")')
        tdSql.checkRows(1)
        tdSql.query(f'select * from {table_name} where tbname in ("{table_name_sub.lower()}1")')
        tdSql.checkRows(1)
        tdSql.query(f'select * from {table_name} where tbname in ("{tb_name_lower}2")')
        tdSql.checkRows(2)
        tdSql.query(f'select * from {table_name} where tbname in ("{tb_name_lower.upper()}2")')
        tdSql.checkRows(2)
        tdSql.query(f'select * from {table_name} where tbname in ("{tb_name_upper}3")')
        tdSql.checkRows(3)
        tdSql.query(f'select * from {table_name} where tbname in ("{tb_name_upper.lower()}3")')
        tdSql.checkRows(3)
        
        ## query where tbname in multi
        tdSql.query(f'select * from {table_name} where id=5 and tbname in ("{table_name_sub}1", "{tb_name_lower.upper()}2", "{tb_name_upper.lower()}3")')
        tdSql.checkRows(1)
        tdSql.query(f'select * from {table_name} where tbname in ("{table_name_sub}1", "{tb_name_lower.upper()}2", "{tb_name_upper.lower()}3")')
        tdSql.checkRows(6)

    def run(self):
        tdSql.prepare()
        self.checkStbWhereIn()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())