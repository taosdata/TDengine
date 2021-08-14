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
from copy import deepcopy
import string
import random
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def cleanTb(self):
        query_sql = "show stables"
        res_row_list = tdSql.query(query_sql, True)
        stb_list = map(lambda x: x[0], res_row_list)
        for stb in stb_list:
            tdSql.execute(f'drop table if exists {stb}')

        query_sql = "show tables"
        res_row_list = tdSql.query(query_sql, True)
        tb_list = map(lambda x: x[0], res_row_list)
        for tb in tb_list:
            tdSql.execute(f'drop table if exists {tb}')

    def getLongWildcardStr(self, len=None):
        """
            generate long wildcard str
        """    
        maxWildCardsLength = int(tdSql.getVariable('maxWildCardsLength')[0])
        if len:
            chars = ''.join(random.choice(string.ascii_letters.lower()) for i in range(len))
        else:
            chars = ''.join(random.choice(string.ascii_letters.lower()) for i in range(maxWildCardsLength+1))
        return chars
    
    def genTableName(self):
        '''
            generate table name
            hp_name--->'%str'
            lp_name--->'str%'
            ul_name--->'st_r'
        '''
        table_name = self.getLongWildcardStr()
        table_name_list = list(table_name)
        table_name_list.pop(-1)

        if len(table_name_list) > 1:
            lp_name = deepcopy(table_name_list)
            lp_name[-1] = '%'
            lp_name = ''.join(lp_name)

            ul_name = list(lp_name)
            ul_name[int(len(ul_name)/2)] = '_'
            ul_name = ''.join(ul_name)

            table_name_list = list(table_name)
            hp_name = deepcopy(table_name_list)
            hp_name.pop(1)
            hp_name[0] = '%'
            hp_name = ''.join(hp_name)
        else:
            hp_name = '%'
            lp_name = '%'
            ul_name = '_'
        return table_name, hp_name, lp_name, ul_name

    def checkRegularTableWildcardLength(self):
        '''
            check regular table wildcard length with % and _
        '''
        self.cleanTb()
        table_name, hp_name, lp_name, ul_name = self.genTableName()
        tdSql.execute(f"CREATE TABLE {table_name} (ts timestamp, a1 int)")
        sql_list = [f'show tables like "{hp_name}"', f'show tables like "{lp_name}"', f'show tables like "{ul_name}"']
        for sql in sql_list:
            tdSql.query(sql)
            if len(table_name) >= 1:
                tdSql.checkRows(1)
            else:
                tdSql.error(sql)
        
        exceed_sql_list = [f'show tables like "%{hp_name}"', f'show tables like "{lp_name}%"', f'show tables like "{ul_name}%"']
        for sql in exceed_sql_list:
            tdSql.error(sql)

    def checkSuperTableWildcardLength(self):
        '''
            check super table wildcard length with % and _
        '''
        self.cleanTb()
        table_name, hp_name, lp_name, ul_name = self.genTableName()
        tdSql.execute(f"CREATE TABLE {table_name} (ts timestamp, c1 int) tags (t1 int)")
        sql_list = [f'show stables like "{hp_name}"', f'show stables like "{lp_name}"', f'show stables like "{ul_name}"']
        for sql in sql_list:
            tdSql.query(sql)
            if len(table_name) >= 1:
                tdSql.checkRows(1)
            else:
                tdSql.error(sql)

        exceed_sql_list = [f'show stables like "%{hp_name}"', f'show stables like "{lp_name}%"', f'show stables like "{ul_name}%"']
        for sql in exceed_sql_list:
            tdSql.error(sql)
    
    def checkRegularWildcardSelectLength(self):
        '''
            check regular table wildcard select length with % and _
        '''
        self.cleanTb()
        table_name, hp_name, lp_name, ul_name = self.genTableName()
        tdSql.execute(f"CREATE TABLE {table_name} (ts timestamp, bi1 binary(200), nc1 nchar(200))")
        tdSql.execute(f'insert into {table_name} values (now, "{table_name}", "{table_name}")')
        sql_list = [f'select * from {table_name} where bi1 like "{hp_name}"',
                    f'select * from {table_name} where bi1 like "{lp_name}"',
                    f'select * from {table_name} where bi1 like "{ul_name}"',
                    f'select * from {table_name} where nc1 like "{hp_name}"',
                    f'select * from {table_name} where nc1 like "{lp_name}"',
                    f'select * from {table_name} where nc1 like "{ul_name}"']
        for sql in sql_list:
            tdSql.query(sql)
            if len(table_name) >= 1:
                tdSql.checkRows(1)
            else:
                tdSql.error(sql)
        
        exceed_sql_list = [f'select * from {table_name} where bi1 like "%{hp_name}"',
                            f'select * from {table_name} where bi1 like "{lp_name}%"',
                            f'select * from {table_name} where bi1 like "{ul_name}%"',
                            f'select * from {table_name} where nc1 like "%{hp_name}"',
                            f'select * from {table_name} where nc1 like "{lp_name}%"',
                            f'select * from {table_name} where nc1 like "{ul_name}%"']
        for sql in exceed_sql_list:
            tdSql.error(sql)
        
    def checkStbWildcardSelectLength(self):
        '''
            check stb wildcard select length with % and _
        '''
        self.cleanTb()
        table_name, hp_name, lp_name, ul_name = self.genTableName()

        tdSql.execute(f'CREATE TABLE {table_name} (ts timestamp, bi1 binary(200), nc1 nchar(200)) tags (si1 binary(200), sc1 nchar(200))')
        tdSql.execute(f'create table {table_name}_sub1 using {table_name} tags ("{table_name}", "{table_name}")')
        tdSql.execute(f'insert into {table_name}_sub1 values (now, "{table_name}", "{table_name}");')

        sql_list = [f'select * from {table_name} where bi1 like "{hp_name}"',
                    f'select * from {table_name} where bi1 like "{lp_name}"',
                    f'select * from {table_name} where bi1 like "{ul_name}"',
                    f'select * from {table_name} where nc1 like "{hp_name}"',
                    f'select * from {table_name} where nc1 like "{lp_name}"',
                    f'select * from {table_name} where nc1 like "{ul_name}"',
                    f'select * from {table_name} where si1 like "{hp_name}"',
                    f'select * from {table_name} where si1 like "{lp_name}"',
                    f'select * from {table_name} where si1 like "{ul_name}"',
                    f'select * from {table_name} where sc1 like "{hp_name}"',
                    f'select * from {table_name} where sc1 like "{lp_name}"',
                    f'select * from {table_name} where sc1 like "{ul_name}"']
      
        for sql in sql_list:
            tdSql.query(sql)
            if len(table_name) >= 1:
                tdSql.checkRows(1)
            else:
                tdSql.error(sql)
        exceed_sql_list = [f'select * from {table_name} where bi1 like "%{hp_name}"',
                            f'select * from {table_name} where bi1 like "{lp_name}%"',
                            f'select * from {table_name} where bi1 like "{ul_name}%"',
                            f'select * from {table_name} where nc1 like "%{hp_name}"',
                            f'select * from {table_name} where nc1 like "{lp_name}%"',
                            f'select * from {table_name} where nc1 like "{ul_name}%"',
                            f'select * from {table_name} where si1 like "%{hp_name}"',
                            f'select * from {table_name} where si1 like "{lp_name}%"',
                            f'select * from {table_name} where si1 like "{ul_name}%"',
                            f'select * from {table_name} where sc1 like "%{hp_name}"',
                            f'select * from {table_name} where sc1 like "{lp_name}%"',
                            f'select * from {table_name} where sc1 like "{ul_name}%"']
        for sql in exceed_sql_list:
            tdSql.error(sql)

    def run(self):
        tdSql.prepare()
        self.checkRegularTableWildcardLength()
        self.checkSuperTableWildcardLength()
        self.checkRegularWildcardSelectLength()
        self.checkStbWildcardSelectLength()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

