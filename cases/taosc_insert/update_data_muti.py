
###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
import random
from taostest import TDCase, T
from taostest.util.common import TDCom
class TDTestCase(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.dbname = 'db_test'
        self.ntbname = 'ntb'
        self.stbname = 'stb'
        self.rowNum = 10
        self.tbnum = 5
        self.ts = 1537146000000
        self.str_length = 20
        self.column_dict = {
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
            'col9': 'float',
            'col10': 'double',
            'col11': 'bool',
            'col12': f'binary({self.str_length})',
            'col13': f'nchar({self.str_length})'
        }
        self.tinyint_val = random.randint(-128,127)
        self.smallint_val = random.randint(-32768,32767)
        self.int_val = random.randint(-2147483648,2147483647)
        self.bigint_val = random.randint(-9223372036854775808,9223372036854775807)
        self.untingint_val = random.randint(0,255)
        self.unsmallint_val = random.randint(0,65535)
        self.unint_val = random.randint(0,4294967295)
        self.unbigint_val = random.randint(0,18446744073709551615)
        self.float_val = random.uniform(-3.40E+38,3.40E+38)
        self.double_val = random.uniform(-1.7E+308*(1E-300),1.7E+308*(1E-300))
        self.bool_val = random.randint(0,2)%2
        self.binary_val = self.tdCom.get_long_name(random.randint(0,self.str_length))
        self.nchar_val = self.tdCom.get_long_name(random.randint(0,self.str_length))
        self.data = {
            'tinyint':self.tinyint_val,
            'smallint':self.smallint_val,
            'int':self.int_val,
            'bigint':self.bigint_val,
            'tinyint unsigned':self.untingint_val,
            'smallint unsigned':self.unsmallint_val,
            'int unsigned':self.unint_val,
            'bigint unsigned':self.unbigint_val,
            'bool':self.bool_val,
            'float':self.float_val,
            'double':self.double_val,
            'binary':self.binary_val,
            'nchar':self.nchar_val
                    }
    def update_data(self,dbname,tbname,tb_num,rows,values,col_type):
        sql = f'insert into '
        for j in range(tb_num):
            sql += f'{dbname}.{tbname}_{j} values'
            for i in range(rows):
                if 'binary' in col_type.lower() or 'nchar' in col_type.lower():
                    sql += f'({self.ts+i},"{values}")'
                else:
                    sql += f'({self.ts+i},{values})'
            sql += ' '
        self.tdSql.execute(sql)

    def insert_data(self,col_type,tbname,rows,data):
        for i in range(rows):
            if col_type.lower() == 'tinyint':
                self.tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["tinyint"]})')
            elif col_type.lower() == 'smallint':
                self.tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["smallint"]})')
            elif col_type.lower() == 'int':
                self.tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["int"]})')
            elif col_type.lower() == 'bigint':
                self.tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["bigint"]})')
            elif col_type.lower() == 'tinyint unsigned':
                self.tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["tinyint unsigned"]})')
            elif col_type.lower() == 'smallint unsigned':
                self.tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["smallint unsigned"]})')
            elif col_type.lower() == 'int unsigned':
                self.tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["int unsigned"]})')
            elif col_type.lower() == 'bigint unsigned':
                self.tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["bigint unsigned"]})')
            elif col_type.lower() == 'bool':
                self.tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["bool"]})')
            elif col_type.lower() == 'float':
                self.tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["float"]})')
            elif col_type.lower() == 'double':
                self.tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["double"]})')
            elif 'binary' in col_type.lower():
                self.tdSql.execute(f'''insert into {tbname} values({self.ts+i},"{data['binary']}")''')
            elif 'nchar' in col_type.lower():
                self.tdSql.execute(f'''insert into {tbname} values({self.ts+i},"{data['nchar']}")''')

    def data_check(self,dbname,tbname,tbnum,rownum,data,col_name,col_type):
        if 'binary' in col_type.lower():
            self.update_data(dbname,f'{tbname}',tbnum,rownum,data['binary'],col_type)
        elif 'nchar' in col_type.lower():
            self.update_data(dbname,f'{tbname}',tbnum,rownum,data['nchar'],col_type)
        else:
            self.update_data(dbname,f'{tbname}',tbnum,rownum,data[col_type],col_type)
        self.tdSql.execute(f'flush database {dbname}')
        self.tdSql.execute('reset query cache')
        for i in range(self.tbnum):
            self.tdSql.query(f'select {col_name} from {dbname}.{tbname}_{i}')
            for j in range(rownum):
                if col_type.lower() == 'float' or col_type.lower() == 'double':
                    if abs(self.tdSql.query_data[j][0] - data[col_type]) / data[col_type] <= 0.0001:
                        self.tdSql.checkEqual(self.tdSql.query_data[j][0],self.tdSql.query_data[j][0])
                elif 'binary' in col_type.lower():
                    self.tdSql.checkEqual(self.tdSql.query_data[j][0],data['binary'])
                elif 'nchar' in col_type.lower():
                    self.tdSql.checkEqual(self.tdSql.query_data[j][0],data['nchar'])
                else:
                    self.tdSql.checkEqual(self.tdSql.query_data[j][0],data[col_type])
    def update_data_ntb(self):
        self.tdSql.execute(f'drop database if exists {self.dbname}')
        self.tdCom.createDb(self.dbname)
        self.tdSql.execute(f'use {self.dbname}')
        for col_name,col_type in self.column_dict.items():
            for i in range(self.tbnum):
                self.tdSql.execute(f'create table {self.dbname}.{self.ntbname}_{i} (ts timestamp,{col_name} {col_type})')
                for j in range(self.rowNum):
                    self.tdSql.execute(f'insert into {self.dbname}.{self.ntbname}_{i} values({self.ts+j},null)' )
            self.tdSql.execute(f'flush database {self.dbname}')
            self.tdSql.execute('reset query cache')
            self.data_check(self.dbname,self.ntbname,self.tbnum,self.rowNum,self.data,col_name,col_type)
            for i in range(self.tbnum):
                self.tdSql.execute(f'drop table {self.ntbname}_{i}')
    def update_data_ctb(self):
        self.tdSql.execute(f'drop database if exists {self.dbname}')
        self.tdCom.createDb(self.dbname)
        self.tdSql.execute(f'use {self.dbname}')
        for col_name,col_type in self.column_dict.items():
            self.tdSql.execute(f'create table {self.dbname}.{self.stbname} (ts timestamp,{col_name} {col_type}) tags(t0 int)')
            for i in range(self.tbnum):
                self.tdSql.execute(f'create table {self.dbname}.{self.stbname}_{i} using {self.dbname}.{self.stbname} tags(1)')
                for j in range(self.rowNum):
                    self.tdSql.execute(f'insert into {self.dbname}.{self.stbname}_{i} values({self.ts+j},null)' )
            self.tdSql.execute(f'flush database {self.dbname}')
            self.tdSql.execute('reset query cache')
            self.data_check(self.dbname,self.stbname,self.tbnum,self.rowNum,self.data,col_name,col_type)
            self.tdSql.execute(f'drop table {self.stbname}')
    def run(self):
        self.update_data_ntb()
        self.update_data_ctb()
    
    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            update_data_muti check <jiacy>
            """
        return case_description

    def author(self):
        return "Jiacy"

    def tags(self):
        return T.Write.TaoscSql.Insert