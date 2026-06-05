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

from sql import tdSql

class TDSetSql:
    def init(self, conn, logSql):
        
        self.stbname = 'stb'

    def set_create_normaltable_sql(self, ntbname='ntb', 
    column_dict={'ts':'timestamp','col1':'tinyint','col2':'smallint','col3':'int','col4':'bigint','col5': 'unsigned int','col6': 'unsigned tinyint','col7': 'unsigned smallint',
    'col8': 'unsigned int','col9': 'unsigned bigint','col10': 'float','col11': 'double','col12': 'bool','col13': 'binary(20)','col14': 'nchar(20)'}):
        column_sql = ''
        for k, v in column_dict.items():
            column_sql += f"{k} {v},"
        create_ntb_sql = f'create table {ntbname} ({column_sql[:-1]})'
        return create_ntb_sql

    def set_create_stable_sql(self,stbname='stb',
    column_dict={'ts':'timestamp','col1':'tinyint','col2':'smallint','col3':'int','col4':'bigint','col5': 'unsigned int','col6': 'unsigned tinyint','col7': 'unsigned smallint',
    'col8': 'unsigned int','col9': 'unsigned bigint','col10': 'float','col11': 'double','col12': 'bool','col13': 'binary(20)','col14': 'nchar(20)'},
    tag_dict={'ts_tag':'timestamp','t1':'tinyint','t2':'smallint','t3':'int','t4':'bigint','t5': 'unsigned int','t6': 'unsigned tinyint','t7': 'unsigned smallint',
    't8': 'unsigned int','t9': 'unsigned bigint','t10': 'float','t11': 'double','t12': 'bool','t13': 'binary(20)','t14': 'nchar(20)'}):
        column_sql = ''
        tag_sql = ''
        for k,v in column_dict.items():
            column_sql += f"{k} {v},"
        for k,v in tag_dict.items():
            tag_sql += f"{k} {v},"
        create_stb_sql = f'create table {stbname} ({column_sql[:-1]}) tags({tag_sql[:-1]})'
        return create_stb_sql
    
    def set_insertsql(self,column_dict,tbname,binary_str=None,nchar_str=None):
        sql = ''
        for k, v in column_dict.items():
            if v.lower() == 'timestamp' or v.lower() == 'tinyint' or v.lower() == 'smallint' or v.lower() == 'int' or v.lower() == 'bigint' or \
            v.lower() == 'tinyint unsigned' or v.lower() == 'smallint unsigned' or v.lower() == 'int unsigned' or v.lower() == 'bigint unsigned' or v.lower() == 'bool':
                sql += '%d,'
            elif v.lower() == 'float' or v.lower() == 'double':
                sql += '%f,'
            elif 'binary' in v.lower():
                sql += f'"{binary_str}%d",'
            elif 'nchar' in v.lower():
                sql += f'"{nchar_str}%d",'
        return (f'insert into {tbname} values({sql[:-1]})')

    def insert_values(self,column_dict,i,insert_sql,insert_list,ts):
        for k, v in column_dict.items():
            if v.lower() in[ 'tinyint' , 'smallint' , 'int', 'bigint' , 'tinyint unsigned' , 'smallint unsigned' , 'int unsigned' , 'bigint unsigned'] or\
            'binary' in v.lower() or 'nchar' in v.lower():
                insert_list.append(0 + i)
            elif v.lower() == 'float' or v.lower() == 'double':
                insert_list.append(0.1 + i)
            elif v.lower() == 'bool':
                insert_list.append(i % 2)
            elif v.lower() == 'timestamp':
                insert_list.append(ts + i)
        tdSql.execute(insert_sql%(tuple(insert_list)))
        