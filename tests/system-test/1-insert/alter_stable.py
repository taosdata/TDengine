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

import random
import string
import threading
from util.log import *
from util.cases import *
from util.sql import *
from util.sqlset import *
from util import constant
from util.common import *
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        self.fname = __file__ + '.tmp.sql'
        self.dbname = 'db1'
        self.ntbname = 'ntb'
        self.stbname = 'stb'
        self.stbnum = 10
        self.ntbnum = 10
        self.colnum = 52
        self.tagnum = 15
        self.collen = 320
        self.colnum_modify = 40
        self.tagnum_modify = 40
        self.collen_old_modify = 160
        self.collen_new_modify = 455
        self.taglen_old_modify = 80
        self.taglen_new_modify = 155
        self.binary_length = 20 # the length of binary for column_dict
        self.nchar_length = 20  # the length of nchar for column_dict
        self.threadnum = 2
        self.column_dict = {
            'ts'  : 'timestamp',
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
            'col12': f'binary({self.binary_length})',
            'col13': f'nchar({self.nchar_length})'
        }
        self.tag_dict = {
            'ts_tag'  : 'timestamp',
            't1': 'tinyint',
            't2': 'smallint',
            't3': 'int',
            't4': 'bigint',
            't5': 'tinyint unsigned',
            't6': 'smallint unsigned',
            't7': 'int unsigned',
            't8': 'bigint unsigned',
            't9': 'float',
            't10': 'double',
            't11': 'bool',
            't12': f'binary({self.binary_length})',
            't13': f'nchar({self.nchar_length})'
        }
        self.tag_list = [
            f'now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据"'
        ]
        self.tbnum = 1
        self.values_list = [
            f'now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据"'
        ]
        self.column_add_dict = {
            'col_time'      : 'timestamp',
            'col_tinyint'   : 'tinyint',
            'col_smallint'  : 'smallint',
            'col_int'       : 'int',
            'col_bigint'    : 'bigint',
            'col_untinyint' : 'tinyint unsigned',
            'col_smallint'  : 'smallint unsigned',
            'col_int'       : 'int unsigned',
            'col_bigint'    : 'bigint unsigned',
            'col_bool'      : 'bool',
            'col_float'     : 'float',
            'col_double'    : 'double',
            'col_binary'    : f'binary({constant.BINARY_LENGTH_MAX})',
            'col_nchar'     : f'nchar({constant.NCAHR_LENGTH_MAX})'

        }

    def alter_stable_check(self):
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        for i in self.values_list:
            tdSql.execute(f'insert into {self.ntbname} values({i})')
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_list[i]})')
            for j in self.values_list:
                tdSql.execute(f'insert into {self.stbname}_{i} values({j})')
        for key,values in self.column_add_dict.items():
            tdSql.execute(f'alter stable {self.stbname} add column {key} {values}')
            tdSql.query(f'describe {self.stbname}')
            tdSql.checkRows(len(self.column_dict)+len(self.tag_dict)+1)
            for i in range(self.tbnum):
                tdSql.query(f'describe {self.stbname}_{i}')
                tdSql.checkRows(len(self.column_dict)+len(self.tag_dict)+1)
                tdSql.query(f'select {key} from {self.stbname}_{i}')
                tdSql.checkRows(len(self.values_list))
            for i in range(self.tbnum):
                tdSql.error(f'alter stable {self.stbname}_{i} add column {key} {values}')
                tdSql.error(f'alter stable {self.stbname}_{i} drop column {key}')
            #! bug TD-16921
            tdSql.error(f'alter stable {self.ntbname} add column {key} {values}')
            tdSql.error(f'alter stable {self.ntbname} drop column {key}')
            tdSql.execute(f'alter stable {self.stbname} drop column {key}')
            tdSql.query(f'describe {self.stbname}')
            tdSql.checkRows(len(self.column_dict)+len(self.tag_dict))
            for i in range(self.tbnum):
                tdSql.query(f'describe {self.stbname}_{i}')
                tdSql.checkRows(len(self.column_dict)+len(self.tag_dict))
            tdSql.error(f'select {key} from {self.stbname} ')
        for key,values in self.column_dict.items():
            if 'binary' in values.lower():
                v = f'binary({self.binary_length+1})'
                v_error = f'binary({self.binary_length-1})'
                tdSql.error(f'alter stable {self.stbname} modify column {key} {v_error}')
                tdSql.execute(f'alter stable {self.stbname} modify column {key} {v}')
                tdSql.query(f'describe {self.stbname}')
                result = tdCom.getOneRow(1,'VARCHAR')
                tdSql.checkEqual(result[0][2],self.binary_length+1)
                for i in range(self.tbnum):
                    tdSql.query(f'describe {self.stbname}_{i}')
                    result = tdCom.getOneRow(1,'VARCHAR')
                    tdSql.checkEqual(result[0][2],self.binary_length+1)
                    tdSql.error(f'alter stable {self.stbname}_{i} modify column {key} {v}')
               #! bug TD-16921
                tdSql.error(f'alter stable {self.ntbname} modify column {key} {v}')
            elif 'nchar' in values.lower():
                v = f'nchar({self.binary_length+1})'
                v_error = f'nchar({self.binary_length-1})'
                tdSql.error(f'alter stable {self.stbname} modify column {key} {v_error}')
                tdSql.execute(f'alter stable {self.stbname} modify column {key} {v}')
                tdSql.query(f'describe {self.stbname}')
                result = tdCom.getOneRow(1,'NCHAR')
                tdSql.checkEqual(result[0][2],self.binary_length+1)
                for i in range(self.tbnum):
                    tdSql.query(f'describe {self.stbname}_{i}')
                    result = tdCom.getOneRow(1,'NCHAR')
                    tdSql.checkEqual(result[0][2],self.binary_length+1)
                    tdSql.error(f'alter stable {self.stbname}_{i} modify column {key} {v}')
                #! bug TD-16921
                tdSql.error(f'alter stable {self.ntbname} modify column {key} {v}')
            else:
                for v in self.column_dict.values():
                    tdSql.error(f'alter stable {self.stbname} modify column {key} {v}')
                    tdSql.error(f'alter stable {self.ntbname} modify column {key} {v}')
                    for i in range(self.tbnum):
                        tdSql.error(f'alter stable {self.stbname}_{i} modify column {key} {v}')
    
    def alter_stable_column_varchar_39001(self):
        """Check alter stable column varchar 39001 from 39000(TS-3841)
        """
        stbname = "st1"
        column_dict = {
            'ts'  : 'timestamp',
            'col1': 'varchar(39000)',
            'col2': 'tinyint',
            'col3': 'timestamp',
            'col4': 'tinyint',
            'col5': 'timestamp',
            'col6': 'varchar(18)',
            'col7': 'varchar(17)'
        }
        tag_dict = {
            'id': 'int'
        }

        tdSql.execute(self.setsql.set_create_stable_sql(stbname, column_dict, tag_dict))
        res = tdSql.getResult(f'desc {stbname}')
        tdLog.info(res)
        assert(res[1][2] == 39000)
        tdSql.execute(f'alter stable {stbname} modify column col1 varchar(39001)')
        res = tdSql.getResult(f'desc {stbname}')
        tdLog.info(res)
        assert(res[1][2] == 39001)

    def prepareAlterEnv(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database if not exists {self.dbname} vgroups 2')
        tdSql.execute(f'use {self.dbname}')

    def destroyAlterEnv(self):
        tdSql.execute(f'drop database if exists {self.dbname}')

    def alterTableTask(self, i):
        os.system(f'taos -f {self.fname}.{i};')

    def executeAlterTable(self, opt):
        threads = []
        for i in range(self.threadnum):
            thread = threading.Thread(target=self.alterTableTask, args=(i,))
            threads.append(thread)
            thread.start()
        for i in range(self.threadnum):
            threads[i].join()

    def checkAlterTable(self, opt):
        if opt in ["stb_add_col", "stb_add_tag"]:
            for i in range(self.stbnum):
                tdSql.execute(f'desc {self.stbname}_{i}')
        elif opt in ["stb_modify_col", "stb_modify_tag"]:
            for i in range(self.stbnum):
                tdSql.execute(f'desc {self.stbname}_{i}')
        elif opt in ["ntb_add_col", "ntb_modify_col"]:
            for i in range(self.ntbnum):
                tdSql.execute(f'desc {self.ntbname}_{i}')

    def destroyAlterTable(self):
        for i in range(self.threadnum):
            if os.path.isfile(f'{self.fname}.{i}'):
                os.remove(f'{self.fname}.{i}')
    
    def prepareAlterTable(self, opt):
        self.destroyAlterTable()
        lines = [f'use {self.dbname};\n']
        if opt in ["stb_add_col", "stb_add_tag"]:
            for i in range(self.stbnum):
                tdSql.execute(f'create table if not exists {self.stbname}_{i} (ts timestamp, c_0 NCHAR({self.collen})) tags(t0 nchar({self.collen}));')
            for i in range(self.stbnum):
                if opt == 'stb_add_col': 
                    for c in range(1, self.colnum):
                        lines.append(f'alter table {self.stbname}_{i} add column c_{c} NCHAR({self.collen});\n')
                else:
                    for c in range(1, self.tagnum):
                        lines.append(f'alter table {self.stbname}_{i} add tag t_{c} NCHAR({self.collen});\n')
        elif opt in ["stb_modify_col", "stb_modify_tag"]:
            for i in range(self.stbnum):
                createTbSql = f'CREATE table if not exists {self.stbname}_{i} (ts timestamp'
                for j in range(self.colnum_modify):
                    createTbSql += f',c_{j} NCHAR({self.collen_old_modify})'
                createTbSql += f') tags(t_0 NCHAR({self.taglen_old_modify})'
                for k in range(1,self.tagnum_modify):
                    createTbSql += f',t_{k} NCHAR({self.taglen_old_modify})'
                createTbSql += f');'
                tdLog.info(createTbSql)
                tdSql.execute(createTbSql)
            for i in range(self.stbnum):
                if opt == 'stb_modify_col': 
                    for c in range(self.colnum_modify):
                        lines.append(f'alter table {self.stbname}_{i} modify column c_{c} NCHAR({self.collen_new_modify});\n')
                else:
                    for c in range(self.tagnum_modify):
                        lines.append(f'alter table {self.stbname}_{i} modify tag t_{c} NCHAR({self.taglen_new_modify});\n')
        elif opt in ['ntb_add_col']:
            for i in range(self.ntbnum):
                tdSql.execute(f'create table if not exists {self.ntbname}_{i} (ts timestamp, c_0 NCHAR({self.collen}));')
            for i in range(self.ntbnum):
                for c in range(1, self.colnum):
                    lines.append(f'alter table {self.ntbname}_{i} add column c_{c} NCHAR({self.collen});\n')
        elif opt in ['ntb_modify_col']:
            for i in range(self.ntbnum):
                createTbSql = f'CREATE table if not exists {self.ntbname}_{i} (ts timestamp'
                for j in range(self.colnum_modify):
                    createTbSql += f',c_{j} NCHAR({self.collen_old_modify})'
                createTbSql += f');'
                tdLog.info(createTbSql)
                tdSql.execute(createTbSql)
            for i in range(self.ntbnum):
                for c in range(self.colnum_modify):
                    lines.append(f'alter table {self.ntbname}_{i} modify column c_{c} NCHAR({self.collen_new_modify});\n')
        # generate sql file
        with open(f'{self.fname}.0', "a") as f:
            f.writelines(lines)
        # clone sql file in case of race condition
        for i in range(1, self.threadnum):
            shutil.copy(f'{self.fname}.0', f'{self.fname}.{i}')

    def alter_stable_multi_client_check(self):
        """Check alter stable/ntable var type column/tag(PI-23)
        """
        alter_table_check_type = ["stb_add_col", "stb_add_tag", "stb_modify_col", "stb_modify_tag", "ntb_add_col", "ntb_modify_col"]

        for opt in alter_table_check_type:
            self.prepareAlterEnv()
            self.prepareAlterTable(opt)
            self.executeAlterTable(opt)
            self.checkAlterTable(opt)
            self.destroyAlterTable()
        self.destroyAlterEnv()

    def run(self):
        self.alter_stable_check()
        self.alter_stable_column_varchar_39001()
        self.alter_stable_multi_client_check()
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
