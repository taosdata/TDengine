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

from taostest import TDCase, T
from taostest.util.common import TDCom
import copy
import random


class TestDB(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def dbname_length_check(self):
        """
        max length: 64
        """
        dbname = self.tdCom.get_long_name(length=self.tdCom.boundary_config["DBNAME_MAX_LENGTH"], mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        res = self.tdSql.getOneRow(0, dbname)
        self.tdSql.checkEqual(res[0][0], dbname)
        dbname_exceed = self.tdCom.get_long_name(length=self.tdCom.boundary_config["DBNAME_MAX_LENGTH"]+1, mode="letters")
        self.tdSql.error(f'create database if not exists {dbname_exceed}')
        self.tdSql.execute(f'drop database if exists {dbname}')

    def db_params_check(self):
        """
        db params
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        #BUFFER
        #PAGE
        #NPAGE
        param_keys = ["vgroups", "replica", "strict", "days", "keep", "cache", "blocks", "minrows", "maxrows",
            "wal", "fsync", "comp", "cachelast", "precision", "ttl", "single_stable", "stream_mode", "status"]
        for param_key in param_keys:
            param_key_value_list = list()
            if param_key == "cachelast":
                param_key_value_list = [0, 1, 2, 3]
            elif param_key == "comp":
                param_key_value_list = [0, 1, 2]
            elif param_key == "quorum" or param_key == "wal":
                param_key_value_list = [1, 2]
            elif param_key == "precision":
                param_key_value_list = ['"ms"', '"us"', '"ns"']
            #! bug when vgroups = 4096
            elif param_key == "vgroups":
                param_key_value_list = [1, 86]
            elif param_key == "cache":
                param_key_value_list = [1, 128]
            elif param_key == "blocks":
                param_key_value_list = [3, 10000]
            elif param_key == "minrows":
                param_key_value_list = [10, 1000]
            elif param_key == "maxrows":
                param_key_value_list = [200, 10000]
            elif param_key == "fsync":
                param_key_value_list = [0, 180000]
            elif param_key == "status":
                # ! unknown
                param_key_value_list = [1]
            elif param_key == "single_stable" or param_key == "stream_mode" or param_key == "strict":
                param_key_value_list = [0, 1]
            else:
                param_key_value = random.randint(2, 365000)
                if param_key == "keep":
                    test_db = f'{dbname}_{param_key}_{param_key_value}'
                    self.tdSql.execute(f'create database if not exists {test_db} days {param_key_value-1} {param_key} {param_key_value}')
                    self.tdSql.query('show databases')
                    db_field_kv_dict = self.tdSql.get_db_field_kv(0, test_db)
                    if "," in str(db_field_kv_dict[param_key]):
                        self.tdSql.checkEqual(db_field_kv_dict[param_key], f'{param_key_value},{param_key_value},{param_key_value}')
                    else:
                        self.tdSql.checkEqual(db_field_kv_dict[param_key], param_key_value)

                    self.tdSql.execute(f'drop database {test_db}')
                    self.tdSql.error(f'create database if not exists {test_db} days {param_key_value-1} {param_key} {param_key_value-2}')
                    # ! bug
                    # self.tdSql.error(f'create database if not exists {test_db} {param_key} 365001')
                elif param_key == "days":
                    continue
                    # test_db = f'{dbname}_{param_key}_{param_key_value}'
                    # self.tdSql.execute(f'create database if not exists {test_db} days {param_key_value-1} {param_key} {param_key_value}')
                    # self.tdSql.query('show databases')
                    # db_field_kv_dict = self.tdSql.get_db_field_kv(0, test_db)
                    # self.tdSql.checkEqual(str(db_field_kv_dict[param_key]), str(param_key_value))
                    # self.tdSql.execute(f'drop database {test_db}')
                elif param_key == "replica":
                    #! bug res=0 but replica is set to 1
                    continue
                    # param_key_value = 1
                    # test_db = f'{dbname}_{param_key}_{param_key_value}'
                    # self.tdSql.execute(f'create database if not exists {test_db} {param_key} {param_key_value}')
                    # self.tdSql.query('show databases')
                    # db_field_kv_dict = self.tdSql.get_db_field_kv(0, test_db)
                    # self.tdSql.checkEqual(str(db_field_kv_dict[param_key]), str(param_key_value))
                    # self.tdSql.execute(f'drop database {test_db}')
                else:
                    test_db = f'{dbname}_{param_key}_{param_key_value}'
                    self.tdSql.execute(f'create database if not exists {test_db} {param_key} {param_key_value}')
                    self.tdSql.query('show databases')
                    db_field_kv_dict = self.tdSql.get_db_field_kv(0, test_db)
                    self.tdSql.checkEqual(str(db_field_kv_dict[param_key]), str(param_key_value))
                    self.tdSql.execute(f'drop database {test_db}')
            if len(param_key_value_list) > 0:
                #! vgroups upper_limited bug
                if param_key == "vgroups" or param_key == "status":
                    continue
                for param_key_value in param_key_value_list:
                    test_db = f'{dbname}_{param_key}_{param_key_value}'.replace('"', "")
                    self.tdSql.execute(f'create database if not exists {test_db} {param_key} {param_key_value}')
                    self.tdSql.query('show databases')
                    db_field_kv_dict = self.tdSql.get_db_field_kv(0, test_db)
                    if param_key == "strict" and param_key_value == 0:
                        self.tdSql.checkEqual(str(db_field_kv_dict[param_key]), "nostrict")
                    elif param_key == "strict" and param_key_value == 1:
                        self.tdSql.checkEqual(str(db_field_kv_dict[param_key]), "strict")
                    else:
                        self.tdSql.checkEqual(str(db_field_kv_dict[param_key]), str(param_key_value).replace('"', ""))
                    self.tdSql.execute(f'drop database {test_db}')
                    if param_key != "precision":
                        self.tdSql.error(f'create database if not exists {test_db} {param_key} {param_key_value_list[0] - 1}')
                        self.tdSql.error(f'create database if not exists {test_db} {param_key} {param_key_value_list[-1] + 1}')

    def alter_db(self):
        """
        alter db
        """
        self.tdSql.drop_all_db()
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        # blocks
        self.tdSql.execute(f'alter database {dbname} blocks 12')
        self.tdSql.query('show databases')
        res = self.tdSql.getOneRow(0, dbname)
        self.tdSql.checkEqual(int(res[0][9]), 12)
        # wal
        self.tdSql.execute(f'alter database {dbname} wal 2')
        self.tdSql.query('show databases')
        res = self.tdSql.getOneRow(0, dbname)
        self.tdSql.checkEqual(int(res[0][12]), 2)
        # fsync
        self.tdSql.execute(f'alter database {dbname} fsync 1000')
        self.tdSql.query('show databases')
        res = self.tdSql.getOneRow(0, dbname)
        self.tdSql.checkEqual(int(res[0][13]), 1000)
        # keep
        self.tdSql.execute(f'alter database {dbname} keep 36500')
        self.tdSql.query('show databases')
        res = self.tdSql.getOneRow(0, dbname)
        if str(res[0][7]) == '36500':
            self.tdSql.checkEqual(int(res[0][7]), 36500)
        elif str(res[0][7]) == '36500,36500,36500':
            self.tdSql.checkEqual(str(res[0][7]), '36500,36500,36500')
        else:
            self.tdSql.checkEqual(str(res[0][7]), 'unexpected value')
        # # replica
        # out of dnodes
        # for replica in [2, 1]:
        #     self.tdSql.execute(f'alter database {dbname} replica {replica}')
        #     self.tdSql.execute('show databases')
        #     res = self.tdSql.getOneRow(0, dbname)
        #     self.tdSql.checkEqual(res[0][4], replica)
        # quorum
        # Database options not changed
        # for quorum in [1, 2]:
        #     self.tdSql.execute(f'alter database {dbname} quorum {quorum}')
        #     self.tdSql.execute('show databases')
        #     res = self.tdSql.getOneRow(0, dbname)
        #     self.tdSql.checkEqual(res[0][5], quorum)
        # cachelast
        for cachelast in [1, 0]:
            self.tdSql.execute(f'alter database {dbname} cachelast {cachelast}')
            self.tdSql.query('show databases')
            res = self.tdSql.getOneRow(0, dbname)
            self.tdSql.checkEqual(res[0][15], cachelast)

    def dbname_backquote_check(self):
        """
        backquote check
        """
        dbname = '1' + self.tdCom.get_long_name(length=10)
        self.tdSql.execute(f'create database if not exists `{dbname}`')
        self.tdSql.query('show databases')
        res = self.tdSql.getOneRow(0, dbname)
        self.tdSql.checkEqual(res[0][0], dbname)
        dbname = self.tdCom.get_long_name(length=3, mode="letters")
        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove('`')
        symbol_list.remove('\\')
        for insert_str in symbol_list:
            d_list = list(dbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                dbname_new = ''.join(d_list_new)
                self.tdSql.execute(f'create database if not exists `{dbname_new}`')
                self.tdSql.query('show databases')
                res = self.tdSql.getOneRow(0, dbname_new)
                self.tdSql.checkEqual(res[0][0], dbname_new)
                self.tdSql.execute(f'drop database if exists `{dbname_new}`')

    def upper_lower_dbname_check(self):
        """
        case insensitive
        """
        for dbname in [self.tdCom.get_long_name(length=10, mode="letters_mixed"), self.tdCom.get_long_name(length=5, mode="letters_mixed").upper()]:
            self.tdSql.execute(f'create database if not exists {dbname}')
            self.tdSql.query('show databases')
            res = self.tdSql.getOneRow(0, dbname.lower())
            self.tdSql.checkEqual(res[0][0], dbname.lower())
            self.tdSql.execute(f'drop database if exists {dbname}')

    def illegal_dbsql_check(self):
        """
        mixed invalid symbol
        mixed space
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.error(f'create database {dbname}')
        self.tdSql.error(f'create data base if not exists {dbname}')
        self.tdSql.error(f'create database i f not exists {dbname}')
        self.tdSql.error(f'cre ate database if not exists {dbname}')
        self.tdSql.error(f'create database if n ot exists {dbname}')
        self.tdSql.error(f'create database if not e xists {dbname}')
        self.tdSql.error(f'@create database if not exists {dbname}')
        self.tdSql.error(f'cre#ate database if not exists {dbname}')
        self.tdSql.error(f'create( database if not exists {dbname}')
        self.tdSql.error(f'create )database if not exists {dbname}')
        self.tdSql.error(f'create data&base if not exists {dbname}')
        self.tdSql.error(f'create database- if not exists {dbname}')
        self.tdSql.error(f'create database ¥if not exists {dbname}')
        self.tdSql.error(f'create database i*f not exists {dbname}')
        self.tdSql.error(f'create database if! not exists {dbname}')
        self.tdSql.error(f'create database if +not exists {dbname}')
        self.tdSql.error(f'create database if n!ot exists {dbname}')
        self.tdSql.error(f'create database if not| exists {dbname}')
        self.tdSql.error(f'create database if not >exists {dbname}')
        self.tdSql.error(f'create database if not ex<ists {dbname}')
        self.tdSql.error(f'create database if not exists? {dbname}')
        self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self) -> bool:
        self.dbname_length_check()
        # tfz: these two cases are moved to db_param_*.py
        # self.db_params_check()
        # self.alter_db()
        self.dbname_backquote_check()
        self.upper_lower_dbname_check()
        self.illegal_dbsql_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            dbname_length_check <jayden>: [TD-13419] : db name length check (max 64);\n
            db_params_check <jayden>: [TD-1499-1to4] : db params check;\n
            alter_db <jayden>: [TD-1499-1to4] : alter_db;\n
            dbname_backquote_check <jayden>: [TD-1499-1to4] : dbname_backquote_check;\n
            upper_lower_dbname_check <jayden>: [TD-13419] : case insensitive;\n
            illegal_dbsql_check <jayden>: [TD-13419] : illegal dbname check; """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Drop, T.Write.TaoscSql.Database.Alter

