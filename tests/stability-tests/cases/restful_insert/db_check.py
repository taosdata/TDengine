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
from taostest.util.rest import TDRest

class TestDB(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.api_type = 'restful'
    def dbname_length_check(self):
        """
        max length: 64
        """
        dbname = self.tdCom.get_long_name(length=self.tdCom.boundary_config["DBNAME_MAX_LENGTH"], mode="letters")
        self.tdCom.createDb(dbname)
        self.tdRest.request('select * from information_schema.ins_databases')
        res = self.tdRest.getOneRow(0, dbname)
        self.tdSql.checkEqual(res[0][0], dbname)
        dbname_exceed = self.tdCom.get_long_name(length=self.tdCom.boundary_config["DBNAME_MAX_LENGTH"]+1, mode="letters")
        self.tdRest.error(f'create database if not exists {dbname_exceed}')
        self.tdRest.request(f'drop database if exists {dbname}')


    def dbname_backquote_check(self):
        """
        backquote check
        """
        dbname = '1' + self.tdCom.get_long_name() 
        
        self.tdCom.createDb(f'`{dbname}`')
        self.tdRest.request('select * from information_schema.ins_databases')
        res = self.tdRest.getOneRow(0, dbname)
        self.tdSql.checkEqual(res[0][0], dbname)
        dbname = self.tdCom.get_long_name(3)
        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove('`')
        symbol_list.remove('\\')
        symbol_list.remove('.')
        for insert_str in symbol_list:
            d_list = list(dbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                dbname_new = ''.join(d_list_new)
                self.tdCom.createDb(f'`{dbname_new}`')
                self.tdRest.request('select * from information_schema.ins_databases')
                res = self.tdRest.getOneRow(0, dbname_new)
                self.tdSql.checkEqual(res[0][0], dbname_new)
                self.tdRest.request(f'drop database if exists `{dbname_new}`')

    def upper_lower_dbname_check(self):
        """
        case insensitive
        """
        for dbname in [self.tdCom.get_long_name(length=10, mode="letters_mixed"), self.tdCom.get_long_name(length=5, mode="letters_mixed").upper()]:
            self.tdCom.createDb(dbname)
            self.tdRest.request('select * from information_schema.ins_databases')
            res = self.tdRest.getOneRow(0, dbname.lower())
            self.tdSql.checkEqual(res[0][0], dbname.lower())
            self.tdRest.request(f'drop database if exists {dbname}')

    def illegal_dbsql_check(self):
        """
        mixed invalid symbol
        mixed space
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname)
        self.tdRest.error(f'create database {dbname}')
        self.tdRest.error(f'create data base if not exists {dbname}')
        self.tdRest.error(f'create database i f not exists {dbname}')
        self.tdRest.error(f'cre ate database if not exists {dbname}')
        self.tdRest.error(f'create database if n ot exists {dbname}')
        self.tdRest.error(f'create database if not e xists {dbname}')
        self.tdRest.error(f'@create database if not exists {dbname}')
        self.tdRest.error(f'cre#ate database if not exists {dbname}')
        self.tdRest.error(f'create( database if not exists {dbname}')
        self.tdRest.error(f'create )database if not exists {dbname}')
        self.tdRest.error(f'create data&base if not exists {dbname}')
        self.tdRest.error(f'create database- if not exists {dbname}')
        self.tdRest.error(f'create database ¥if not exists {dbname}')
        self.tdRest.error(f'create database i*f not exists {dbname}')
        self.tdRest.error(f'create database if! not exists {dbname}')
        self.tdRest.error(f'create database if +not exists {dbname}')
        self.tdRest.error(f'create database if n!ot exists {dbname}')
        self.tdRest.error(f'create database if not| exists {dbname}')
        self.tdRest.error(f'create database if not >exists {dbname}')
        self.tdRest.error(f'create database if not ex<ists {dbname}')
        self.tdRest.error(f'create database if not exists? {dbname}')
        self.tdRest.request(f'drop database if exists {dbname}')

    def run(self) -> bool:
        self.dbname_length_check()
        self.dbname_backquote_check()
        self.upper_lower_dbname_check()
        self.illegal_dbsql_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            dbname_length_check <jayden>: [TD-13419] : db name length check (max 64);\n
            dbname_backquote_check <jayden>: [TD-1499-1to4] : dbname_backquote_check;\n
            upper_lower_dbname_check <jayden>: [TD-13419] : case insensitive;\n
            illegal_dbsql_check <jayden>: [TD-13419] : illegal dbname check; """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Drop

