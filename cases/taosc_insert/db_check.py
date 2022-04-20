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


class TestDB(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def dbname_length_check(self):
        """
        max length: 32
        """
        dbname = self.tdCom.get_long_name(length=self.tdCom.boundary_config["DBNAME_MAX_LENGTH"], mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        res = self.tdSql.getOneRow(0, dbname)
        self.tdSql.checkEqual(res[0][0], dbname)
        dbname_exceed = self.tdCom.get_long_name(length=self.tdCom.boundary_config["DBNAME_MAX_LENGTH"]+1, mode="letters")
        self.tdSql.error(f'create database if not exists {dbname_exceed}')
        self.tdSql.execute(f'drop database if exists {dbname}')

    def dbname_backquote_unsupport_check(self):
        """
        backquote unsupported
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.error(f'create database if not exists `{dbname}`')

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
        for insert_str in self.tdCom.gen_symbol_list():
            d_list = list(dbname)
            for i in range(len(d_list) + 1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                dbname_new = ''.join(d_list_new)
                self.tdSql.error(f'create database if not exists `{dbname_new}`')
        self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self) -> bool:
        self.dbname_length_check()
        self.dbname_backquote_unsupport_check()
        self.upper_lower_dbname_check()
        self.illegal_dbsql_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            dbname_length_check <jayden>: [TD-13419] : db name length check (max 32);\n
            dbname_backquote_unsupport_check <jayden>: [TD-13419] : unsupport backquote;\n
            upper_lower_dbname_check <jayden>: [TD-13419] : case insensitive;\n
            illegal_dbsql_check <jayden>: [TD-13419] : illegal dbname check; """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Drop, T.Write.TaoscSql.Database.Alter

