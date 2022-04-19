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

from taostest.util.rest import TDRest
import copy


class TestDB(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest(env_setting=self.env_setting)

    def dbname_length_check(self):
        """
        max length: 32
        """
        self.tdRest.drop_all_db()
        dbname = self.tdCom.get_long_name(length=self.tdCom.boundary_config["DBNAME_MAX_LENGTH"], mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request('show databases')
        res = self.tdRest.getOneRow(0, dbname)
        self.tdSql.checkEqual(res[0][0], dbname)
        dbname_exceed = self.tdCom.get_long_name(length=self.tdCom.boundary_config["DBNAME_MAX_LENGTH"]+1, mode="letters")
        self.tdRest.error(f'create database if not exists {dbname_exceed}')
        self.tdSql.checkEqual(self.tdRest.resp["desc"], "invalid operation: name too long")
        self.tdRest.request(f'drop database if exists {dbname}')

    def dbname_backquote_unsupport_check(self):
        """
        backquote unsupported
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdRest.error(f'create database if not exists `{dbname}`')
        self.tdSql.checkEqual(self.tdRest.resp["desc"], "invalid operation: invalid db name")

    def alter_db(self):
        """
        alter db
        """
        self.tdRest.drop_all_db()
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        # blocks
        self.tdRest.request(f'alter database {dbname} blocks 12')
        self.tdRest.request('show databases')
        res = self.tdRest.getOneRow(0, dbname)
        self.tdSql.checkEqual(int(res[0][9]), 12)
        # keep
        self.tdRest.request(f'alter database {dbname} keep 365')
        self.tdRest.request('show databases')
        res = self.tdRest.getOneRow(0, dbname)
        if str(res[0][7]) == '365':
            self.tdSql.checkEqual(int(res[0][7]), 365)
        elif str(res[0][7]) == '365,365,365':
            self.tdSql.checkEqual(str(res[0][7]), '365,365,365')
        else:
            self.tdSql.checkEqual(str(res[0][7]), 'unexpected value')
        # comp
        for comp in [0, 1]:
            self.tdRest.request(f'alter database {dbname} comp {comp}')
            self.tdRest.request('show databases')
            res = self.tdRest.getOneRow(0, dbname)
            self.tdSql.checkEqual(res[0][14], comp)
        # # replica
        # out of dnodes
        # for replica in [2, 1]:
        #     self.tdRest.request(f'alter database {dbname} replica {replica}')
        #     self.tdRest.request('show databases')
        #     res = self.tdRest.getOneRow(0, dbname)
        #     self.tdSql.checkEqual(res[0][4], replica)
        # quorum
        # Invalid database options
        # for quorum in [2, 1]:
        #     self.tdRest.request(f'alter database {dbname} quorum {quorum}')
        #     self.tdRest.request('show databases')
        #     res = self.tdRest.getOneRow(0, dbname)
        #     self.tdSql.checkEqual(res[0][5], quorum)
        # cachelast
        for cachelast in [0, 1]:
            self.tdRest.request(f'alter database {dbname} cachelast {cachelast}')
            self.tdRest.request('show databases')
            res = self.tdRest.getOneRow(0, dbname)
            self.tdSql.checkEqual(res[0][15], cachelast)

    def upper_lower_dbname_check(self):
        """
        case insensitive
        """
        for dbname in [self.tdCom.get_long_name(length=10, mode="letters_mixed"), self.tdCom.get_long_name(length=5, mode="letters_mixed").upper()]:
            self.tdRest.request(f'create database if not exists {dbname}')
            self.tdRest.request('show databases')
            res = self.tdRest.getOneRow(0, dbname.lower())
            self.tdSql.checkEqual(res[0][0], dbname.lower())
            self.tdRest.request(f'drop database if exists {dbname}')

    def illegal_dbsql_check(self):
        """
        mixed invalid symbol
        mixed space
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.error(f'create database {dbname}')
        self.tdSql.checkEqual(self.tdRest.resp['desc'], "Database already exists")
        self.tdRest.error(f'create database if not exists 1{dbname}')
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
        for insert_str in self.tdCom.gen_symbol_list():
            d_list = list(dbname)
            for i in range(len(d_list) + 1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                dbname_new = ''.join(d_list_new)
                self.tdRest.error(f'create database if not exists `{dbname_new}`')

    def run(self) -> bool:
        self.dbname_length_check()
        self.dbname_backquote_unsupport_check()
        self.alter_db()
        self.upper_lower_dbname_check()
        self.illegal_dbsql_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            dbname_length_check <jayden>: [TD-12748] : db name length check (max 32);\n
            dbname_backquote_unsupport_check <jayden>: [TD-12748] : unsupport backquote;\n
            alter_db <jayden>: [TD-12748] : alter db params;\n
            upper_lower_dbname_check <jayden>: [TD-12748] : case insensitive;\n
            illegal_dbsql_check <jayden>: [TD-12748] : illegal dbname check;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.RestfulSql.Database.Create, T.Write.RestfulSql.Database.Drop, T.Write.RestfulSql.Database.Alter
