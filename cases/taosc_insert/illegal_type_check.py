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

class TestIllegalType(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def int_illegal_type_value_check(self):
        """
        int_type_list = ['tinyint', 'smallint', 'int', 'bigint', 'tinyint unsigned', 'smallint unsigned', 'int unsigned', 'bigint unsigned']
        error_type_value_list = ['contain letters', 'contain illegal symbols', 'contain spaces', 'bool values']
        """
        int_type_list = ['tinyint', 'smallint', 'int', 'bigint', 'tinyint unsigned', 'smallint unsigned', 'int unsigned', 'bigint unsigned']
        # TODO confirm bool values: bool in tag: create ok and trans to 1, bool in insert: insert error
        error_type_value_list = ['a10', '1b0', '10c', '%10', '1$0', '10*', '1 0']
        for test_type in int_type_list:
            dbname = self.tdCom.get_long_name(length=5, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} precision "ms"')
            self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 {test_type}) tags (tag_ts timestamp, t1 {test_type})')
            self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1)')
            for error_type_value in error_type_value_list:
                self.tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags (now, {error_type_value})')
            self.tdSql.error(f'insert into {dbname}.tb values (now, {error_type_value})')
            self.tdSql.execute(f'drop database if exists {dbname}')

    def float_illegal_type_value_check(self):
        """
        float_type_list = ['float', 'double']
        error_type_value_list = ['contain letters', 'contain illegal symbols', 'contain spaces', 'bool values']
        """
        float_type_list = ['float', 'double']
        # TODO confirm bool values: bool in tag: create ok and trans to 1, bool in insert: insert error
        error_type_value_list = ['a1.1', '1b.1', '1.1c', '%1.1', '1$.1', '1.1*', '1 .0', '1. 0']
        for test_type in float_type_list:
            dbname = self.tdCom.get_long_name(length=5, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} precision "ms"')
            self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 {test_type}) tags (tag_ts timestamp, t1 {test_type})')
            self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1.1)')
            for error_type_value in error_type_value_list:
                self.tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags (now, {error_type_value})')
            self.tdSql.error(f'insert into {dbname}.tb values (now, {error_type_value})')
            self.tdSql.execute(f'drop database if exists {dbname}')

    def bool_illegal_type_value_check(self):
        """
        bool_type_list = ['true', 'false']
        error_type_value_list = ['contain letters', 'contain digit', 'contain illegal symbols', 'contain spaces']
        """
        bool_type_list = ['bool']
        error_type_value_list = ['aTrue', 'Fablse', 'Falsec', '1True', 'Fa2lse', 'False3', '*True', 'Fa.lse', 'False%', 'Tru e']
        for test_type in bool_type_list:
            dbname = self.tdCom.get_long_name(length=5, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} precision "ms"')
            self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 {test_type}) tags (tag_ts timestamp, t1 {test_type})')
            self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, True)')
            for error_type_value in error_type_value_list:
                self.tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags (now, {error_type_value})')
            self.tdSql.error(f'insert into {dbname}.tb values (now, {error_type_value})')
            self.tdSql.execute(f'drop database if exists {dbname}')

    def binary_illegal_type_value_check(self):
        """
        binary_type_list = ['binary', 'nchar']
        error_type_value_list = ['contain illegal symbols', 'contain spaces']
        """
        binary_type_list = ['binary(16)', 'nchar(16)']
        # TODO confirm bool values: bool in tag: create ok and trans to 1, bool in insert: insert error
        error_type_value_list = ['%hh', 'h$h', 'hh*', 'h h']
        for test_type in binary_type_list:
            dbname = self.tdCom.get_long_name(length=5, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} precision "ms"')
            self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 {test_type}) tags (tag_ts timestamp, t1 {test_type})')
            self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1.1)')
            for error_type_value in error_type_value_list:
                self.tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags (now, {error_type_value})')
            self.tdSql.error(f'insert into {dbname}.tb values (now, {error_type_value})')
            self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.int_illegal_type_value_check()
        self.float_illegal_type_value_check()
        self.bool_illegal_type_value_check()
        self.binary_illegal_type_value_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            int_illegal_type_value_check <jayden>: [TD-13419] : int_illegal_type_value_check;\n
            float_illegal_type_value_check <jayden>: [TD-13419] : float_illegal_type_value_check;\n
            bool_illegal_type_value_check <jayden>: [TD-13419] : bool_illegal_type_value_check;\n
            binary_illegal_type_value_check <jayden>: [TD-13419] : binary_illegal_type_value_check;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Abnormal
