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

class TestMultiTableInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def multi_stb_insert(self):
        """
        multi stables insert
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table {dbname}.stb1 (ts timestamp, c11 int, c12 int ) TAGS(t11 int, t12 int)')
        self.tdSql.execute(f'create table {dbname}.stb2 (ts timestamp, c21 int, c22 int ) TAGS(t21 int, t22 int)')
        self.tdSql.execute(f'create table {dbname}.stb3 (ts timestamp, c31 int, c32 int ) TAGS(t31 int, t32 int)')
        self.tdSql.execute(f'create table {dbname}.stb4 (ts timestamp, c41 int, c42 int ) TAGS(t41 int, t42 int)')
        # separate
        self.tdSql.execute(f'create table {dbname}.tb1 using {dbname}.stb1 TAGS(11, 12)')
        self.tdSql.execute(f'create table {dbname}.tb2 using {dbname}.stb2 TAGS(21, 22)')
        self.tdSql.execute(f'insert into {dbname}.tb1 values (now, 11, 12) {dbname}.tb2 values (now, 21, 22)')
        # combine
        self.tdSql.execute(f'insert into {dbname}.tb3 using {dbname}.stb3 tags (31, 32) values (now, 31, 32) {dbname}.tb4 using {dbname}.stb4 tags (41, 42) values (now, 41, 42)')

        self.tdSql.query(f'select * from {dbname}.stb1')
        self.tdSql.checkEqual(self.tdSql.query_data[0][1:], (11, 12, 11, 12))
        self.tdSql.query(f'select * from {dbname}.stb2')
        self.tdSql.checkEqual(self.tdSql.query_data[0][1:], (21, 22, 21, 22))
        self.tdSql.query(f'select * from {dbname}.stb3')
        self.tdSql.checkEqual(self.tdSql.query_data[0][1:], (31, 32, 31, 32))
        self.tdSql.query(f'select * from {dbname}.stb4')
        self.tdSql.checkEqual(self.tdSql.query_data[0][1:], (41, 42, 41, 42))
        self.tdSql.execute(f'drop database if exists {dbname}')

    def multi_stb_insert_with_specified_column(self):
        """
        multi stables insert
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table {dbname}.stb1 (ts timestamp, c11 int, c12 int ) TAGS(t11 int, t12 int)')
        self.tdSql.execute(f'create table {dbname}.stb2 (ts timestamp, c21 int, c22 int ) TAGS(t21 int, t22 int)')
        self.tdSql.execute(f'create table {dbname}.stb3 (ts timestamp, c31 int, c32 int ) TAGS(t31 int, t32 int)')
        self.tdSql.execute(f'create table {dbname}.stb4 (ts timestamp, c41 int, c42 int ) TAGS(t41 int, t42 int)')
        # separate
        self.tdSql.execute(f'create table {dbname}.tb1 using {dbname}.stb1 TAGS(11, 12)')
        self.tdSql.execute(f'create table {dbname}.tb2 using {dbname}.stb2 TAGS(21, 22)')
        self.tdSql.execute(f'insert into {dbname}.tb1(ts, c11) values (now, 11) {dbname}.tb2(ts, c22) values (now, 22)')
        # combine
        self.tdSql.execute(f'insert into {dbname}.tb3(ts, c31) using {dbname}.stb3 tags (31, 32) values (now, 31) {dbname}.tb4(ts, c42) using {dbname}.stb4 tags (41, 42) values (now, 42)')

        self.tdSql.query(f'select * from {dbname}.stb1')
        self.tdSql.checkEqual(self.tdSql.query_data[0][1:], (11, None, 11, 12))
        self.tdSql.query(f'select * from {dbname}.stb2')
        self.tdSql.checkEqual(self.tdSql.query_data[0][1:], (None, 22, 21, 22))
        self.tdSql.query(f'select * from {dbname}.stb3')
        self.tdSql.checkEqual(self.tdSql.query_data[0][1:], (31, None, 31, 32))
        self.tdSql.query(f'select * from {dbname}.stb4')
        self.tdSql.checkEqual(self.tdSql.query_data[0][1:], (None, 42, 41, 42))
        self.tdSql.execute(f'drop database if exists {dbname}')

    def multi_tb_insert(self):
        """
        multi stables insert
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table {dbname}.tb1 (ts timestamp, c11 int, c12 int )')
        self.tdSql.execute(f'create table {dbname}.tb2 (ts timestamp, c21 int, c22 int )')
        self.tdSql.execute(f'create table {dbname}.tb3 (ts timestamp, c31 int, c32 int )')
        self.tdSql.execute(f'insert into {dbname}.tb1 values (now, 11, 12) {dbname}.tb2 values (now, 21, 22) {dbname}.tb3 values (now, 31, 32)')

        self.tdSql.query(f'select * from {dbname}.tb1')
        self.tdSql.checkEqual(self.tdSql.query_data[0][1:], (11, 12))
        self.tdSql.query(f'select * from {dbname}.tb2')
        self.tdSql.checkEqual(self.tdSql.query_data[0][1:], (21, 22))
        self.tdSql.query(f'select * from {dbname}.tb3')
        self.tdSql.checkEqual(self.tdSql.query_data[0][1:], (31, 32))
        self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self) -> bool:
        self.multi_stb_insert()
        self.multi_stb_insert_with_specified_column()
        self.multi_tb_insert()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            multi_stb_insert <jayden>: [TD-12748] : multi stables insert;\n
            multi_stb_insert_with_specified_column <jayden>: [TD-12748] : multi tables insert when specified column;\n
            multi_tb_insert <jayden>: [TD-12748] : multi tables insert;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Insert.MultiTableInsert

