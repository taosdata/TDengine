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

class TestAlterInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def insert_after_alter_stb_schema(self):
        """
        insert after alter stb schema
        """
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int, c2 int) tags (t1 int, t2 int)')
        self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (1, 1)')
        self.tdSql.execute(f'insert into {dbname}.tb values (now, 1, 1)')
        # drop column
        self.tdSql.execute(f'alter stable {dbname}.stb drop column c2')
        self.tdSql.execute(f'insert into {dbname}.tb values (now-1m, 2)')
        self.tdSql.error(f'insert into {dbname}.tb values (now-1m, 2, 2)')
        self.tdSql.error(f'select t1, t2, c1, c2 from {dbname}.tb')
        self.tdSql.query(f'select t1, t2, c1 from {dbname}.tb where c1 = 2')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (1, 1, 2))

        # add column
        self.tdSql.execute(f'alter stable {dbname}.stb add column c2 int')
        self.tdSql.execute(f'insert into {dbname}.tb values (now-2m, 2, 2)')
        self.tdSql.query(f'select t1, t2, c1, c2 from {dbname}.tb where c2 = 2')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (1, 1, 2, 2))

        # add tag
        self.tdSql.execute(f'alter stable {dbname}.stb add tag t3 binary(5)')
        self.tdSql.execute(f'alter table {dbname}.stb add tag t4 nchar(5)')
        self.tdSql.execute(f'insert into {dbname}.tb values (now-2m, 3, 3)')
        self.tdSql.query(f'select t1, t2, t3, t4, c1, c2 from {dbname}.tb where c2 = 3')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (1, 1, None, None, 3, 3))

        # set tag
        self.tdSql.error(f'alter stable {dbname}.tb set tag t3 = "11111"')
        self.tdSql.execute(f'alter table {dbname}.tb set tag t3 = "11111"')
        self.tdSql.execute(f'alter table {dbname}.tb set tag t4 = "11111"')
        self.tdSql.error(f'alter stable {dbname}.tb set tag t3 = "111111"')
        self.tdSql.error(f'alter table {dbname}.tb set tag t4 = "111111"')
        self.tdSql.execute(f'insert into {dbname}.tb values (now-4m, 4, 4)')
        self.tdSql.query(f'select t1, t2, t3, t4, c1, c2 from {dbname}.tb where c2 = 4')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (1, 1, "11111", "11111", 4, 4))

        # modify tag length
        self.tdSql.execute(f'alter stable {dbname}.stb modify tag t3 binary(6)')
        self.tdSql.execute(f'alter table {dbname}.stb modify tag t4 nchar(6)')
        self.tdSql.error(f'alter stable {dbname}.tb set tag t3 = "111111"')
        self.tdSql.execute(f'alter table {dbname}.tb set tag t3 = "111111"')
        self.tdSql.execute(f'alter table {dbname}.tb set tag t4 = "111111"')
        self.tdSql.error(f'alter stable {dbname}.tb set tag t3 = "1111111"')
        self.tdSql.error(f'alter table {dbname}.tb set tag t4 = "1111111"')
        self.tdSql.execute(f'insert into {dbname}.tb values (now-5m, 5, 5)')
        self.tdSql.query(f'select t1, t2, t3, t4, c1, c2 from {dbname}.tb where c2 = 5')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (1, 1, "111111", "111111", 5, 5))

        # modify column
        self.tdSql.execute(f'alter stable {dbname}.stb add column c3 binary(5)')
        self.tdSql.execute(f'alter table {dbname}.stb add column c4 nchar(5)')
        self.tdSql.error(f'alter stable {dbname}.tb add column c3 binary(5)')
        self.tdSql.error(f'alter table {dbname}.tb add column c4 nchar(5)')
        self.tdSql.execute(f'insert into {dbname}.tb values (now-6m, 6, 6, "11111", "11111")')
        self.tdSql.error(f'insert into {dbname}.tb values (now-6m, 6, 6, "111111", "111111")')
        self.tdSql.query(f'select t1, t2, t3, t4, c1, c2, c3, c4 from {dbname}.tb where c2 = 6')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (1, 1, "111111", "111111", 6, 6, "11111", "11111"))

        # modify column length
        self.tdSql.execute(f'alter stable {dbname}.stb modify column c3 binary(6)')
        self.tdSql.execute(f'alter table {dbname}.stb modify column c4 nchar(6)')
        self.tdSql.error(f'alter table {dbname}.tb modify column c3 binary(6)')
        self.tdSql.error(f'alter table {dbname}.tb modify column c4 nchar(6)')
        self.tdSql.execute(f'insert into {dbname}.tb values (now-6m, 7, 7, "111111", "111111")')
        self.tdSql.error(f'insert into {dbname}.tb values (now-7m, 7, 7, "1111111", "1111111")')
        self.tdSql.query(f'select t1, t2, t3, t4, c1, c2, c3, c4 from {dbname}.tb where c2 = 7')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (1, 1, "111111", "111111", 7, 7, "111111", "111111"))

         # rename tag
        self.tdSql.execute(f'alter table {dbname}.stb rename tag t1 t11')
        self.tdSql.execute(f'alter stable {dbname}.stb rename tag t2 t22')
        self.tdSql.error(f'alter table {dbname}.tb rename tag t3 t33')
        self.tdSql.error(f'alter stable {dbname}.tb rename tag t4 t44')
        self.tdSql.execute(f'insert into {dbname}.tb values (now-8m, 8, 8, "111111", "111111")')
        self.tdSql.query(f'select t11, t22, t3, t4, c1, c2, c3, c4 from {dbname}.tb where c2 = 8')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (1, 1, "111111", "111111", 8, 8, "111111", "111111"))

        # rename column
        self.tdSql.error(f'alter table {dbname}.stb rename column c3 c33')
        self.tdSql.error(f'alter stable {dbname}.stb rename column c3 c33')
        self.tdSql.error(f'alter table {dbname}.tb rename column c3 c33')
        self.tdSql.error(f'alter stable {dbname}.tb rename column c3 c33')
        self.tdSql.execute(f'drop database if exists {dbname}')

    def insert_after_alter_tb_schema(self):
        """
        insert after alter tb schema
        """
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdSql.execute(f'create table if not exists {dbname}.tb (col_ts timestamp, c1 int, c2 int)')
        self.tdSql.execute(f'insert into {dbname}.tb values (now, 1, 1)')

        # add column
        self.tdSql.execute(f'alter table {dbname}.tb add column c3 binary(5)')
        self.tdSql.execute(f'alter table {dbname}.tb add column c4 nchar(5)')
        self.tdSql.execute(f'alter table {dbname}.tb add column c5 nchar(5)')
        self.tdSql.error(f'alter stable {dbname}.tb add column c6 nchar(5)')
        self.tdSql.execute(f'insert into {dbname}.tb values (now-1m, 2, 2, "11111", "11111", "11111")')
        self.tdSql.error(f'insert into {dbname}.tb values (now-1m, 2, 2, "111111", "11111", "11111")')
        self.tdSql.error(f'insert into {dbname}.tb values (now-1m, 2, 2, "11111", "111111", "11111")')
        self.tdSql.query(f'select c1, c2, c3, c4, c5 from {dbname}.tb where c1 = 2')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (2, 2, "11111", "11111", "11111"))

        # drop column
        self.tdSql.execute(f'alter table {dbname}.tb drop column c5')
        self.tdSql.error(f'alter stable {dbname}.tb drop column c4')
        self.tdSql.execute(f'insert into {dbname}.tb values (now-2m, 3, 3, "11111", "11111")')
        self.tdSql.error(f'insert into {dbname}.tb values (now-3m, 3, 3, "11111", "11111", "11111")')
        self.tdSql.error(f'select c1, c2, c3, c4, c5 from {dbname}.tb where c1 = 3')
        self.tdSql.query(f'select c1, c2, c3, c4 from {dbname}.tb where c1 = 3')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (3, 3, "11111", "11111"))

        # modify column
        self.tdSql.error(f'alter stable {dbname}.tb modify column c3 binary(6)')
        self.tdSql.error(f'alter stable {dbname}.tb modify column c4 nchar(6)')
        self.tdSql.execute(f'alter table {dbname}.tb modify column c3 binary(6)')
        self.tdSql.execute(f'alter table {dbname}.tb modify column c4 nchar(6)')
        self.tdSql.execute(f'insert into {dbname}.tb values (now-3m, 4, 4, "111111", "111111")')
        self.tdSql.error(f'insert into {dbname}.tb values (now-3m, 4, 4, "1111111", "1111111")')
        self.tdSql.query(f'select c1, c2, c3, c4 from {dbname}.tb where c1 = 4')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (4, 4, "111111", "111111"))

        # rename column
        self.tdSql.execute(f'alter table {dbname}.tb rename column c3 c33')
        self.tdSql.query(f'insert into {dbname}.tb values (now-4m, 5, 5, "111111", "111111")')
        self.tdSql.error(f'select c1, c2, c3, c4 from {dbname}.tb where c1 = 5')
        self.tdSql.query(f'select c1, c2, c33, c4 from {dbname}.tb where c1 = 5')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (5, 5, "111111", "111111"))

    def run(self) -> bool:
        self.tdCom.drop_all_db()
        self.insert_after_alter_stb_schema()
        self.insert_after_alter_tb_schema()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            insert_after_alter_column <jayden>: [TD-12748] : insert after alter column;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Stable.Alter