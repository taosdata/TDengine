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

class TestMultiThreads(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def multi_threads_create_db(self):
        """
        multi threads create db
        """
        self.tdSql.drop_all_db()
        sql_list = list()
        db_list = list()
        for i in range(5):
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            sql = f'create database if not exists {dbname}'
            sql_list.append(sql)
            db_list.append(dbname)
        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)
        self.tdSql.query(f'show databases')
        for dbname in db_list:
            # could not use checkEqual because maybe other agent is writing to database such as prometheus
            self.tdSql.checkIn(dbname, self.tdSql.getColNameList())

    def multi_threads_create_stb(self):
        """
        multi threads create stb
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        sql_list = list()
        stb_list = list()
        for i in range(5):
            stbname = self.tdCom.get_long_name(length=10, mode="letters")
            sql = f'create table {dbname}.{stbname} (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )'
            sql_list.append(sql)
            stb_list.append(stbname)

        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)
        self.tdSql.query(f'show {dbname}.stables')
        self.tdSql.checkEqual(sorted(stb_list), sorted(self.tdSql.getColNameList()))

    def multi_threads_create_tb(self):
        """
        multi threads create tb
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        sql_list = list()
        tb_list = list()
        for i in range(5):
            tbname = self.tdCom.get_long_name(length=10, mode="letters")
            sql = f'create table {dbname}.{tbname} (ts timestamp, c11 int, c12 float)'
            sql_list.append(sql)
            tb_list.append(tbname)

        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)
        self.tdSql.query(f'show {dbname}.tables')
        self.tdSql.checkEqual(sorted(tb_list), sorted(self.tdSql.getColNameList()))

    def multi_threads_insert(self):
        """
        multi threads insert
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table {dbname}.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        self.tdSql.execute(f'create table {dbname}.tb using {dbname}.stb TAGS(1, 1)')
        sql_list = list()
        for i in range(5):
            sql = f'insert into {dbname}.tb values (now-{i}m, {i}, {i})'
            sql_list.append(sql)

        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)
        self.tdSql.query(f'select count(*) from {dbname}.tb')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], 5)

    def multi_threads_create_drop_db_stb_tb(self):
        """
        multi threads create or drop db stb tb
        """
        db = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {db}')
        self.tdSql.execute(f'create table {db}.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        sql_list = list()
        db_list = list()
        stb_list = list()
        tb_list = list()
        for i in range(5):
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            sql = f'create database if not exists {dbname}'
            sql_list.append(sql)
            db_list.append(dbname)

            stbname = self.tdCom.get_long_name(length=10, mode="letters")
            sql = f'create table {db}.{stbname} (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )'
            sql_list.append(sql)
            stb_list.append(stbname)

            tbname = self.tdCom.get_long_name(length=10, mode="letters")
            sql = f'create table {db}.{tbname} using {db}.stb TAGS({i}, {i})'
            sql_list.append(sql)
            tb_list.append(tbname)

        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)

        self.tdSql.query(f'show databases')
        for dbname in db_list:
            # could not use checkEqual because maybe other agent is writing to database such as prometheus
            self.tdSql.checkIn(dbname, self.tdSql.getColNameList())

        self.tdSql.query(f'show {db}.stables')
        for stb in stb_list:
            self.tdSql.checkIn(stb, self.tdSql.getColNameList())

        self.tdSql.query(f'show {db}.tables')
        self.tdSql.checkEqual(sorted(tb_list), sorted(self.tdSql.getColNameList()))

        drop_list = list()
        for tbname in tb_list:
            sql = f'drop table {db}.{tbname}'
            drop_list.append(sql)

        for stbname in stb_list:
            sql = f'drop stable {db}.{stbname}'
            drop_list.append(sql)

        for dbname in db_list:
            sql = f'drop database {dbname}'
            drop_list.append(sql)

        tlist = self.tdSql.genMultiThreadSeq(drop_list)
        self.tdSql.multiThreadRun(tlist)

        self.tdSql.query(f'show databases')
        for dbname in db_list:
            # could not use checkEqual because maybe other agent is writing to database such as prometheus
            self.tdSql.checkNotIn(dbname, self.tdSql.getColNameList())

        self.tdSql.query(f'show {db}.stables')
        for stb in stb_list:
            self.tdSql.checkNotIn(stb, self.tdSql.getColNameList())

        self.tdSql.query(f'show {db}.tables')
        for tb in tb_list:
            self.tdSql.checkNotIn(tb, self.tdSql.getColNameList())

    def multi_threads_create_drop_db_stb_tb_mixed(self):
        """
        multi threads create db stb tb mixed
        """
        db = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {db}')
        self.tdSql.execute(f'create database if not exists {db}_1')
        self.tdSql.execute(f'create table {db}.stb1 (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        self.tdSql.execute(f'create table {db}.stb2 (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        self.tdSql.execute(f'create table {db}.tb1 using {db}.stb1 TAGS(1, 1)')
        self.tdSql.execute(f'create table {db}.tb2 using {db}.stb2 TAGS(2, 2)')
        sql_list = list()

        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        sql_list.append(f'drop database {db}_1')
        sql_list.append(f'drop table {db}.stb1')
        sql_list.append(f'drop table {db}.tb2')

        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)

        self.tdSql.query(f'show databases')
        self.tdSql.checkNotIn(f'{db}_1', self.tdSql.getColNameList())

        self.tdSql.query(f'show {db}.stables')
        self.tdSql.checkNotIn("stb1", self.tdSql.getColNameList())

        self.tdSql.query(f'show {db}.tables')
        self.tdSql.checkNotIn("tb2", self.tdSql.getColNameList())

    def insert_when_dropping_tb(self):
        """
        insert when dropping tb
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table {dbname}.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        self.tdSql.execute(f'create table {dbname}.tb using {dbname}.stb TAGS(1, 1)')
        sql_list = list()
        for i in range(5):
            sql = f'insert into {dbname}.tb values (now-{i}m, {i}, {i})'
            sql_list.append(sql)

        sql_list.append(f'drop table {dbname}.tb')
        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)
        self.tdSql.query(f'show {dbname}.tables')
        self.tdSql.checkNotIn("tb", self.tdSql.getColNameList())

    def insert_when_dropping_db(self):
        """
        insert when dropping db
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table {dbname}.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        self.tdSql.execute(f'create table {dbname}.tb using {dbname}.stb TAGS(1, 1)')
        sql_list = list()
        for i in range(5):
            sql = f'insert into {dbname}.tb values (now-{i}m, {i}, {i})'
            sql_list.append(sql)

        sql_list.append(f'drop database {dbname}')
        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)
        self.tdSql.query(f'show databases')
        self.tdSql.checkNotIn(dbname, self.tdSql.getColNameList())

    def create_table_when_dropping_db(self):
        """
        create table when dropping db
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table {dbname}.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        sql_list = list()
        sql_list.append(f'drop database {dbname}')
        sql_list.append(f'create table {dbname}.tb using {dbname}.stb TAGS(1, 1)')
        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)
        self.tdSql.query(f'show databases')
        self.tdSql.checkNotIn(dbname, self.tdSql.getColNameList())

    def drop_table_when_dropping_db(self):
        """
        drop table when dropping db
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table {dbname}.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        self.tdSql.execute(f'create table {dbname}.tb using {dbname}.stb TAGS(1, 1)')
        sql_list = list()
        sql_list.append(f'drop database {dbname}')
        sql_list.append(f'drop stable {dbname}.stb')
        sql_list.append(f'drop stable {dbname}.tb')
        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)
        self.tdSql.query(f'show databases')
        self.tdSql.checkNotIn(dbname, self.tdSql.getColNameList())

    def del_column_inserting(self):
        """
        del column when inserting
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table {dbname}.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        self.tdSql.execute(f'create table {dbname}.tb using {dbname}.stb TAGS(1, 1)')
        sql_list = list()
        for i in range(5):
            sql = f'insert into {dbname}.tb values (now-{i}m, {i}, {i})'
            sql_list.append(sql)

        sql_list.append(f'alter table {dbname}.stb drop column c12')
        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)
        self.tdSql.error(f'select c12 from {dbname}.stb')

    def add_column_when_inserting(self):
        """
        add column when inserting
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table {dbname}.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        self.tdSql.execute(f'create table {dbname}.tb using {dbname}.stb TAGS(1, 1)')
        sql_list = list()
        for i in range(5):
            sql = f'insert into {dbname}.tb values (now-{i}m, {i}, {i})'
            sql_list.append(sql)

        sql_list.append(f'alter table {dbname}.stb add column c13 int')
        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)
        self.tdSql.execute(f'select c13 from {dbname}.stb')

    def alter_column_when_dropping(self):
        """
        alter column when dropping
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table {dbname}.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        self.tdSql.execute(f'create table {dbname}.tb using {dbname}.stb TAGS(1, 1)')
        sql_list = list()

        sql_list.append(f'alter table {dbname}.stb add column c13 int')
        sql_list.append(f'drop table {dbname}.tb')
        sql_list.append(f'drop table {dbname}.stb')
        sql_list.append(f'drop database {dbname}')
        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)
        self.tdSql.query(f'show databases')
        self.tdSql.checkNotIn(dbname, self.tdSql.getColNameList())

    def run(self) -> bool:
        self.multi_threads_create_db()
        self.multi_threads_create_stb()
        self.multi_threads_create_tb()
        self.multi_threads_insert()
        self.multi_threads_create_drop_db_stb_tb()
        self.multi_threads_create_drop_db_stb_tb_mixed()
        self.insert_when_dropping_tb()
        self.insert_when_dropping_db()
        self.create_table_when_dropping_db()
        self.drop_table_when_dropping_db()
        self.del_column_inserting()
        self.add_column_when_inserting()
        self.alter_column_when_dropping()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            multi_threads_create_db <jayden>: [TD-12748] : multi threads create db;\n
            multi_threads_create_stb <jayden>: [TD-12748] : multi threads create stb;\n
            multi_threads_create_tb <jayden>: [TD-12748] : multi threads create tb;\n
            multi_threads_insert <jayden>: [TD-12748] : multi threads insert;\n
            multi_threads_create_drop_db_stb_tb <jayden>: [TD-12748] : multi threads create db stb tb;\n
            multi_threads_create_drop_db_stb_tb_mixed <jayden>: [TD-12748] : multi threads create db stb tb mixed;\n
            insert_when_dropping_tb <jayden>: [TD-12748] : insert when dropping tb;\n
            insert_when_dropping_db <jayden>: [TD-12748] : insert when dropping db;\n
            create_table_when_dropping_db <jayden>: [TD-12748] : create table when dropping db;\n
            drop_table_when_dropping_db <jayden>: [TD-12748] : drop table when dropping db;\n
            del_column_inserting <jayden>: [TD-12748] : del column when inserting;\n
            add_column_when_inserting <jayden>: [TD-12748] : add column when inserting;\n
            alter_column_when_dropping <jayden>: [TD-12748] : alter column when dropping;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.MultiThread

