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
import random

class TestTd22981(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def alter_rep3(self):
        self.tdSql.execute('drop database if exists d0;')
        self.tdSql.execute('create database d0 replica 1 keep 365 minRows 100 maxRows 4096 comp 2 vgroups 2 precision "ms";')
        self.tdSql.execute('use d0;')
        self.tdSql.execute('create table if not exists almlog (starttime timestamp,endtime timestamp,durationtime int, alarmno int, alarmtext nchar(256),isactive nchar(64)) tags (mcid nchar(16));')
        self.tdSql.execute('create table if not exists mplog (starttime timestamp,mpid int, paravalue nchar(256),mptype nchar(32)) tags (mcid nchar(16));')
        self.tdSql.execute('create table almlog_m5103 using almlog tags("m5103");')
        self.tdSql.execute('create table mplog_n0204_4 using mplog tags("n0204");')
        self.tdSql.execute('insert into almlog_m5103 values(now,now+1s,10,0,"","dismissed");')
        self.tdSql.execute('insert into mplog_n0204_4 (starttime) values(now);')
        self.tdSql.execute('flush database d0;')
        self.tdSql.execute('alter database d0 replica 3;')
        self.tdSql.execute('select * from almlog;')

    def alter_schema(self, stb_count):
        self.tdCom.createDb(dbname='test', replica=1)
        type_list = ["tinyint", "smallint", "int", "bigint", "tinyint unsigned", "smallint unsigned", "int unsigned", "bigint unsigned", "float", "double", "varchar(8)", "nchar(4)", "bool"]
        # create stable
        for i in range(stb_count):
            create_stb_sql = f'create stable stb{i} (ts timestamp, c1 {random.choice(type_list)}, c2 {random.choice(type_list)}, c3 {random.choice(type_list)}, c4 {random.choice(type_list)},\
                    c5 {random.choice(type_list)}, c6 {random.choice(type_list)}, c7 {random.choice(type_list)}, c8 {random.choice(type_list)}, c9 {random.choice(type_list)}, \
                    c10 {random.choice(type_list)}, c11 {random.choice(type_list)}, c12 {random.choice(type_list)}, c13 {random.choice(type_list)}) tags \
                    (t1 {random.choice(type_list)}, t2 {random.choice(type_list)}, t3 {random.choice(type_list)}, t4 {random.choice(type_list)},\
                    t5 {random.choice(type_list)}, t6 {random.choice(type_list)}, t7 {random.choice(type_list)}, t8 {random.choice(type_list)}, t9 {random.choice(type_list)}, \
                    t10 {random.choice(type_list)}, t11 {random.choice(type_list)}, t12 {random.choice(type_list)}, t13 {random.choice(type_list)})'
            self.tdSql.execute(create_stb_sql)
        # create child table and insert
        for i in range(stb_count):
            v = random.randint(1, 100)
            create_ctb_sql = f'create table ctb{i} using stb{i} tags ({v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v})'
            self.tdSql.execute(create_ctb_sql)
            insert_sql = f'insert into ctb{i} values (now, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v})'
            self.tdSql.execute(insert_sql)
        # alter
        for i in range(stb_count):
            v = random.randint(1, 100)
            self.tdSql.execute(f'alter stable stb{i} add column c14 int')
            self.tdSql.execute(f'alter stable stb{i} add tag t14 int')
            self.tdSql.execute(f'alter stable stb{i} rename tag t1 t21')
            insert_sql = f'insert into ctb{i} values (now, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v})'
            self.tdSql.execute(f'alter stable stb{i} add column c15 binary(6)')
            self.tdSql.execute(f'alter stable stb{i} add tag t15 binary(6)')
            insert_sql = f'insert into ctb{i} values (now, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v})'
            self.tdSql.execute(f'alter stable stb{i} modify column c15 binary(7)')
            self.tdSql.execute(f'alter stable stb{i} modify tag t15 binary(7)')
            insert_sql = f'insert into ctb{i} values (now, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v})'
            self.tdSql.execute(f'alter stable stb{i} add column c16 nchar(6)')
            self.tdSql.execute(f'alter stable stb{i} add tag t16 nchar(6)')
            insert_sql = f'insert into ctb{i} values (now, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v})'
            self.tdSql.execute(f'alter stable stb{i} modify column c16 nchar(7)')
            self.tdSql.execute(f'alter stable stb{i} modify tag t16 nchar(7)')
            insert_sql = f'insert into ctb{i} values (now, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v})'
            self.tdSql.execute(f'alter stable stb{i} drop column c14')
            self.tdSql.execute(f'alter stable stb{i} drop tag t14')
            insert_sql = f'insert into ctb{i} values (now, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v}, {v})'

    def run(self):
        self.alter_schema(10000)
        self.alter_rep3()
    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            test_td23659
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write