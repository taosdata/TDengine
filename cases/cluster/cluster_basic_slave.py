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
from curses.ascii import alt
from http import client
import os ,time
import random
import time
from types import DynamicClassAttribute
import taos
import copy
import datetime
from itertools import product
from itertools import combinations
import subprocess
import logging
from faker import Faker
from taostest import TDCase
from queryutil.createdata import *
from queryutil.where import *
from itertools import product
from itertools import combinations
import subprocess
from taostest.util.file import dict2file
from taostest.util.remote import Remote
import subprocess
import threading

fake = Faker('zh_CN')


class TestCluster(TDCase):

    def init(self):

        self._ts = 1420041600000  # 2015-01-01 00:00:00  this is begin time for first record
        self._ts_step = 1
        self._row_nums = 100
        self._col_nums = 12  # col types is float almost
        self._stables_nums = 100
        self._table_nums = 100
        self.tables_of_per_stable = 10
        self._tags_nums = 10
        self._replica = 3
        self._db_nums = 100
        self._alter_times = 1000
        self._dbs = ["db_%d" % db_num for db_num in range(self._db_nums)]
        self._used_dbs = []
        self._tags = ["alter_tags_%d" %
                      tag_num for tag_num in range(self._tags_nums)]
        self._alter_tags = []
        self._stablenames = ["stable_%d" %
                             table_num for table_num in range(self._stables_nums)]
        self._used_stables = []

    def get_db_name(self):
        dbname = random.sample(self._dbs, 1)[0]
        self._dbs.remove(dbname)
        self._used_dbs.append(dbname)
        return dbname

    def get_stable_name(self):
        stablename = random.sample(self._stablenames, 1)[0]
        self._stablenames.remove(stablename)
        self._used_stables.append(stablename)
        return stablename

    def get_tag_name(self):
        tagname = random.sample(self._tags, 1)[0]
        self._tags.remove(tagname)
        self._alter_tags.append(tagname)
        return tagname

    def create_db_tables(self, db_name, stable_name):

        Dynamic_cols = ''
        if self._col_nums <= 11:
            col_nums = self._col_nums
        else:
            col_nums = self._col_nums-11

        for i in range(col_nums):  # there are 11 basic rows
            if i == col_nums - 1:
                Dynamic_cols += "col_%i  float" % i
            else:
                Dynamic_cols += "col_%i  float ," % i

        if self._col_nums > 11:
            basic_cols = 'q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , '
        else:
            basic_cols = ''
        Dynamic_tags = ''
        for i in range(self._tags_nums):
            if i == self._tags_nums-1:
                Dynamic_tags += "tag_%i  nchar(32)" % i
            else:
                Dynamic_tags += "tag_%i  nchar(32) ," % i

        create_db_sql = ' create database if not exists {} replica {}'.format(
            db_name, self._replica)
        create_table_sqls = "create stable {} (ts timestamp ,{} {} ) tags ({} )".format(
            stable_name, basic_cols, Dynamic_cols, Dynamic_tags)

        # drop database first

        self.tdSql.execute("drop database {}".format(db_name))

        # create database and tables
        self.tdSql.execute(create_db_sql)
        self.tdSql.execute("use {}".format(db_name))
        self.tdSql.execute(create_table_sqls)

    def generate_insert_rows(self, ts):
        values = []
        
        for i in range(self._col_nums):
            values.append(str(float(ts + i*0.1)))
        

        basic_values = []
        if self._col_nums > 11:  # if col_nums >11 ,it contain 11 basic rows
            basic_values = [str(fake.random_int(min=-2147483647, max=2147483647, step=1)),
                            str(fake.random_int(min=-9223372036854775807,
                                max=9223372036854775807, step=1)),
                            str(fake.random_int(min=-32767, max=32767, step=1)
                                ), str(fake.random_int(min=-127, max=127, step=1)),
                            str(fake.pyfloat()), str(fake.pyfloat()), '"true"', "'" + str(fake.pystr()) + "'", "'" + str(fake.pystr()) + "'", str(ts + i)]
        # auto create table
        if self._col_nums > 11:
            values = values[11:]
        str_values = " ,".join(values)
        str_basic_values = " ,".join(basic_values)
        row_values = "{} ,{} , {}".format(ts, str_basic_values, str_values)
        return row_values


    def insert_per_rows(self, db_name, stable_name):

        tags = []
        for i in range(self._tags_nums):
            tags.append("tag_%d" % i)
        str_tags = "'" + "' ,'".join(tags) + "'"

        for row in range(self._row_nums):  # per insert will write very slowly

            ts = self._ts + self._ts_step * row
            rows = self.generate_insert_rows(ts)
         
            for sub_table_ind in range(self.tables_of_per_stable):
                insert_sql = 'insert into sub_{}_{} using {} tags ({}) values ({})'.format(
                    stable_name, sub_table_ind, stable_name, str_tags,rows )
                self.tdSql.execute(insert_sql)

    def alter_talbes():
        pass

    def alter_tags(self):
        # # alter tag length
        # altered_length = []
        # alter_tags = ["tag_%d" % i for i in range(10)]

        # for x in range(200):
            
        #     if x % 10 ==0:
        #         dbname = self.get_db_name
        #         tbname = self.get_stable_name
        #         self.insert_per_rows(dbname , tbname)

        #     tag = random.sample(alter_tags, 1)[0]
        #     alter_time_step = random.randint(1, 5)
        #     time.sleep(alter_time_step)
        #     alter_length_sql = "alter stable {} modify {} nchar(100)".format(
        #         tbname, tag)

        # ALTER STABLE stb_name MODIFY TAG tag_name data_type(length)
        # # add tag
        # ALTER STABLE stb_name ADD TAG new_tag_name tag_type
        # # drop tag
        # ALTER STABLE stb_name DROP TAG tag_name
        # # change tag
        # ALTER STABLE stb_name CHANGE TAG old_tag_name new_tag_name
        pass

    def alter_dbs():
        pass

    def drop_tables():
        pass

    def drop_dbs():
        pass

    def run(self):

        # self.tdSql.execute("create database db0")
        # self.tdSql.execute('use db0')
        self.create_db_tables("db0", "st")
        self.insert_per_rows("db0", "st")

    def cleanup(self):
        pass

    def author(self):
        '''
        abstract about author
        '''
        return "wenzhouwww"

    def tags(self):
        '''
        set tags
        '''
        return "cluster", ""

    def desc(self) -> str:
        case_description = '''
            [test]<wenzhouwww> test case for loop kill and start TDengine ;
        '''
        return case_description
