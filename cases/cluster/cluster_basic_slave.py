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
import os
import time
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
        self._dbs = 0
        self._used_dbs = []
        self._tags = 0
        self._alter_tags = []
        self._stablenames = 0
        self._used_stables = []

    def get_db_name(self):

        dbname = 'db_%d'%self._dbs
        self._used_dbs.append(dbname)
        self._dbs  += 1 
        return dbname

    def get_stable_name(self):
        stablename = "stable_%d"%self._stablenames
        self._used_stables.append(stablename)
        self._stablenames += 1
        return stablename

    def get_tag_name(self):
        tagname = "alter_tag_%d"%self._alter_tags
        self._alter_tags.append(tagname)
        self._alter_tags +=1 
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

        self.tdSql.execute("use {}".format(db_name))

        tags = []
        for i in range(self._tags_nums):
            tags.append("tag_%d" % i)
        str_tags = "'" + "' ,'".join(tags) + "'"

        for row in range(self._row_nums):  # per insert will write very slowly

            ts = self._ts + self._ts_step * row
            rows = self.generate_insert_rows(ts)

            for sub_table_ind in range(self.tables_of_per_stable):
                insert_sql = 'insert into sub_{}_{} using {} tags ({}) values ({})'.format(
                    stable_name, sub_table_ind, stable_name, str_tags, rows)
                self.tdSql.execute(insert_sql)

    def alter_talbes():
        pass

    def alter_tags(self):

        def _MODIFY_TAG(dbname, stbname, old_tag, set_length):
            self.tdSql.execute("use {}".format(dbname))
            alter_length_sql = "alter stable {} modify TAG {} nchar({})".format(
                stbname, old_tag, set_length)

            # get before schema and last rows
            self.tdSql.query("describe {}".format(stbname))
            schema = self.tdSql.query_data
            tag_value = []
            get_index = 0
            index = 0
            for schema_item in schema:
                if schema_item[0] == old_tag and schema_item[-1] == "TAG":
                    get_index = index
                if schema_item[-1] == "TAG":
                    tag_value.append(str(schema_item[0]))
                    index += 1

            tag_value[get_index] = tag_value[get_index] + \
                "e" * (set_length - len(tag_value[get_index]))
            str_tags = " , ".join(tag_value)

            self.tdSql.query(" select last(*) from {}".format(stbname))
            last_row = self.tdSql.query_data
            # replace datetime , only first ts and the q_ts is timestamp
            replace_row = []
            for elem in last_row[0]:
                if isinstance(elem, bool):
                    elem = str(elem)
                elif isinstance(elem, str):
                    elem = "'" + elem + "'"
                else:
                    pass
                replace_row.append(str(elem))

            extra_ts_start = self._ts + self._ts_step * self._row_nums
            self.tdSql.execute(alter_length_sql)
            self.tdSql.execute("reset query cache ")
            self.tdSql.query("describe {}".format(stbname))
            schema = self.tdSql.query_data

            self.tdSql.query("show tables ")
            tables = self.tdSql.query_data
            sub_tablenames = []
            for table in tables:
                sub_tablenames.append(table[0])

            random_sub_table = random.sample(sub_tablenames, 1)[0]

            for i in range(100):
                extra_ts = extra_ts_start + i * self._ts_step
                replace_row[0] = str(extra_ts)
                replace_row[10] = str(extra_ts)

                str_value = " , ".join(replace_row)
                insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    "extra_schema_table", stbname, str_tags,  str_value)
                self.tdSql.execute(insert_sql)
                insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    random_sub_table, stbname, str_tags,  str_value)
                self.tdSql.execute(insert_sql)
            self.tdSql.query("select count(*) from {}".format(stbname))
            self.tdSql.checkData(0, 0, self._row_nums *
                                 self.tables_of_per_stable + 200)

        def _ADD_TAG(dbname, stbname, new_tag):
            self.tdSql.execute("use {}".format(dbname))
            alter_length_sql = "alter stable {} ADD TAG {} int".format(
                stbname, new_tag)

            # get before schema and last rows
            self.tdSql.query("describe {}".format(stbname))
            schema = self.tdSql.query_data
            self.tdSql.query(" select last(*) from {}".format(stbname))
            last_row = self.tdSql.query_data
            # replace datetime , only first ts and the q_ts is timestamp
            replace_row = []
            for elem in last_row[0]:
                if isinstance(elem, bool):
                    elem = str(elem)
                elif isinstance(elem, str):
                    elem = "'" + elem + "'"
                else:
                    pass
                replace_row.append(str(elem))

            extra_ts_start = self._ts + self._ts_step * self._row_nums
            self.tdSql.query("describe {}".format(stbname))
            schema = self.tdSql.query_data
            tag_value = []
            index = 0
            for schema_item in schema:
                if schema_item[-1] == "TAG":
                    tag_value.append(str(schema_item[0]))
                    index += 1
            tag_value.append("10086")

            self.tdSql.execute(alter_length_sql)
            self.tdSql.execute("reset query cache ")
            str_tags = " , ".join(tag_value)

            self.tdSql.query("show tables ")
            tables = self.tdSql.query_data
            sub_tablenames = []
            for table in tables:
                sub_tablenames.append(table[0])

            random_sub_table = random.sample(sub_tablenames, 1)[0]

            for i in range(100):
                extra_ts = extra_ts_start + i * self._ts_step
                replace_row[0] = str(extra_ts)
                replace_row[10] = str(extra_ts)

                str_value = " , ".join(replace_row)
                insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    "extra_schema_table", stbname, str_tags,  str_value)
                self.tdSql.execute(insert_sql)
                insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    random_sub_table, stbname, str_tags,  str_value)
                self.tdSql.execute(insert_sql)
            self.tdSql.query("select count(*) from {}".format(stbname))
            self.tdSql.checkData(0, 0, self._row_nums *
                                 self.tables_of_per_stable + 200)
        
        def _DROP_TAG(dbname, stbname, old_tag):
            self.tdSql.execute("use {}".format(dbname))
            alter_length_sql = "alter stable {} DROP TAG {}".format(
                stbname, old_tag)

            # get before schema and last rows
            self.tdSql.query("describe {}".format(stbname))
            schema = self.tdSql.query_data
            self.tdSql.query(" select last(*) from {}".format(stbname))
            last_row = self.tdSql.query_data
            # replace datetime , only first ts and the q_ts is timestamp
            replace_row = []
            for elem in last_row[0]:
                if isinstance(elem, bool):
                    elem = str(elem)
                elif isinstance(elem, str):
                    elem = "'" + elem + "'"
                else:
                    pass
                replace_row.append(str(elem))

            extra_ts_start = self._ts + self._ts_step * self._row_nums
            self.tdSql.query("describe {}".format(stbname))
            schema = self.tdSql.query_data
            tag_value = []
            get_index = 0
            index = 0
            for schema_item in schema:
                if schema_item[0] == old_tag and schema_item[-1] == "TAG":
                    get_index = index
                    continue
                if schema_item[-1] == "TAG":
                    tag_value.append(str(schema_item[0]))
                    index += 1

            self.tdSql.execute(alter_length_sql)
            self.tdSql.execute("reset query cache ")
            str_tags = " , ".join(tag_value)

            self.tdSql.query("show tables ")
            tables = self.tdSql.query_data
            sub_tablenames = []
            for table in tables:
                sub_tablenames.append(table[0])

            random_sub_table = random.sample(sub_tablenames, 1)[0]

            for i in range(100):
                extra_ts = extra_ts_start + i * self._ts_step
                replace_row[0] = str(extra_ts)
                replace_row[10] = str(extra_ts)

                str_value = " , ".join(replace_row)
                insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    "extra_schema_table", stbname, str_tags,  str_value)
                self.tdSql.execute(insert_sql)
                insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    random_sub_table, stbname, str_tags,  str_value)
                self.tdSql.execute(insert_sql)
            self.tdSql.query("select count(*) from {}".format(stbname))
            self.tdSql.checkData(0, 0, self._row_nums *
                                 self.tables_of_per_stable + 200)

        def _CHANGE_TAG(dbname, stbname, old_tag, new_tag):

            self.tdSql.execute("use {}".format(dbname))
            alter_length_sql = "alter stable {} CHANGE TAG {} {}".format(
                stbname, old_tag , new_tag)

            # get before schema and last rows
            self.tdSql.query("describe {}".format(stbname))
            schema = self.tdSql.query_data
            self.tdSql.query(" select last(*) from {}".format(stbname))
            last_row = self.tdSql.query_data
            # replace datetime , only first ts and the q_ts is timestamp
            replace_row = []
            for elem in last_row[0]:
                if isinstance(elem, bool):
                    elem = str(elem)
                elif isinstance(elem, str):
                    elem = "'" + elem + "'"
                else:
                    pass
                replace_row.append(str(elem))

            extra_ts_start = self._ts + self._ts_step * self._row_nums
            self.tdSql.query("describe {}".format(stbname))
            schema = self.tdSql.query_data
            tag_value = []
            get_index = 0
            index = 0
            for schema_item in schema:
                if schema_item[0] == old_tag and schema_item[-1] == "TAG":
                    get_index = index
                    tag_value.append(str(new_tag))
                    continue
                if schema_item[-1] == "TAG":
                    tag_value.append(str(schema_item[0]))
                    index += 1

            self.tdSql.execute(alter_length_sql)
            self.tdSql.execute("reset query cache ")
            str_tags = " , ".join(tag_value)

            self.tdSql.query("show tables ")
            tables = self.tdSql.query_data
            sub_tablenames = []
            for table in tables:
                sub_tablenames.append(table[0])

            random_sub_table = random.sample(sub_tablenames, 1)[0]

            for i in range(100):
                extra_ts = extra_ts_start + i * self._ts_step
                replace_row[0] = str(extra_ts)
                replace_row[10] = str(extra_ts)

                str_value = " , ".join(replace_row)
                insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    "extra_schema_table", stbname, str_tags,  str_value)
                self.tdSql.execute(insert_sql)
                insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    random_sub_table, stbname, str_tags,  str_value)
                self.tdSql.execute(insert_sql)
            self.tdSql.query("select count(*) from {}".format(stbname))
            self.tdSql.checkData(0, 0, self._row_nums *
                                 self.tables_of_per_stable + 200)

        def _SET_TAG(dbname, stbname, old_tag, set_tag_value):
            self.tdSql.execute("use {}".format(dbname))
            
            # get before schema and last rows
            self.tdSql.query("describe {}".format(stbname))
            schema = self.tdSql.query_data
            self.tdSql.query(" select last(*) from {}".format(stbname))
            last_row = self.tdSql.query_data
            # replace datetime , only first ts and the q_ts is timestamp
            replace_row = []
            for elem in last_row[0]:
                if isinstance(elem, bool):
                    elem = str(elem)
                elif isinstance(elem, str):
                    elem = "'" + elem + "'"
                else:
                    pass
                replace_row.append(str(elem))
            extra_ts_start = self._ts + self._ts_step * self._row_nums
            self.tdSql.query("describe {}".format(stbname))
            schema = self.tdSql.query_data
            tag_value = []
            get_index = 0
            index = 0
            for schema_item in schema:
                if schema_item[0] == old_tag and schema_item[-1] == "TAG":
                    get_index = index
                    tag_value.append(str(set_tag_value))
                    continue
                if schema_item[-1] == "TAG":
                    tag_value.append(str(schema_item[0]))
                    index += 1

            self.tdSql.query("show tables ")
            tables = self.tdSql.query_data
            sub_tablenames = []
            for table in tables:
                sub_tablenames.append(table[0])

            random_sub_table = random.sample(sub_tablenames, 1)[0]

            alter_length_sql = "alter table {} SET TAG {}=\"{}\" ".format(
                random_sub_table, old_tag , set_tag_value)
            
            self.tdSql.execute(alter_length_sql)
            self.tdSql.execute("reset query cache ")
            str_tags = " , ".join(tag_value)

            for i in range(100):
                extra_ts = extra_ts_start + i * self._ts_step
                replace_row[0] = str(extra_ts)
                replace_row[10] = str(extra_ts)

                str_value = " , ".join(replace_row)
                insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    "extra_schema_table", stbname, str_tags,  str_value)
                self.tdSql.execute(insert_sql)
                insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    random_sub_table, stbname, str_tags,  str_value)
                self.tdSql.execute(insert_sql)
            self.tdSql.query("select count(*) from {}".format(stbname))
            self.tdSql.checkData(0, 0, self._row_nums *
                                 self.tables_of_per_stable + 200)
            self.tdSql.query("select {} from {}".format(old_tag ,random_sub_table ))
            self.tdSql.checkData(0,0,set_tag_value)

        def alter_tasks_loop(alter_nums , alter_step_time , db_nums ,tag_nums , stable_nums):
            pass


    def alter_dbs(self,dbname):
        db_propertys = {"days": int(random.randint(1, 5)),
                    #   "keep": int(random.randint(10, 20)),
                      "blocks": int(random.randint(1, 6)*2),
                      "quorum": int(random.randint(0, 3)),
                      "comp": int(random.randint(0, 3)),
                      "minrows": int(random.randint(1, 3)*100),
                    #   "replica": int(random.randint(1, 3))
                      }
        alter_list = ['days', 'blocks',
                      'quorum', 'comp', 'minrows']
        random_key = random.sample(alter_list, 1)[0]
        random_value = db_propertys[random_key]
        sql = "alter database {} {} {}".format(
            dbname, random_key, random_value)

        db_propertys_index = {
            "days" : 6 ,
            "blocks" : 9 ,
            "quorum" : 5 ,
            "comp" : 14 , 
            "minrows" : 10 
        }
        # alter database  randomly
        try:
            self.tdSql.execute(sql)
            # check alter success
            self.tdSql.query("show databases")
            databases = self.tdSql.query_data
            for db in databases:
                if db[0] == dbname:
                    if not db[db_propertys_index[random_key]] == random_value:
                        print("alter sql :" , sql)
                        raise ("alter database wrong somethings")
                        break

        except Exception as e:
            pass


    def create_drop_stables(self, dbname ,stb_nums):
        pass

    
    def create_drop_tables(self, dbname ,stablename ,table_nums):
        pass

    def create_drop_dbs(self, db_nums):

        pass

    def run(self):

        # self.tdSql.execute("create database db0")
        # self.tdSql.execute('use db0')
        self.create_db_tables("db0", "st")
        self.insert_per_rows("db0", "st")
        self.tdSql.query("describe st")
        self.alter_dbs("db0")
        # self.alter_tags()
        # get before schema and last rows

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
