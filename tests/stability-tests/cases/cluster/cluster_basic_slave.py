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
import os ,taos
from shutil import ExecError
import time
import random
import time
from types import DynamicClassAttribute
from numpy import append
import taos
import copy
import datetime
from itertools import product
from itertools import combinations
import subprocess
import logging
from faker import Faker
from taostest import TDCase
from Query.queryutil.createdata import *
from Query.queryutil.where import *
from itertools import product
from itertools import combinations
import subprocess
from taostest.util.file import dict2file
from taostest.util.remote import Remote
from taostest.util.sql import TDSql
import subprocess
import threading

fake = Faker('zh_CN')

class myTDSQL(TDSql):
    def __init__(self, logger, run_log_dir, set_error_msg):
        super().__init__(logger, run_log_dir, set_error_msg)
        self._conn =  self.get_connection(None ,"dnode_3:6030")

class TestCluster(TDCase):
    
    def init(self):

        self._ts = 1420041600000  # 2015-01-01 00:00:00  this is begin time for first record
        self._ts_step = 1
        self._row_nums = 2
        self._col_nums = 12  # col types is float almost
        self._stables_nums = 100
        self._table_nums = 100
        self.tables_of_per_stable = 2
        self._tags_nums = 10
        self._replica = 3
        self._db_nums = 100
        self._alter_times = 1000
        self._dbs = 0
        self._used_dbs = []
        self._tags = 0
        self._used_tags = []
        self._stablenames = 0
        self._used_stables = []
        self.tdSql = myTDSQL(logger = self.logger, run_log_dir = self.run_log_dir, set_error_msg=self.set_error_msg)
        
        

    def get_db_name(self , n=1): # n is db nums for all , default = 1  return an string list

        
        dbnames = []
        for db in range(n):
            dbname = 'db_%d'%self._dbs
            dbnames.append(dbname)
            self._dbs  += 1

        self._used_dbs += dbnames
        
        return dbnames

    def get_stable_name(self, n=1):

        stablenames = []
        for stable in range(n):
            stablename  = "stable_%d"%self._stablenames
            stablenames.append(stablename)
            self._stablenames += 1

        self._used_stables += stablenames

        return stablenames

    def get_tag_name(self,n=1 ):

        tagnames = []
        for alter_tag in range(n):
            tagname = "alter_tag_%d"%self._tags
            tagnames.append(tagname)
            self._tags +=1 
        self._used_tags +=tagnames
        return tagnames

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

    def insert_extra_rows(self , db_name ,stable_name ,row_nums):

        self.tdSql.execute("use {}".format(db_name))

        tags = []
        for i in range(self._tags_nums):
            tags.append("tag_%d" % i)
        str_tags = "'" + "' ,'".join(tags) + "'"

        for row in range(row_nums):  # per insert will write very slowly

            ts = self._ts + self._ts_step * self._row_nums + row*self._ts_step  # extra start ts is end of last regular insert rows 
            rows = self.generate_insert_rows(ts)

            for sub_table_ind in range(self.tables_of_per_stable):
                insert_sql = 'insert into sub_{}_{} using {} tags ({}) values ({})'.format(
                    stable_name, sub_table_ind, stable_name, str_tags, rows)
                self.tdSql.execute(insert_sql)    

    def insert_extra_rows_for_table(self , db_name ,stable_name , sub_table_name , row_nums): 
        self.tdSql.execute("use {}".format(db_name))
        tags = []
        for i in range(self._tags_nums):
            tags.append("tag_%d" % i)
        str_tags = "'" + "' ,'".join(tags) + "'"
        for row in range(row_nums):  # per insert will write very slowly
            ts = self._ts + self._ts_step * self._row_nums + row*self._ts_step  # extra start ts is end of last regular insert rows 
            rows = self.generate_insert_rows(ts)
            insert_sql = 'insert into {} using {} tags ({}) values ({})'.format(
                    sub_table_name, stable_name, str_tags, rows)
            self.tdSql.execute(insert_sql) 

 
    def alter_tags(self , dbname , stbname ,random_timesleep):

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
                # insert_sql = " insert into {} using {} tags ({}) values({})".format(
                #     random_sub_table, stbname, str_tags,  str_value)
                # self.tdSql.execute(insert_sql)

                self.tdSql.execute("use {}".format(dbname))

            self.tdSql.execute("reset query cache")
            self.tdSql.query("select count(*) from {}".format(stbname))
            self.tdSql.checkData(0, 0, self._row_nums *
                                 self.tables_of_per_stable + 100)
            self.tdSql.execute("drop table extra_schema_table")
            # self.tdSql.execute("drop table {}".format(random_sub_table))
            
        def _ADD_TAG(dbname, stbname, new_tag):
            self.tdSql.execute("use {}".format(dbname))
            alter_length_sql = "alter stable {} ADD TAG {} nchar(32)".format(
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
            tag_value.append(new_tag)

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
                # insert_sql = " insert into {} using {} tags ({}) values({})".format(
                #     random_sub_table, stbname, str_tags,  str_value)
                # self.tdSql.execute(insert_sql)
            self.tdSql.execute("reset query cache ")
            self.tdSql.query("select count(*) from {}".format(stbname))
            self.tdSql.checkData(0, 0, self._row_nums *
                                 self.tables_of_per_stable + 100)
            self.tdSql.execute(" drop table extra_schema_table ")
        
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
                # insert_sql = " insert into {} using {} tags ({}) values({})".format(
                #     random_sub_table, stbname, str_tags,  str_value)
                # self.tdSql.execute(insert_sql)
            self.tdSql.execute("reset query cache ")
            self.tdSql.query("select count(*) from {}".format(stbname))
            self.tdSql.checkData(0, 0, self._row_nums *
                                 self.tables_of_per_stable + 100)
            self.tdSql.execute(" drop table extra_schema_table ")

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
                # insert_sql = " insert into {} using {} tags ({}) values({})".format(
                #     random_sub_table, stbname, str_tags,  str_value)
                # self.tdSql.execute(insert_sql)
            self.tdSql.execute("reset query cache ")
            self.tdSql.query("select count(*) from {}".format(stbname))
            self.tdSql.checkData(0, 0, self._row_nums *
                                 self.tables_of_per_stable + 100)
            self.tdSql.execute("drop table extra_schema_table ")

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
                # insert_sql = " insert into {} using {} tags ({}) values({})".format(
                #     random_sub_table, stbname, str_tags,  str_value)
                # self.tdSql.execute(insert_sql)
            self.tdSql.execute("reset query cache ")
            self.tdSql.query("select count(*) from {}".format(stbname))
            self.tdSql.checkData(0, 0, self._row_nums *
                                 self.tables_of_per_stable + 100)
            self.tdSql.execute("reset query cache ")
            self.tdSql.query("select {} from {}".format(old_tag ,random_sub_table ))
            self.tdSql.checkData(0,0,set_tag_value)
            self.tdSql.execute("drop table extra_schema_table")
        
        #====================== main logtic ========================

        self.tdSql.execute("drop database  if exists {}".format(dbname))
        self.create_db_tables(dbname , stbname)
        self.insert_per_rows(dbname , stbname)
        
        # set an constantly alter tag task 
        for i in range(100):
            _MODIFY_TAG(dbname , stbname , "tag_0" , i+33)
            # time.sleep(random.randint(1,random_timesleep))
        self.tdSql.execute("drop database {}".format(dbname))
        self.create_db_tables(dbname , stbname)
        self.insert_per_rows(dbname , stbname)
        alter_tags_lists = self.get_tag_name(100)

        has_add_tags = []
        for alter_tag in alter_tags_lists:
            _ADD_TAG(dbname , stbname , alter_tag)
            has_add_tags.append(alter_tag)
            drop_tag = random.sample(has_add_tags,1)[0]
            _CHANGE_TAG(dbname , stbname ,drop_tag ,drop_tag+"_change"  )
            _SET_TAG(dbname , stbname ,drop_tag+"_change" , drop_tag+"_set")
            _DROP_TAG(dbname , stbname , drop_tag+"_change")
            has_add_tags.remove(drop_tag)
            
            # time.sleep(random.randint(1,random_timesleep))

    def alter_dbs(self,dbname):
        db_propertys = {"days": int(random.randint(1, 5)),
                    #   "keep": int(random.randint(10, 20)),
                    #   "blocks": int(random.randint(1, 6)*2),
                    #   "quorum": int(random.randint(0, 3)),
                      "comp": int(random.randint(0, 3)),
                      "minrows": int(random.randint(1, 3)*100),
                    #   "replica": int(random.randint(1, 3))
                      }
        alter_list = ['days',  'comp', 'minrows']
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
        # try:
        #     self.tdSql.execute(sql)
        #     # check alter success
        #     self.tdSql.query("show databases")
        #     databases = self.tdSql.query_data
        #     for db in databases:
        #         if db[0] == dbname:
        #             if not db[db_propertys_index[random_key]] == random_value:
        #                 print("alter sql :" , sql)
        #                 raise BaseException("alter database wrong somethings")
        #                 break

        # except Exception as e:
        #     pass


    def create_drop_stables(self, dbname ,stb_nums):
        self.tdSql.execute("use {}".format(dbname))
        stablenames = self.get_stable_name(stb_nums)
        for stablename in stablenames:
            self.create_db_tables(dbname , stablename)
            self.insert_per_rows(dbname , stablename)
        drop_stables = random.sample(stablenames,int(stb_nums/2)+1)
        for drop_stable in drop_stables:
            drop_sql = "drop stable {}".format(drop_stable)
            # for _ in range(100):  # drop stable 100 times
            #     self.tdSql.execute(drop_sql)
            #     self.alter_dbs(dbname)
            #     self.create_db_tables(dbname ,drop_stable)
            #     self.insert_per_rows(dbname ,drop_stable)
            self.tdSql.execute(drop_sql)
        # check stables if droped should not exists
        self.tdSql.query("show stables")
        stables_data = self.tdSql.query_data
        stables_list = []
        for stable in stables_data:
            stables_list.append(stable[0])
        for drop_stable in drop_stables:
            if drop_stable in stables_list:
                raise BaseException (" {} has been dropped before ".format(drop_stable))
        # insert extra rows 
        exists_tables = list(set(stablenames)-set(drop_stables))
        for stable in exists_tables:
            self.insert_extra_rows(dbname ,stable,100 )
        for stable in drop_stables:
            self.create_db_tables(dbname ,stable)
            self.insert_extra_rows(dbname ,stable,100)
        # check rows total
        for stable in exists_tables:
            self.tdSql.execute("reset query cache ")
            self.tdSql.query("select count(*) from {}".format(stable))
            self.tdSql.checkData(0,0,self.tables_of_per_stable*(self._row_nums + 100))
        for stable in drop_stables:
            self.tdSql.execute("reset query cache ")
            self.tdSql.query("select count(*) from {}".format(stable))
            self.tdSql.checkData(0,0,self.tables_of_per_stable* 100)
        

    def create_drop_tables(self, dbname ,stablename ,table_nums):
        self.tdSql.execute("use {}".format(dbname))
        if table_nums >self.tables_of_per_stable:
            table_nums =self.tables_of_per_stable
        self.tdSql.execute("drop database {}".format(dbname))
        self.create_db_tables(dbname ,stablename)
        self.insert_per_rows(dbname ,stablename)

        # get tables
        self.tdSql.query("show tables")
        tables_data = self.tdSql.query_data
        tables_list = []
        for table in tables_data:
            tables_list.append(table[0])
        # get drop_tables
        drop_tables = random.sample(tables_list , table_nums)
        
        # drop tables  
        for drop_table in drop_tables:
            drop_sql = " drop table {} ".format(drop_table)

            for _ in range(100):    # drop table 100 times
                self.tdSql.execute(drop_sql)
                self.alter_dbs(dbname)
                self.insert_extra_rows_for_table(dbname ,stablename , drop_table , 100) # it will auto create drop table
            # check rows 
            self.tdSql.execute("reset query cache ")
            self.tdSql.query(" select count(*) from {}".format(drop_table))
            self.tdSql.checkData(0,0,100)
            self.tdSql.execute(drop_sql)

        # insert data and it will auto create droped tables

        self.insert_extra_rows(dbname ,stablename ,100)

        # check rows for insert again 

        exists_tables = list(set(tables_list) - set(drop_tables))

        for table in exists_tables:
            self.tdSql.execute("reset query cache ")
            self.tdSql.query(" select count(*) from {} ".format(table))
            self.tdSql.checkData(0,0,self._row_nums +100)
        for table in drop_tables:
            self.tdSql.execute("reset query cache ")
            self.tdSql.query(" select count(*) from {} ".format(table))
            self.tdSql.checkData(0,0,100)


    def create_drop_dbs(self, db_nums):

        dbs = self.get_db_name(db_nums)
        
        drop_dbs = []
        # prepare dbs and insert data
        for index , db in enumerate(dbs):
            self.tdSql.execute("drop database if exists {}".format(db))
            self.create_db_tables(db , "stable_drop_dbs")
            self.insert_per_rows(db , "stable_drop_dbs")
            if index % 3 ==0:
                drop_dbs.append(db)
        
        print("create dbs done! ")

        # drop dbs 
        for drop_db in drop_dbs:
            drop_sql = "drop database {}".format(drop_db)   # drop 100 times
            create_sql = "create database {} replica {}".format(drop_db,self._replica)
            for _ in range(100):
                self.tdSql.execute(drop_sql)
                self.tdSql.execute("reset query cache")
                self.tdSql.execute(create_sql)
                self.alter_dbs(drop_db)
                # self.create_db_tables(drop_db , "stable_drop_dbs")
                # self.insert_per_rows(drop_db , "stable_drop_dbs")
                self.tdSql.execute("show databases")
                self.tdSql.execute("reset query cache")
            self.tdSql.execute(drop_sql)
        # check database not exists
        self.tdSql.execute("reset query cache")
        self.tdSql.query("show databases")
        databases = self.tdSql.query_data
        for db in databases:
            if db[0] in drop_dbs:
                raise BaseException(f"db {db[0]} should has been droped ")
                break
        # create database again and insert extra rows 
        for drop_db in drop_dbs:
            create_sql = "create database {} replica {}".format(drop_db,self._replica)
            self.tdSql.execute(create_sql)
        
        # insert extra data for all  databases

        exists_dbs = list(set(dbs) - set(drop_dbs))
        for exist_db in exists_dbs:
            self.insert_extra_rows(exist_db ,"stable_drop_dbs" ,100 )
            # check rows 
            self.tdSql.execute("reset query cache ")
            self.tdSql.query("select count(*) from {}.stable_drop_dbs".format(exist_db))
            self.tdSql.checkData(0,0,self.tables_of_per_stable*self._row_nums +100*self.tables_of_per_stable)
        for drop_db in drop_dbs:
            self.create_db_tables(drop_db ,"stable_drop_dbs")
            self.insert_extra_rows(drop_db ,"stable_drop_dbs" ,100 )
            # check rows 
            self.tdSql.execute("reset query cache ")
            self.tdSql.query("select count(*) from {}.stable_drop_dbs".format(drop_db))
            self.tdSql.checkData(0,0,100*self.tables_of_per_stable)

    def basic_alter_shema_task(self, db_nums , stable_nums,table_nums , time_sleep):
        self.create_drop_dbs(db_nums)  # create drop database task
        dbs = self.get_db_name(db_nums)
        for db in dbs:
            self.tdSql.execute("drop database  if exists {}".format(db))
            self.tdSql.execute("create database {} replica {}".format(db ,self._replica))
            self.create_drop_stables(db,stable_nums)
            self.tdSql.execute("drop database  if exists {}".format(db))
            # time.sleep(time_sleep)
        dbs = self.get_db_name(db_nums)
        for db in dbs:
            self.tdSql.execute("drop database  if exists {}".format(db))
            self.tdSql.execute("create database {} replica {}".format(db ,self._replica))
            self.create_drop_tables(db ,"drop_table", table_nums)
            self.tdSql.execute("drop database  if exists {}".format(db))
            # time.sleep(time_sleep)
        dbs = self.get_db_name(db_nums)
        stables = self.get_stable_name(stable_nums)
        for db in dbs:
            for stable in stables:
                self.alter_tags(db , stable, 1)  # alter tags will auto reset db and stables
                self.tdSql.execute("drop database  if exists {}".format(db))
                # time.sleep(time_sleep)


    def run(self):
   
        self.basic_alter_shema_task( db_nums=2 , stable_nums=2,table_nums=2 , time_sleep=1)
        self.envMgr.stopDnode("dnode_1:6030")
        self.tdSql.execute("create database dbca")

        
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
