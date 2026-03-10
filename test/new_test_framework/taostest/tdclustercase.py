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
import random
import threading
import time
from faker import Faker

from .tdcase import TDCase
fake = Faker('zh_CN')
class ClusterCase(TDCase):

    def init(self):
        self.ts = 1420041600000  # 2015-01-01 00:00:00  this is begin time for first record
        # case SECCUSS or FAILED
        self._status = True
        self._conf = self.get_component_by_name(
            "taosd")[0]
        self.params = {"_ts" : 1420041600000 , "_ts_step":1 ,"_row_nums":2 ,"_col_nums":12 , 
        "tables_of_per_stable":2 ,"_tags_nums" : 10 , "_replica" :3 }


    def insert_into_table(self, db_name, stable_name, table_name, table_num, row_num, replicas, endpoint: str = None):
        '''
        thread to write data
        @db_name:
        @stable_name:
        @table_num:
        @row_num: row number per table
        @replicas:
        '''
        if not endpoint is None:
            client_0 = self.tdSql.get_connection(None, endpoint)
        else:
            client_0 = self.tdSql.get_connection(self._conf)
        # clean
        sql = "drop database if exists %s" % (db_name)
        self.logger.info(sql)
        client_0.execute(sql)
        # create database
        sql = "create database %s replica %d" % (db_name, replicas)
        self.logger.info(sql)
        client_0.execute(sql)
        # use database
        sql = "use %s" % (db_name)
        self.logger.info(sql)
        client_0.execute(sql)
        # create table
        sql = "create stable %s (ts timestamp, c1 int) tags(t1 int)" % (stable_name)
        self.logger.info(sql)
        client_0.execute(sql)
        i = 0
        while i < table_num:
            tb = f"{table_name}_{i}"
            # create table
            sql = "create table %s using %s tags( %d )" % (tb, stable_name, i)
            self.logger.info(sql)
            client_0.execute(sql)
            i = i + 1
            j = 0
            while j < row_num:
                ts = self.ts + j
                k = 0
                n = 50
                if row_num - j < n:
                    n = row_num - j
                value_statement = ""
                while k < n:
                    value_statement = f"{value_statement} ({ts}+{k}a, {j})"
                    k = k + 1
                j = j + n
                sql = f"insert into {tb} values {value_statement}"
                # self.logger.info(sql)
                client_0.execute(sql)
        client_0.close()
        self.logger.info("write thread exit")

    
    def basic_alter_shema_task(self, db_nums , stable_nums,table_nums , time_sleep ,params , endpoint: str = None , check_status=True):

        if not endpoint is None:
            client_0 = self.tdSql.get_connection(None, endpoint)
        else:
            client_0 = self.tdSql.get_connection(self._conf)
        check_status = check_status
        def checkData(row, col, data, sql ):
            if check_status:
                result = client_0.query(sql)
                query_data = result.fetch_all()
                real_data = query_data[row][col]
                
                # 直接比较，相等则返回
                if real_data == data:
                    self.logger.debug(f"checkData success, row={row} col={col} expect={data} real={real_data}")
                    return True
                else:
                    return False
            else:
                return True

            
        params["_dbs"]=0 
        params["_tags"]=0
        params["_stablenames"]=0
        def get_db_name(n=1): # n is db nums for all , default = 1  return an string list
            
            dbnames = []
            for db in range(n):
                dbname = 'db_%d'%params["_dbs"]
                dbnames.append(dbname)
                params["_dbs"]  += 1
            
            return dbnames

        def get_stable_name( n=1):

            stablenames = []
            for stable in range(n):
                stablename  = "stable_%d"%params["_stablenames"]
                stablenames.append(stablename)
                params["_stablenames"] += 1

            return stablenames

        def get_tag_name( n=1 ):

            tagnames = []
            for alter_tag in range(n):
                tagname = "alter_tag_%d"%params["_tags"]
                tagnames.append(tagname)
                params["_tags"] +=1 
            return tagnames

        def create_db_tables( db_name, stable_name):

            Dynamic_cols = ''
            if params["_col_nums"] <= 11:
                col_nums = params["_col_nums"]
            else:
                col_nums = params["_col_nums"]-11

            for i in range(col_nums):  # there are 11 basic rows
                if i == col_nums - 1:
                    Dynamic_cols += "col_%i  float" % i
                else:
                    Dynamic_cols += "col_%i  float ," % i

            if params["_col_nums"] > 11:
                basic_cols = 'q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , '
            else:
                basic_cols = ''
            Dynamic_tags = ''
            for i in range(params["_tags_nums"]):
                if i == params["_tags_nums"]-1:
                    Dynamic_tags += "tag_%i  nchar(32)" % i
                else:
                    Dynamic_tags += "tag_%i  nchar(32) ," % i

            create_db_sql = ' create database if not exists {} replica {}'.format(
                db_name, params["_replica"])
            create_table_sqls = "create stable {} (ts timestamp ,{} {} ) tags ({} )".format(
                stable_name, basic_cols, Dynamic_cols, Dynamic_tags)


            # create database and tables
            client_0.execute(create_db_sql)
            self.logger.info(create_db_sql)
            client_0.execute("use {}".format(db_name))
            self.logger.info("use {}".format(db_name))
            client_0.execute(create_table_sqls)
            self.logger.info(create_table_sqls)

        def generate_insert_rows(ts):
            values = []

            for i in range(params["_col_nums"]):
                values.append(str(float(ts + i*0.1)))

            basic_values = []
            if params["_col_nums"] > 11:  # if col_nums >11 ,it contain 11 basic rows
                basic_values = [str(fake.random_int(min=-2147483647, max=2147483647, step=1)),
                                str(fake.random_int(min=-9223372036854775807,
                                    max=9223372036854775807, step=1)),
                                str(fake.random_int(min=-32767, max=32767, step=1)
                                    ), str(fake.random_int(min=-127, max=127, step=1)),
                                str(fake.pyfloat()), str(fake.pyfloat()), '"true"', "'" + str(fake.pystr()) + "'", "'" + str(fake.pystr()) + "'", str(ts + i)]
            # auto create table
            if params["_col_nums"] > 11:
                values = values[11:]
            str_values = " ,".join(values)
            str_basic_values = " ,".join(basic_values)
            row_values = "{} ,{} , {}".format(ts, str_basic_values, str_values)
            return row_values

        def insert_per_rows(db_name, stable_name):

            client_0.execute("use {}".format(db_name))
            self.logger.info("use {}".format(db_name))

            tags = []
            for i in range(params["_tags_nums"]):
                tags.append("tag_%d" % i)
            str_tags = "'" + "' ,'".join(tags) + "'"

            for row in range(params["_row_nums"]):  # per insert will write very slowly

                ts = params["_ts"] + params["_ts_step"] * row
                rows = generate_insert_rows(ts)

                for sub_table_ind in range(params["tables_of_per_stable"]):
                    insert_sql = 'insert into sub_{}_{} using {} tags ({}) values ({})'.format(
                        stable_name, sub_table_ind, stable_name, str_tags, rows)
                    client_0.execute(insert_sql)
                    self.logger.info(insert_sql)

        def insert_extra_rows(db_name ,stable_name ,row_nums):

            client_0.execute("use {}".format(db_name))
            self.logger.info("use {}".format(db_name))

            tags = []
            for i in range(params["_tags_nums"]):
                tags.append("tag_%d" % i)
            str_tags = "'" + "' ,'".join(tags) + "'"

            for row in range(row_nums):  # per insert will write very slowly

                ts = params["_ts"] + params["_ts_step"] * params["_row_nums"] + row*params["_ts_step"]  # extra start ts is end of last regular insert rows 
                rows = generate_insert_rows(ts)

                for sub_table_ind in range(params["tables_of_per_stable"]):
                    insert_sql = 'insert into sub_{}_{} using {} tags ({}) values ({})'.format(
                        stable_name, sub_table_ind, stable_name, str_tags, rows)
                    client_0.execute(insert_sql)
                    self.logger.info(insert_sql)    

        def insert_extra_rows_for_table( db_name ,stable_name , sub_table_name , row_nums): 
            client_0.execute("use {}".format(db_name))
            self.logger.info("use {}".format(db_name))
            tags = []
            for i in range(params["_tags_nums"]):
                tags.append("tag_%d" % i)
            str_tags = "'" + "' ,'".join(tags) + "'"
            for row in range(row_nums):  # per insert will write very slowly
                ts = params["_ts"] + params["_ts_step"] * params["_row_nums"] + row*params["_ts_step"]  # extra start ts is end of last regular insert rows 
                rows = generate_insert_rows(ts)
                insert_sql = 'insert into {} using {} tags ({}) values ({})'.format(
                        sub_table_name, stable_name, str_tags, rows)
                client_0.execute(insert_sql)
                self.logger.info(insert_sql) 

    
        def alter_tags(dbname , stbname ,random_timesleep):

            def _MODIFY_TAG(dbname, stbname, old_tag, set_length):
                client_0.execute("use {}".format(dbname))
                alter_length_sql = "alter stable {} modify TAG {} nchar({})".format(
                    stbname, old_tag, set_length)

                # get before schema and last rows
                result = client_0.query("describe {}".format(stbname))
                schema = result.fetch_all()
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
                result = client_0.query(" select last(*) from {}".format(stbname))
                last_row = result.fetch_all()
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

                extra_ts_start = params["_ts"] + params["_ts_step"] * params["_row_nums"]
                client_0.execute(alter_length_sql)
                self.logger.info(alter_length_sql)
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ")
                result = client_0.query("describe {}".format(stbname))
                self.logger.info("describe {}".format(stbname))
                schema = result.fetch_all()

                result = client_0.query("show tables ")
                self.logger.info("show tables ")
                tables = result.fetch_all()
                sub_tablenames = []
                for table in tables:
                    sub_tablenames.append(table[0])

                random_sub_table = random.sample(sub_tablenames, 1)[0]

                for i in range(100):
                    extra_ts = extra_ts_start + i * params["_ts_step"]
                    replace_row[0] = str(extra_ts)
                    replace_row[10] = str(extra_ts)

                    str_value = " , ".join(replace_row)
                    insert_sql = " insert into {} using {} tags ({}) values({})".format(
                        "extra_schema_table", stbname, str_tags,  str_value)
                    client_0.execute(insert_sql)
                    self.logger.info(insert_sql)
                    # insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    #     random_sub_table, stbname, str_tags,  str_value)
                    # client_0.execute(insert_sql)

                    client_0.execute("use {}".format(dbname))
                    self.logger.info("use {}".format(dbname))
                client_0.execute("reset query cache")
                self.logger.info("reset query cache")

                query_sql = "select count(*) from {}".format(stbname)
                checkData(0, 0, params["_row_nums"] *
                                    params["tables_of_per_stable"] + 100,query_sql)
                client_0.execute("drop table extra_schema_table")
                self.logger.info("drop table extra_schema_table")
                # client_0.execute("drop table {}".format(random_sub_table))
                
            def _ADD_TAG(dbname, stbname, new_tag):
                client_0.execute("use {}".format(dbname))
                self.logger.info("use {}".format(dbname))
                alter_length_sql = "alter stable {} ADD TAG {} nchar(32)".format(
                    stbname, new_tag)

                # get before schema and last rows
                result = client_0.query("describe {}".format(stbname))
                schema = result.fetch_all()
                result = client_0.query(" select last(*) from {}".format(stbname))
                last_row = result.fetch_all()
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

                extra_ts_start = params["_ts"] + params["_ts_step"] * params["_row_nums"]
                result = client_0.query("describe {}".format(stbname))
                schema = result.fetch_all()
                tag_value = []
                index = 0
                for schema_item in schema:
                    if schema_item[-1] == "TAG":
                        tag_value.append(str(schema_item[0]))
                        index += 1
                tag_value.append(new_tag)

                client_0.execute(alter_length_sql)
                self.logger.info(alter_length_sql)
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ")
                str_tags = " , ".join(tag_value)

                result = client_0.query("show tables ")
                self.logger.info("show tables ")
                tables = result.fetch_all()
                sub_tablenames = []
                for table in tables:
                    sub_tablenames.append(table[0])

                random_sub_table = random.sample(sub_tablenames, 1)[0]

                for i in range(100):
                    extra_ts = extra_ts_start + i * params["_ts_step"]
                    replace_row[0] = str(extra_ts)
                    replace_row[10] = str(extra_ts)

                    str_value = " , ".join(replace_row)
                    insert_sql = " insert into {} using {} tags ({}) values({})".format(
                        "extra_schema_table", stbname, str_tags,  str_value)
                    client_0.execute(insert_sql)
                    self.logger.info(insert_sql)
                    # insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    #     random_sub_table, stbname, str_tags,  str_value)
                    # client_0.execute(insert_sql)
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ")
                query_sql = "select count(*) from {}".format(stbname)
                checkData(0, 0, params["_row_nums"] *
                                    params["tables_of_per_stable"] + 100,query_sql)
                client_0.execute(" drop table extra_schema_table ")
                self.logger.info(" drop table extra_schema_table ")
            
            def _DROP_TAG(dbname, stbname, old_tag):
                client_0.execute("use {}".format(dbname))
                self.logger.info("use {}".format(dbname))
                alter_length_sql = "alter stable {} DROP TAG {}".format(
                    stbname, old_tag)

                # get before schema and last rows
                result = client_0.query("describe {}".format(stbname))
                schema = result.fetch_all()
                result = client_0.query(" select last(*) from {}".format(stbname))
                last_row = result.fetch_all()
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

                extra_ts_start = params["_ts"] + params["_ts_step"] * params["_row_nums"]
                result = client_0.query("describe {}".format(stbname))
                schema = result.fetch_all()
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

                client_0.execute(alter_length_sql)
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ")
                str_tags = " , ".join(tag_value)

                result = client_0.query("show tables ")
                self.logger.info("show tables ")
                tables = result.fetch_all()
                sub_tablenames = []
                for table in tables:
                    sub_tablenames.append(table[0])

                random_sub_table = random.sample(sub_tablenames, 1)[0]

                for i in range(100):
                    extra_ts = extra_ts_start + i * params["_ts_step"]
                    replace_row[0] = str(extra_ts)
                    replace_row[10] = str(extra_ts)

                    str_value = " , ".join(replace_row)
                    insert_sql = " insert into {} using {} tags ({}) values({})".format(
                        "extra_schema_table", stbname, str_tags,  str_value)
                    client_0.execute(insert_sql)
                    self.logger.info(insert_sql)
                    # insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    #     random_sub_table, stbname, str_tags,  str_value)
                    # client_0.execute(insert_sql)
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ")
                query_sql = "select count(*) from {}".format(stbname)
                checkData(0, 0, params["_row_nums"] *
                                    params["tables_of_per_stable"] + 100,query_sql)
                client_0.execute(" drop table extra_schema_table ")
                self.logger.info(" drop table extra_schema_table ")

            def _CHANGE_TAG(dbname, stbname, old_tag, new_tag):

                client_0.execute("use {}".format(dbname))
                self.logger.info("use {}".format(dbname))
                alter_length_sql = "alter stable {} CHANGE TAG {} {}".format(
                    stbname, old_tag , new_tag)

                # get before schema and last rows
                result = client_0.query("describe {}".format(stbname))
                schema = result.fetch_all()
                result = client_0.query(" select last(*) from {}".format(stbname))
                last_row = result.fetch_all()
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

                extra_ts_start = params["_ts"] + params["_ts_step"] * params["_row_nums"]
                result = client_0.query("describe {}".format(stbname))
                schema = result.fetch_all()
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

                client_0.execute(alter_length_sql)
                self.logger.info(alter_length_sql)
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ")
                str_tags = " , ".join(tag_value)

                result = client_0.query("show tables ")
                tables = result.fetch_all()
                sub_tablenames = []
                for table in tables:
                    sub_tablenames.append(table[0])

                random_sub_table = random.sample(sub_tablenames, 1)[0]

                for i in range(100):
                    extra_ts = extra_ts_start + i * params["_ts_step"]
                    replace_row[0] = str(extra_ts)
                    replace_row[10] = str(extra_ts)

                    str_value = " , ".join(replace_row)
                    insert_sql = " insert into {} using {} tags ({}) values({})".format(
                        "extra_schema_table", stbname, str_tags,  str_value)
                    client_0.execute(insert_sql)
                    self.logger.info(insert_sql)
                    # insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    #     random_sub_table, stbname, str_tags,  str_value)
                    # client_0.execute(insert_sql)
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ")
                query_sql = "select count(*) from {}".format(stbname)
                checkData(0, 0, params["_row_nums"] *
                                    params["tables_of_per_stable"] + 100 ,query_sql)
                client_0.execute("drop table extra_schema_table ")
                self.logger.info("drop table extra_schema_table ")

            def _SET_TAG(dbname, stbname, old_tag, set_tag_value):
                client_0.execute("use {}".format(dbname))
                self.logger.info("use {}".format(dbname))
                
                # get before schema and last rows
                result = client_0.query("describe {}".format(stbname))
                schema = result.fetch_all()
                result = client_0.query(" select last(*) from {}".format(stbname))
                last_row = result.fetch_all()
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
                extra_ts_start = params["_ts"] + params["_ts_step"] * params["_row_nums"]
                result = client_0.query("describe {}".format(stbname))
                schema = result.fetch_all()
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

                result = client_0.query("show tables ")
                tables = result.fetch_all()
                sub_tablenames = []
                for table in tables:
                    sub_tablenames.append(table[0])

                random_sub_table = random.sample(sub_tablenames, 1)[0]

                alter_length_sql = "alter table {} SET TAG {}=\"{}\" ".format(
                    random_sub_table, old_tag , set_tag_value)
                
                client_0.execute(alter_length_sql)
                self.logger.info(alter_length_sql)
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ")
                str_tags = " , ".join(tag_value)

                for i in range(100):
                    extra_ts = extra_ts_start + i * params["_ts_step"]
                    replace_row[0] = str(extra_ts)
                    replace_row[10] = str(extra_ts)

                    str_value = " , ".join(replace_row)
                    insert_sql = " insert into {} using {} tags ({}) values({})".format(
                        "extra_schema_table", stbname, str_tags,  str_value)
                    client_0.execute(insert_sql)
                    # insert_sql = " insert into {} using {} tags ({}) values({})".format(
                    #     random_sub_table, stbname, str_tags,  str_value)
                    # client_0.execute(insert_sql)
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ")
                query_sql = "select count(*) from {}".format(stbname)
                checkData(0, 0, params["_row_nums"] *
                                    params["tables_of_per_stable"] + 100 ,query_sql)
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ")
                query_sql = "select {} from {}".format(old_tag ,random_sub_table )
                checkData(0,0,set_tag_value,query_sql)
                client_0.execute("drop table extra_schema_table")
                self.logger.info("drop table extra_schema_table")
            
            #====================== main logtic ========================

            client_0.execute("drop database  if exists {}".format(dbname))
            create_db_tables(dbname , stbname)
            insert_per_rows(dbname , stbname)
            
            # set an constantly alter tag task 
            for i in range(100):
                _MODIFY_TAG(dbname , stbname , "tag_0" , i+33)
                # time.sleep(random.randint(1,random_timesleep))
            client_0.execute("drop database {}".format(dbname))
            self.logger.info("drop database {}".format(dbname))
            create_db_tables(dbname , stbname)
            insert_per_rows(dbname , stbname)
            alter_tags_lists = get_tag_name(100)

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

        def alter_dbs(dbname):
            db_propertys = {"days": int(random.randint(1, 5)),
                        #   "keep": int(random.randint(10, 20)),
                        "blocks": int(random.randint(1, 6)*2),
                        # "quorum": int(random.randint(0, 3)),
                        # "comp": int(random.randint(0, 3)),
                        "minrows": int(random.randint(1, 3)*100),
                        #   "replica": int(random.randint(1, 3))
                        }
            alter_list = ['days', 'blocks',
                      'minrows']
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
                client_0.execute(sql)
                self.logger.info(sql)
                # check alter success
                result = client_0.query("show databases")
                databases = result.fetch_all()
                for db in databases:
                    if db[0] == dbname:
                        if not db[db_propertys_index[random_key]] == random_value:
                            print("alter sql :" , sql)
                            raise BaseException("alter database wrong somethings")
                            break

            except Exception as e:
                pass


        def create_drop_stables(dbname ,stb_nums):
            client_0.execute("use {}".format(dbname))
            stablenames = get_stable_name(stb_nums)
            for stablename in stablenames:
                create_db_tables(dbname , stablename)
                insert_per_rows(dbname , stablename)
            drop_stables = random.sample(stablenames,int(stb_nums/2)+1)
            for drop_stable in drop_stables:
                drop_sql = "drop stable {}".format(drop_stable)
                # for _ in range(100):  # drop stable 100 times
                #     client_0.execute(drop_sql)
                #     alter_dbs(dbname)
                #     create_db_tables(dbname ,drop_stable)
                #     insert_per_rows(dbname ,drop_stable)
                client_0.execute(drop_sql)
                self.logger.info(drop_sql)
            # check stables if droped should not exists
            result = client_0.query("show stables")
            self.logger.info("show stables")
            stables_data = result.fetch_all()
            stables_list = []
            for stable in stables_data:
                stables_list.append(stable[0])
            for drop_stable in drop_stables:
                if drop_stable in stables_list:
                    raise BaseException (" {} has been dropped before ".format(drop_stable))
            # insert extra rows 
            exists_tables = list(set(stablenames)-set(drop_stables))
            for stable in exists_tables:
                insert_extra_rows(dbname ,stable,100 )
            for stable in drop_stables:
                create_db_tables(dbname ,stable)
                insert_extra_rows(dbname ,stable,100)
            # check rows total
            for stable in exists_tables:
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ") 
                query_sql = "select count(*) from {}".format(stable)
                checkData(0,0,params["tables_of_per_stable"]*(params["_row_nums"] + 100),query_sql)
            for stable in drop_stables:
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ") 
                query_sql = "select count(*) from {}".format(stable)
                checkData(0,0,params["tables_of_per_stable"]* 100,query_sql)
            

        def create_drop_tables(dbname ,stablename ,table_nums):
            client_0.execute("use {}".format(dbname))
            if table_nums >params["tables_of_per_stable"]:
                table_nums =params["tables_of_per_stable"]
            client_0.execute("drop database {}".format(dbname))
            create_db_tables(dbname ,stablename)
            insert_per_rows(dbname ,stablename)

            # get tables
            result = client_0.query("show tables")
            self.logger.info("show tables")
            tables_data = result.fetch_all()
            tables_list = []
            for table in tables_data:
                tables_list.append(table[0])
            # get drop_tables
            drop_tables = random.sample(tables_list , table_nums)
            
            # drop tables  
            for drop_table in drop_tables:
                drop_sql = " drop table {} ".format(drop_table)

                for _ in range(100):    # drop table 100 times
                    client_0.execute(drop_sql)
                    alter_dbs(dbname)
                    insert_extra_rows_for_table(dbname ,stablename , drop_table , 100) # it will auto create drop table
                # check rows 
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ")
                query_sql = " select count(*) from {}".format(drop_table)
                checkData(0,0,100,query_sql)
                client_0.execute(drop_sql)
                self.logger.info(drop_sql)

            # insert data and it will auto create droped tables

            insert_extra_rows(dbname ,stablename ,100)

            # check rows for insert again 

            exists_tables = list(set(tables_list) - set(drop_tables))

            for table in exists_tables:
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ")
                query_sql = " select count(*) from {} ".format(table)
                checkData(0,0,params["_row_nums"] +100 ,query_sql)
            for table in drop_tables:
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ")
                query_sql = " select count(*) from {} ".format(table)
                checkData(0,0,100,query_sql)


        def create_drop_dbs(db_nums):

            dbs = get_db_name(db_nums)
            
            drop_dbs = []
            # prepare dbs and insert data
            for index , db in enumerate(dbs):
                client_0.execute("drop database if exists {}".format(db))
                create_db_tables(db , "stable_drop_dbs")
                insert_per_rows(db , "stable_drop_dbs")
                if index % 3 ==0:
                    drop_dbs.append(db)
            
            print("create dbs done! ")

            # drop dbs 
            for drop_db in drop_dbs:
                drop_sql = "drop database {}".format(drop_db)   # drop 100 times
                create_sql = "create database {} replica {}".format(drop_db,params["_replica"])
                for _ in range(100):
                    client_0.execute(drop_sql)
                    self.logger.info(drop_sql)
                    client_0.execute("reset query cache")
                    self.logger.info("reset query cache")
                    client_0.execute(create_sql)
                    self.logger.info(create_sql)
                    alter_dbs(drop_db)
                    # create_db_tables(drop_db , "stable_drop_dbs")
                    # insert_per_rows(drop_db , "stable_drop_dbs")
                    client_0.execute("show databases")
                    self.logger.info("show databases")
                    client_0.execute("reset query cache")
                    self.logger.info("reset query cache")
                client_0.execute(drop_sql)
                self.logger.info(drop_sql)
            # check database not exists
            client_0.execute("reset query cache")
            result = client_0.query("show databases")
            databases = result.fetch_all()
            for db in databases:
                if db[0] in drop_dbs:
                    raise BaseException(f"db {db[0]} should has been droped ")
                    break
            # create database again and insert extra rows 
            for drop_db in drop_dbs:
                create_sql = "create database {} replica {}".format(drop_db,params["_replica"])
                client_0.execute(create_sql)
                self.logger.info(create_sql)
            
            # insert extra data for all  databases

            exists_dbs = list(set(dbs) - set(drop_dbs))
            for exist_db in exists_dbs:
                insert_extra_rows(exist_db ,"stable_drop_dbs" ,100 )
                # check rows 
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ") 
                query_sql = "select count(*) from {}.stable_drop_dbs".format(exist_db)
                checkData(0,0,params["tables_of_per_stable"]*params["_row_nums"] +100*params["tables_of_per_stable"] , query_sql)
            for drop_db in drop_dbs:
                create_db_tables(drop_db ,"stable_drop_dbs")
                insert_extra_rows(drop_db ,"stable_drop_dbs" ,100 )
                # check rows 
                client_0.execute("reset query cache ")
                self.logger.info("reset query cache ") 
                query_sql = "select count(*) from {}.stable_drop_dbs".format(drop_db)
                checkData(0,0,100*params["tables_of_per_stable"],query_sql)

        create_drop_dbs(db_nums)  # create drop database task
        dbs = get_db_name(db_nums)
        for db in dbs:
            client_0.execute("drop database  if exists {}".format(db))
            self.logger.info("drop database  if exists {}".format(db)) 
            client_0.execute("create database {} replica {}".format(db ,params["_replica"]))
            self.logger.info("create database {} replica {}".format(db ,params["_replica"]))
            create_drop_stables(db,stable_nums)
            client_0.execute("drop database  if exists {}".format(db))
            self.logger.info("drop database  if exists {}".format(db))
            time.sleep(time_sleep)
        dbs = get_db_name(db_nums)
        for db in dbs:
            client_0.execute("drop database  if exists {}".format(db))
            self.logger.info("drop database  if exists {}".format(db))
            client_0.execute("create database {} replica {}".format(db ,params["_replica"]))
            self.logger.info("create database {} replica {}".format(db ,params["_replica"])) 
            create_drop_tables(db ,"drop_table", table_nums)
            client_0.execute("drop database  if exists {}".format(db))
            self.logger.info("drop database  if exists {}".format(db)) 
            time.sleep(time_sleep)
        dbs = get_db_name(db_nums)
        stables = get_stable_name(stable_nums)
        for db in dbs:
            for stable in stables:
                alter_tags(db , stable, 1)  # alter tags will auto reset db and stables
                client_0.execute("drop database  if exists {}".format(db))
                self.logger.info("drop database  if exists {}".format(db))
                time.sleep(time_sleep)

    def wait_dnode_status(self, endpoint, status, query_endpoint: str = None):
        '''
        query dnode status periodically until timeout or status match
        @endpoint
        @status
        '''
        if not query_endpoint is None:
            client_0 = self.tdSql.get_connection(None, query_endpoint)
        else:
            client_0 = self.tdSql.get_connection(self._conf)
        sql = "show dnodes"
        '''
        [(1, 'dnode_1:6030', 1, 24, 'ready', 'any', datetime.datetime(2022, 3, 30, 14, 38, 50, 264000), ''),
         (2, 'dnode_2:6030', 2, 24, 'ready', 'any', datetime.datetime(2022, 3, 30, 14, 39, 11, 665000), ''),
         (3, 'dnode_3:6030', 1, 24, 'offline', 'any', datetime.datetime(2022, 3, 30, 14, 39, 23, 60000), 'status msg timeout')]
        '''
        status_match = False
        max_retry_time = 10
        retry_time = 0
        while status_match == False and retry_time < max_retry_time:
            i = 0
            time.sleep(2)
            result = client_0.query(sql)
            data = result.fetch_all()
            self.logger.info("%d show dnodes: %s", retry_time, str(data))
            while i < len(data):
                if data[i][1] == endpoint and data[i][4] == status:
                    status_match = True
                    break
                i = i + 1
            retry_time = retry_time + 1
        return status_match

    def show_vgroups(self, db_name):
        '''
        show vgroups
        '''
        client_0 = self.tdSql.get_connection(self._conf)
        sql = "use %s" % (db_name)
        client_0.execute(sql)
        sql = "show vgroups"
        '''
        [(6, 5, 'ready', 3, 4, 'master', 3, 'slave', 2, 'slave', 0)]
        '''
        result = client_0.query(sql)
        data = result.fetch_all()
        self.logger.info("show vgroups: %s", str(data))
        client_0.close()

    def stop_dnode(self, endpoint, query_endpoint: str = None):
        '''
        stop dnode
        '''
        self.logger.info("stop dnode %s", endpoint)
        self.envMgr.stopDnode(endpoint)
        ret = self.wait_dnode_status(endpoint, "offline", query_endpoint)
        if ret == False:
            self.logger.error("dnode status not match")
            self.error_msg = "dnode status not match"
            self._status = False

    def start_dnode(self, endpoint, query_endpoint: str = None):
        '''
        start dnode
        '''
        self.logger.info("start dnode %s", endpoint)
        self.envMgr.startDnode(endpoint)
        ret = self.wait_dnode_status(endpoint, "ready", query_endpoint)
        if ret == False:
            self.logger.error("dnode status not match")
            self.error_msg = "dnode status not match"
            self._status = False

    def repeatedly_restart_dnode(self, endpoint, interval, times, query_endpoint: str = None):
        '''
        thread to restart dnode
        '''
        i = 0
        while i < times:
            if type(interval) == int:
                sleep_time = interval
            else:
                sleep_time = random.randint(interval[0], interval[1])
            self.logger.debug("%d -> sleep time: %d", i, sleep_time)
            time.sleep(sleep_time)
            # stop dnode_3
            self.stop_dnode(endpoint, query_endpoint)
            time.sleep(sleep_time)
            # start dnode_3
            self.start_dnode(endpoint, query_endpoint)
            i = i + 1
        self.logger.debug("restart dnode exit")

    def get_spec_conn(self, endpoint):
        return self.tdSql.get_connection(None, endpoint)

    def get_slaves(self):
        return self.get_nodes("slave")

    def get_masters(self):
        return self.get_nodes("master")

    def get_nodes(self, role):
        client_0 = self.tdSql.get_connection(self._conf)
        sql = "show mnodes"
        '''
        taos> show mnodes;
           id   |           end_point            |     role     |        role_time        |       create_time       |
        =============================================================================================================
              1 | dnode_1:6030                   | master       | 2022-03-31 16:24:52.380 | 2022-03-31 16:24:52.380 |
        '''
        node_list = []
        mnodes_result = client_0.query(sql)
        mnodes_data = mnodes_result.fetch_all()
        i = 0
        while i < len(mnodes_data):
            if mnodes_data[i][2] == role:
                node_list.append(mnodes_data[i][1])
            i = i + 1
        client_0.close()
        return node_list

    def check_result(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        pass

    def cleanup(self):
        pass
