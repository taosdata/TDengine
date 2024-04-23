# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *
import threading
import random
import string
import time


#
# Auto Gen class
#
class AutoGen:
    def __init__(self, fillOne=False):
        self.ts = 1600000000000
        self.batch_size = 100
        seed = time.time() % 10000
        random.seed(seed)
        self.fillOne = fillOne

    # set start ts
    def set_start_ts(self, ts):
        self.ts = ts

    # set batch size
    def set_batch_size(self, batch_size):
        self.batch_size = batch_size

    #  _columns_sql
    def gen_columns_sql(self, pre, cnt, binary_len, nchar_len):
        types = [ 
            'timestamp',
            'tinyint',
            'smallint',
            'tinyint unsigned',
            'smallint unsigned',
            'int',
            'bigint',
            'int unsigned',
            'bigint unsigned',
            'float',
            'double',
            'bool',
            f'varchar({binary_len})',
            f'nchar({nchar_len})'
        ]

        sqls = ""
        metas = []
        for i in range(cnt):
            colname = f"{pre}{i}"
            sel = i % len(types)
            coltype = types[sel]
            sql = f"{colname} {coltype}"
            if sqls != "":
                sqls += ","
            sqls += sql
            metas.append(sel)
        
        return metas, sqls;    

    # gen tags data
    def gen_data(self, i, marr):
        datas = ""   
        for c in marr:
            data = ""
            if c == 0 : # timestamp
                data = "%d" % (self.ts + i)
            elif c <= 4 : # small
                data = "%d"%(i%128)
            elif c <= 8 : # int
                data = f"{i}"
            elif c <= 10 : # float
                data = "%f"%(i+i/1000)
            elif c <= 11 : # bool
                data = "%d"%(i%2)
            elif c == 12 : # binary
                data = '"' + self.random_string(self.bin_len) + '"'
            elif c == 13 : # binary
                data = '"' + self.random_string(self.nch_len) + '"'

            if datas != "":
                datas += ","
            datas += data
        
        return datas

    # fill one data
    def fillone_data(self, i, marr):
        datas = ""
        for c in marr:
            if datas != "":
                datas += ","

            if c == 0:
                datas += "%d" % (self.ts + i)
            elif c == 12 or c == 13: # binary
                datas += '"1"'
            else:
                datas += '1'
        
        return datas


    # generate specail wide random string
    def random_string(self, count):
        letters = string.ascii_letters
        return ''.join(random.choice(letters) for i in range(count))

    # create db
    def create_db(self, dbname, vgroups = 2, replica = 1):
        self.dbname  = dbname
        tdSql.execute(f'create database {dbname} vgroups {vgroups} replica {replica}')
        tdSql.execute(f"use {dbname}")
        
    # create table or stable
    def create_stable(self, stbname, tag_cnt, column_cnt, binary_len, nchar_len):
        self.bin_len = binary_len
        self.nch_len = nchar_len
        self.stbname = stbname
        self.mtags, tags = self.gen_columns_sql("t", tag_cnt, binary_len, nchar_len)
        self.mcols, cols = self.gen_columns_sql("c", column_cnt - 1, binary_len, nchar_len)

        sql = f"create table {self.dbname}.{stbname} (ts timestamp, {cols}) tags({tags})"
        tdSql.execute(sql)

    # create child table 
    def create_child(self, stbname, prename, cnt):
        self.child_cnt = cnt
        self.child_name = prename
        for i in range(cnt):
            tags_data = self.gen_data(i, self.mtags)
            sql = f"create table {self.dbname}.{prename}{i} using {self.dbname}.{stbname} tags({tags_data})"
            tdSql.execute(sql)

        tdLog.info(f"create child tables {cnt} ok")

    def insert_data_child(self, child_name, cnt, batch_size, step):        
        values = ""
        print("insert child data")
        ts = self.ts

        # loop do
        for i in range(cnt):
            if self.fillOne :
                value = self.fillone_data(i, self.mcols)
            else:
                value = self.gen_data(i, self.mcols)
            ts += step
            values += f"({ts},{value}) "
            if batch_size == 1 or (i > 0 and i % batch_size == 0) :
                sql = f"insert into {self.dbname}.{child_name} values {values}"
                tdSql.execute(sql)
                values = ""

        # end batch
        if values != "":
            sql = f"insert into {self.dbname}.{child_name} values {values}"
            tdSql.execute(sql)
            tdLog.info(f" insert data i={i}")
            values = ""

        tdLog.info(f" insert child data {child_name} finished, insert rows={cnt}")    

    # insert data 
    def insert_data(self, cnt):
        for i in range(self.child_cnt):
            name = f"{self.child_name}{i}"
            self.insert_data_child(name, cnt, self.batch_size, 1)

        tdLog.info(f" insert data ok, child table={self.child_cnt} insert rows={cnt}")

    # insert same timestamp to all childs
    def insert_samets(self, cnt):
        for i in range(self.child_cnt):
            name = f"{self.child_name}{i}"
            self.insert_data_child(name, cnt, self.batch_size, 0)

        tdLog.info(f" insert same timestamp ok, child table={self.child_cnt} insert rows={cnt}")         


