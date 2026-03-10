# -*- coding: utf-8 -*-

import sys
from .log import *
from .sql import *
import threading
import random
import string
import time
import math


#
# Auto Gen class
#

# genDataMode  fillone fillts others
class AutoGen:
    def __init__(self, startTs = 1600000000000, step = 1000, batch = 500, genDataMode="others"):
        self.startTs    = startTs
        self.ts         = startTs
        self.step       = step
        self.batch_size = batch
        self.genDataMode  = genDataMode
        seed = time.time() % 10000
        random.seed(seed)

    # set start ts
    def set_start_ts(self, ts):
        self.startTs = ts
        self.ts = ts

    # set batch size
    def set_batch_size(self, batch_size):
        self.batch_size = batch_size

    #  _columns_sql
    def gen_columns_sql(self, pre, cnt, binary_len, nchar_len, type_set_name='default'):
        type_sets = {
            'default': [ 
                'timestamp',  # 0
                'tinyint',    
                'tinyint unsigned', # 3
                'smallint',
                'smallint unsigned',
                'int', # 5
                'int unsigned',
                'bigint',    # 7
                'bigint unsigned',
                'float',     # 9
                'double',    # 10
                'bool',
                f'binary({binary_len})',  # 12
                f'varbinary({binary_len})',
                f'nchar({nchar_len})',
                f'varchar({nchar_len})'
            ], 
            'varchar_preferred': [ 
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
        }
        types = type_sets.get(type_set_name, type_sets['default'])

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
    # migrate from system-test
    def gen_data_old(self, i, marr):
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
    
    # gen tags data
    def gen_data(self, i, marr):
        datas = ""   
        for c in marr:
            data = ""
            if c == 0 : # timestamp
                data = "%d" % (i)
            elif c <= 2 : # tinyint
                data = "%d"%(i%0x7F)
            elif c <= 4 : # smallint
                data = "%d"%(i%0x7FFF)
            elif c <= 6 : # int32
                data = f"{i%0x7FFFFFFF}"
            elif c <= 8 : # bigint
                data = f"{i}"
            elif c <= 10 : # float
                data = "%f"%(i+i/1000)
            elif c <= 11 : # bool
                data = "%d"%(i%2)
            elif c <= 13 : # binary
                data = '"' + self.random_string(self.bin_len) + '"'
            elif c == 16 : # geometry
                data = f'"point({i} {i})"'
            else : # nchar varchar 
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
    
    # gen tags data
    def fillts_data(self, ts, marr):
        datas = ""   
        for c in marr:
            data = ""
            if c == 0 : # timestamp
                data = "%d" % (ts)
            elif c <= 2 : # tinyint
                data = "%d"%(ts%100)
            elif c <= 4 : # smallint
                data = "%d"%(ts%10000)
            elif c <= 6 : # int32
                data = f"{ts%1000000000}"
            elif c <= 8 : # bigint
                data = f"{ts}"
            elif c == 9 : # float
                data = "%f"%(ts%10000000 )
            elif c == 10 : # double
                data = "%f"%(ts%1000000000000000)
            elif c <= 11 : # bool
                data = "%d"%(ts%2)
            elif c == 16 : # geometry
                data = f'"point({ts%100} {ts%1000})"'
            else : # string binary nchar varchar
                data = f"'{ts}'"
            if datas != "":
                datas += ","
            datas += data
        
        return datas
    
    # check row correct
    def rowCorrect(self, ts, value, c): 
        if c == 0 : # timestamp
            ival = int(value.timestamp() * 1000)
            return (ival == ts, ts)
        elif c <= 2 : # tinyint
            return  (value == ts%100, ts%100)
        elif c <= 4 : # smallint
            return (value == ts%10000, ts%10000)
        elif c <= 6 : # int
            return (value == ts%1000000000, ts%1000000000)
        elif c <= 8 : # bigint
            return (value == ts, ts)
        elif c == 9 : # float
            fval = (ts%10000000)
            return (abs(value - fval) < 0.01, fval)
        elif c == 10 : # double
            fval = (ts%1000000000000000)
            return (abs(value - fval) < 0.0001, fval)
        elif c <= 11 : # bool
            return (value == ts%2, ts%2)
        elif c == 16 : # geometry
            return (value == f'point({ts%100} {ts%1000})', f'point({ts%100} {ts%1000})')
        else : # string binary nchar varchar
            return (int(value) == ts, f"{ts}")


    # check row data correct
    def dataCorrect(self, res, rowCnt, showStep = 1000):
        colCnt = len(self.mcols)
        for i in range(rowCnt):
            for j in range(colCnt):
                ts = int(res[i][0].timestamp() * 1000)
                ret, value = self.rowCorrect(ts, res[i][j+1], self.mcols[j]) # j + 1 , remove first ts column
                if ret == False:
                    tdLog.exit(f"rowCorrect check failed. i={i} j={j} data={res[i][j+1]} expect={value}")
            if i > 0 and i % showStep == 0:
                tdLog.info(f"check data correct rows {i}")


    # generate specail wide random string
    def random_string(self, count):
        letters = string.ascii_letters
        return ''.join(random.choice(letters) for i in range(count))

    # create db
    def create_db(self, dbname, vgroups = 2, replica = 1, others=""):
        self.dbname  = dbname
        tdSql.execute(f'create database {dbname} vgroups {vgroups} replica {replica} {others}')
        tdSql.execute(f"use {dbname}")
        
    # create table or stable
    def create_stable(self, stbname, tag_cnt, column_cnt, binary_len, nchar_len, type_set='default'):
        self.bin_len = binary_len
        self.nch_len = nchar_len
        self.stbname = stbname
        self.mtags, tags = self.gen_columns_sql("t", tag_cnt, binary_len, nchar_len, type_set)
        self.mcols, cols = self.gen_columns_sql("c", column_cnt - 1, binary_len, nchar_len, type_set)

        sql = f"create table {self.dbname}.{stbname} (ts timestamp, {cols}) tags({tags})"
        tdSql.execute(sql)

    # create child table 
    def create_child(self, stbname, prename, cnt, tag_data_mode='new'):
        self.child_cnt = cnt
        self.child_name = prename
        for i in range(cnt):
            if tag_data_mode == 'new':
                tags_data = self.gen_data(i, self.mtags)
            else:
                tags_data = self.gen_data_old(i, self.mtags)
            sql = f"create table {self.dbname}.{prename}{i} using {self.dbname}.{stbname} tags({tags_data})"
            tdSql.execute(sql)

        tdLog.info(f"create child tables {cnt} ok")

    def insert_data_child(self, child_name, cnt, batch_size, step):        
        values = ""
        ts = self.ts

        # loop do
        for i in range(cnt):
            # gen other col data
            if self.genDataMode == "fillone":
                value = self.fillone_data(i, self.mcols)
            elif self.genDataMode == "fillts":
                value = self.fillts_data(ts, self.mcols)
            else:    
                value = self.gen_data(ts, self.mcols)

            # check to execute sql
            values += f"({ts},{value}) "
            if batch_size == 1 or (i > 0 and i % batch_size == 0) :
                sql = f"insert into {self.dbname}.{child_name} values {values}"
                tdSql.execute(sql)
                values = ""

            # move next
            ts += step    

        # end batch
        if values != "":
            sql = f"insert into {self.dbname}.{child_name} values {values}"
            tdSql.execute(sql)
            values = ""

        tdLog.debug(f" insert child data {child_name} finished, insert rows={cnt}")
        return ts

    def insert_data(self, cnt, bContinue=False):
        if not bContinue:
            self.ts = self.startTs

        currTs = self.startTs
        for i in range(self.child_cnt):
            name = f"{self.child_name}{i}"
            currTs = self.insert_data_child(name, cnt, self.batch_size, self.step)

        self.ts = currTs
        tdLog.debug(f" insert data ok, child table={self.child_cnt} insert rows={cnt}")

    # insert same timestamp to all childs
    def insert_samets(self, cnt):
        for i in range(self.child_cnt):
            name = f"{self.child_name}{i}"
            self.insert_data_child(name, cnt, self.batch_size, 0)

        tdLog.info(f" insert same timestamp ok, child table={self.child_cnt} insert rows={cnt}")         
