###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-


from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
import random
import time
import traceback
import taos
import string
from taos import schemaless

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self.setsql = TDSetSql()
        self.conn = conn
        self.schema = {}

    def random_string(self, count):
        letters = string.ascii_letters
        return ''.join(random.choice(letters) for i in range(count))

    def genCol(self, col_name, isTag):
        col_types = ["str","f64","f32","i8","u8","i16","u16","i32","u32","i64","u64"]
        if self.schema.get(col_name) == None:
            col_type = random.choice(col_types)
            self.schema[col_name] =  col_type
        else:
            col_type = self.schema[col_name]

        is_num = True
        val = ""
        if col_type == "str":
            val =  self.random_string(random.randint(1, 10))
            is_num = False
        elif col_type == "f64":
            val = random.randrange(-100000000000000, 1000000000000)/3*2.25678
        elif col_type == "f32":
            val = random.randrange(-100000000, 1000000000)/3*1.2345
        elif col_type == "i8":
            val = random.randint(-128, 127)
        elif col_type == "u8":
            val = random.randint(0, 256)
        elif col_type == "i16":
            val = random.randint(-32768, 32767)
        elif col_type == "u16":
            val = random.randint(0, 256*256)
        elif col_type == "i32":
            val = random.randint(-256*256*256*128, 256*256*256*128)
        elif col_type == "u32":
            val = random.randint(0, 256*256*256*256)
        elif col_type == "i64":
            val = random.randint(-256*256*256*256*256*256*256*128, 256*256*256*256*256*256*256*128)
        elif col_type == "u64":
            val = random.randint(0, 256*256*256*256*256*256*256*256)
        else:
            val = 100

        if isTag:
            col_val = val
        elif is_num:
            col_val = f'{val}{col_type}'
        else:
            col_val = '"' + val + '"'

        return  f'{col_name}={col_val}'   
 

    # cols
    def genCols(self, pre, max, index, isTag):
        col_cnt = random.randint(1, max)
        cols = []
        for i in range(col_cnt):
            col_name = f'{pre}_{index}_{i}'
            cols.append(self.genCol(col_name, isTag))

        return ",".join(cols)

   
    # execute sql
    def insert(self,sql,i):
        print("schema less insert")
        try:
           self.conn.schemaless_insert([sql], schemaless.SmlProtocol.LINE_PROTOCOL, schemaless.SmlPrecision.MILLI_SECONDS)
           tdLog.info(f" exec ok i={i} {sql}")
        except:
           tdLog.info(f" exe failed. i={i} {sql}")
           traceback.print_exc()
    
    def genTags(self, i):
        tags = f"t1={i},t2=abc,t3=work"
        return tags
    
    # change table schema
    def schemaless_insert(self, change_cnt):
        # init
        ts = 1683194263000
        for i in range(change_cnt):
            index = int(i/10000) % 600
            cols = self.genCols("c", 5, index, False)
            tags = self.genTags(index)
            sql = f'{self.stable},{tags} {cols} {ts + i}'
            self.insert(sql, i)

    # run

    def run(self):
        # seed
        #random.seed(int(time.time()))
        self.dbname = "eco_system"
        self.stable = "sml_stb"

        # switch db
        tdSql.execute(f"use {self.dbname};")
        tdSql.execute(f"drop table if exists {self.stable};")
        
        
       
        # change meters
        try:
          self.schemaless_insert(1000000)
        except:
          traceback.print_exc()

        print(self.schema)

        
    # stop
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
