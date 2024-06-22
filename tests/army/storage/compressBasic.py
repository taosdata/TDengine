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

import sys
import time
import random

import taos
import frame
import frame.etool


from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *


class TDTestCase(TBase):
    updatecfgDict = {
        "compressMsgSize" : "100",
    }
    # compress
    compresses = ["lz4","tsz","zlib","zstd","disabled","xz"]

    # level
    levels = ["high","medium","low"]

    # default compress
    defCompress = "lz4"
    # default level
    defLevel    = "medium"

    # datatype 17
    dtypes = [ "tinyint","tinyint unsigned","smallint","smallint unsigned","int","int unsigned",
            "bigint","bigint unsigned","timestamp","bool","float","double","binary(16)","nchar(16)",
            "varchar(16)","varbinary(16)"]
    
    # encode
    encodes = [
        [["tinyint","tinyint unsigned","smallint","smallint unsigned","int","int unsigned","bigint","bigint unsigned"], ["simple8B"]],
        [["timestamp","bigint","bigint unsigned"],  ["Delta-i"]],
        [["bool"],                                  ["Bit-packing"]],
        [["float","double"],                        ["Delta-d"]]
    ]

    
    def combineValid(self, datatype, encode, compress):
        if datatype != "float" and datatype != "double":
            if compress == "tsz":
                return False
        return True

    def genAllSqls(self, stbName, max):

        c = 0 # column number
        t = 0 # table number

        sqls = []
        sql  = ""

        # loop append sqls
        for lines in self.encodes:
            for datatype in lines[0]:
                for encode in lines[1]:
                    for compress in self.compresses:
                        for level in self.levels:
                            if sql == "":
                                # first
                                sql = f"create table {self.db}.st{t} (ts timestamp"
                            else:
                                if self.combineValid(datatype, encode, compress):
                                    sql += f", c{c} {datatype} ENCODE '{encode}' COMPRESS '{compress}' LEVEL '{level}'"
                                    c += 1
                            
                            if c >= max:
                                # append sqls
                                sql += f") tags(groupid int) "
                                sqls.append(sql)
                                # reset
                                sql = ""
                                c = 0
                                t += 1

        # break loop
        if c > 0:
            # append sqls
            sql += f") tags(groupid int) "
            sqls.append(sql)
        
        return sqls

    # check error create
    def errorCreate(self):
        sqls = [
            f"create table terr(ts timestamp, c0 int ENCODE 'simple8B' COMPRESS 'tsz' LEVEL 'high') ",
            f"create table terr(ts timestamp, bi bigint encode 'bit-packing') tags (area int);"
            f"create table terr(ts timestamp, ic int encode 'delta-d') tags (area int);"
        ]
        tdSql.errors(sqls)

        for dtype in self.dtypes:
            # encode
            sql = f"create table terr(ts timestamp, c0 {dtype} ENCODE 'abc') "
            tdSql.error(sql)
            # compress
            sql = f"create table terr(ts timestamp, c0 {dtype} COMPRESS 'def') "
            tdSql.error(sql)
            # level
            sql = f"create table terr(ts timestamp, c0 {dtype} LEVEL 'hig') "
            tdSql.error(sql)

            # tsz check
            if dtype != "float" and dtype != "double":
                sql = f"create table terr(ts timestamp, c0 {dtype} COMPRESS 'tsz') "
                tdSql.error(sql)
    
    # default value correct
    def defaultCorrect(self):
        # get default encode compress level
        sql = f"describe {self.db}.{self.stb}"
        tdSql.query(sql)

        # see AutoGen.types 
        defEncodes = [ "delta-i","delta-i","simple8b","simple8b","simple8b","simple8b","simple8b","simple8b",
                       "simple8b","simple8b","delta-d","delta-d","bit-packing",
                       "disabled","disabled","disabled","disabled"]        

        count = tdSql.getRows()
        for i in range(count):
            node = tdSql.getData(i, 3)
            if node == "TAG":
                break
            # check
            tdSql.checkData(i, 4, defEncodes[i])
            tdSql.checkData(i, 5, self.defCompress)
            tdSql.checkData(i, 6, self.defLevel)

        # geometry encode is disabled
        sql = f"create table {self.db}.ta(ts timestamp, pos geometry(64)) "
        tdSql.execute(sql)
        sql = f"describe {self.db}.ta"
        tdSql.query(sql)
        tdSql.checkData(1, 4, "disabled")

        tdLog.info("check default encode compress and level successfully.")

    def checkDataDesc(self, tbname, row, col, value):
        sql = f"describe {tbname}"
        tdSql.query(sql)
        tdSql.checkData(row, col, value)


    def writeData(self, count):
        self.autoGen.insert_data(count, True)
    
    # alter encode compress level
    def checkAlter(self):
        tbname = f"{self.db}.{self.stb}"
        # alter encode 4
        comp = "delta-i"
        sql = f"alter table {tbname} modify column c7 ENCODE '{comp}';"
        tdSql.execute(sql, show=True)
        self.checkDataDesc(tbname, 8, 4, comp)
        self.writeData(1000)
        sql = f"alter table {tbname} modify column c8 ENCODE '{comp}';"
        tdSql.execute(sql, show=True)
        self.checkDataDesc(tbname, 9, 4, comp)
        self.writeData(1000)

        # alter compress 5
        comps = self.compresses[2:]
        comps.append(self.compresses[0]) # add lz4
        for comp in comps:
            for i in range(self.colCnt - 1):
                col = f"c{i}"
                sql = f"alter table {tbname} modify column {col} COMPRESS '{comp}';"
                tdSql.execute(sql, show=True)
                self.checkDataDesc(tbname, i + 1, 5, comp)
                self.writeData(1000)

        # alter float(c9) double(c10) to tsz
        comp = "tsz"
        sql = f"alter table {tbname} modify column c9 COMPRESS '{comp}';"
        tdSql.execute(sql, show=True)
        self.checkDataDesc(tbname, 10, 5, comp)
        self.writeData(10000)
        sql = f"alter table {tbname} modify column c10 COMPRESS '{comp}';"
        tdSql.execute(sql, show=True)
        self.checkDataDesc(tbname, 11, 5, comp)
        self.writeData(10000)

        # alter level 6
        for level in self.levels:
            for i in range(self.colCnt - 1):
                col = f"c{i}"
                sql = f"alter table {tbname} modify column {col} LEVEL '{level}';"
                tdSql.execute(sql, show=True)
                self.checkDataDesc(tbname, i + 1, 6, level)
                self.writeData(1000)

        # modify two combine


        i = 9
        encode   = "delta-d"
        compress = "zlib"
        sql = f"alter table {tbname} modify column c{i} ENCODE '{encode}' COMPRESS '{compress}';"
        tdSql.execute(sql, show=True)
        self.checkDataDesc(tbname, i + 1, 4, encode)
        self.checkDataDesc(tbname, i + 1, 5, compress)
        
        i = 10
        encode = "delta-d"
        level  = "high"
        sql = f"alter table {tbname} modify column c{i} ENCODE '{encode}' LEVEL '{level}';"
        tdSql.execute(sql, show=True)
        self.checkDataDesc(tbname, i + 1, 4, encode)
        self.checkDataDesc(tbname, i + 1, 6, level)

        i = 2
        compress = "zlib"
        level    = "high"
        sql = f"alter table {tbname} modify column c{i} COMPRESS '{compress}' LEVEL '{level}';"
        tdSql.execute(sql, show=True)
        self.checkDataDesc(tbname, i + 1, 5, compress)
        self.checkDataDesc(tbname, i + 1, 6, level)

        # modify three combine
        i = 7
        encode   = "simple8b"
        compress = "zstd"
        level    = "medium"
        sql = f"alter table {tbname} modify column c{i} ENCODE '{encode}' COMPRESS '{compress}' LEVEL '{level}';"
        tdSql.execute(sql, show=True)
        self.checkDataDesc(tbname, i + 1, 4, encode)
        self.checkDataDesc(tbname, i + 1, 5, compress)
        self.checkDataDesc(tbname, i + 1, 6, level)

        # alter error 
        sqls = [
            "alter table nodb.nostb modify column ts LEVEL 'high';",
            "alter table db.stb modify column ts encode 'simple8b';",
            "alter table db.stb modify column c1 compress 'errorcompress';",
            "alter table db.stb modify column c2 level 'errlevel';",
            "alter table db.errstb modify column c3 compress 'xz';"
        ]
        tdSql.errors(sqls)

    # add column
    def checkAddColumn(self):
        c = 0
        tbname = f"{self.db}.tbadd"
        sql = f"create table {tbname}(ts timestamp, c0 int) tags(area int);"
        tdSql.execute(sql)
    
        # loop append sqls
        for lines in self.encodes:
            for datatype in lines[0]:
                for encode in lines[1]:
                    for compress in self.compresses:
                        for level in self.levels:
                            if self.combineValid(datatype, encode, compress):
                                sql = f"alter table {tbname} add column col{c} {datatype} ENCODE '{encode}' COMPRESS '{compress}' LEVEL '{level}';"
                                tdSql.execute(sql, 3, True)
                                c += 1

        # alter error 
        sqls = [
            f"alter table {tbname} add column a1 int ENCODE 'simple8bAA';",
            f"alter table {tbname} add column a2 int COMPRESS 'AABB';",
            f"alter table {tbname} add column a3 bigint LEVEL 'high1';",
            f"alter table {tbname} add column a4 BINARY(12) ENCODE 'simple8b' LEVEL 'high2';",
            f"alter table {tbname} add column a5 VARCHAR(16) ENCODE 'simple8b' COMPRESS 'gzip' LEVEL 'high3';"
        ]
        tdSql.errors(sqls)

    def validCreate(self):
        sqls = self.genAllSqls(self.stb, 50)
        tdSql.executes(sqls, show=True)
    
    # sql syntax
    def checkSqlSyntax(self):

       # create tables positive
        self.validCreate()

        # create table negtive
        self.errorCreate()

        # check default value corrent
        self.defaultCorrect()

        # check alter and write
        self.checkAlter()

        # check add column
        self.checkAddColumn()

    def checkCorrect(self):
        # check data correct
        tbname = f"{self.db}.{self.stb}"
        # count
        sql = f"select count(*) from {tbname}"
        count = tdSql.getFirstValue(sql)
        step = 100000
        offset = 0

        while offset < count:
            sql = f"select * from {tbname} limit {step} offset {offset}"
            tdSql.query(sql)
            self.autoGen.dataCorrect(tdSql.res, tdSql.getRows(), step)
            offset += step
            tdLog.info(f"check data correct rows={offset}")

        tdLog.info(F"check {tbname} rows {count} data correct successfully.")

 
    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # create db and stable
        self.autoGen = AutoGen(step = 10, genDataMode = "fillts")
        self.autoGen.create_db(self.db, 2, 3)
        tdSql.execute(f"use {self.db}")
        self.colCnt = 17
        self.autoGen.create_stable(self.stb, 5, self.colCnt, 32, 32)
        self.childCnt = 4
        self.autoGen.create_child(self.stb, "d", self.childCnt)
        self.autoGen.insert_data(1000)

        # sql syntax
        self.checkSqlSyntax()

        # operateor
        self.writeData(1000)
        self.flushDb()
        self.writeData(1000)

        # check corrent
        self.checkCorrect()

        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
