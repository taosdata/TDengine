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

import random
import string
import requests
import time
import socket
import json
import toml
from util.boundary import DataBoundary
import taos
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from util.constant import *
from dataclasses import dataclass,field
from typing import List
from datetime import datetime

@dataclass
class DataSet:
    ts_data     : List[int]     = field(default_factory=list)
    int_data    : List[int]     = field(default_factory=list)
    bint_data   : List[int]     = field(default_factory=list)
    sint_data   : List[int]     = field(default_factory=list)
    tint_data   : List[int]     = field(default_factory=list)
    uint_data   : List[int]     = field(default_factory=list)
    ubint_data  : List[int]     = field(default_factory=list)
    usint_data  : List[int]     = field(default_factory=list)
    utint_data  : List[int]     = field(default_factory=list)
    float_data  : List[float]   = field(default_factory=list)
    double_data : List[float]   = field(default_factory=list)
    bool_data   : List[int]     = field(default_factory=list)
    vchar_data  : List[str]     = field(default_factory=list)
    nchar_data  : List[str]     = field(default_factory=list)

    def get_order_set(self,
        rows,
        int_step    :int    = 1,
        bint_step   :int    = 1,
        sint_step   :int    = 1,
        tint_step   :int    = 1,
        uint_step   :int    = 1,
        ubint_step  :int    = 1,
        usint_step  :int    = 1,
        utint_step  :int    = 1,
        float_step  :float  = 1,
        double_step :float  = 1,
        bool_start  :int    = 1,
        vchar_prefix:str    = "vachar_",
        vchar_step  :int    = 1,
        nchar_prefix:str    = "nchar_测试_",
        nchar_step  :int    = 1,
        ts_step     :int    = 1
    ):
        for i in range(rows):
            self.int_data.append( int(i * int_step % INT_MAX ))
            self.bint_data.append( int(i * bint_step % BIGINT_MAX ))
            self.sint_data.append( int(i * sint_step % SMALLINT_MAX ))
            self.tint_data.append( int(i * tint_step % TINYINT_MAX ))
            self.uint_data.append( int(i * uint_step % INT_UN_MAX ))
            self.ubint_data.append( int(i * ubint_step % BIGINT_UN_MAX ))
            self.usint_data.append( int(i * usint_step % SMALLINT_UN_MAX ))
            self.utint_data.append( int(i * utint_step % TINYINT_UN_MAX ))
            self.float_data.append( float(i * float_step % FLOAT_MAX ))
            self.double_data.append( float(i * double_step % DOUBLE_MAX ))
            self.bool_data.append( bool((i + bool_start) % 2 ))
            self.vchar_data.append( f"{vchar_prefix}{i * vchar_step}" )
            self.nchar_data.append( f"{nchar_prefix}{i * nchar_step}")
            self.ts_data.append( int(datetime.timestamp(datetime.now()) * 1000 - i * ts_step))

    def get_disorder_set(self, rows, **kwargs):
        for k, v in kwargs.items():
            int_low = v if k == "int_low" else INT_MIN
            int_up = v if k == "int_up" else INT_MAX
            bint_low = v if k == "bint_low" else BIGINT_MIN
            bint_up = v if k == "bint_up" else BIGINT_MAX
            sint_low = v if k == "sint_low" else SMALLINT_MIN
            sint_up = v if k == "sint_up" else SMALLINT_MAX
            tint_low = v if k == "tint_low" else TINYINT_MIN
            tint_up = v if k == "tint_up" else TINYINT_MAX
        pass


class TDCom:
    def __init__(self):
        self.sml_type = None
        self.env_setting = None
        self.smlChildTableName_value = None
        self.defaultJSONStrType_value = None
        self.smlTagNullName_value = None
        self.default_varchar_length = 256
        self.default_nchar_length = 256
        self.default_varchar_datatype = "letters"
        self.default_nchar_datatype = "letters"
        self.default_tagname_prefix = "t"
        self.default_colname_prefix = "c"
        self.default_stbname_prefix = "stb"
        self.default_ctbname_prefix = "ctb"
        self.default_tbname_prefix = "tb"
        self.default_tag_index_start_num = 1
        self.default_column_index_start_num = 1
        self.default_stbname_index_start_num = 1
        self.default_ctbname_index_start_num = 1
        self.default_tbname_index_start_num = 1
        self.default_tagts_name = "ts"
        self.default_colts_name = "ts"
        self.dbname = "test"
        self.stb_name = "stb"
        self.ctb_name = "ctb"
        self.tb_name = "tb"
        self.need_tagts = False
        self.tag_type_str = ""
        self.column_type_str = ""
        self.columns_str = None
        self.ts_value = None
        self.tag_value_list = list()
        self.column_value_list = list()
        self.full_type_list = ["tinyint", "smallint", "int", "bigint", "tinyint unsigned", "smallint unsigned", "int unsigned", "bigint unsigned", "float", "double", "binary", "nchar", "bool"]
        self.white_list = ["statsd", "node_exporter", "collectd", "icinga2", "tcollector", "information_schema", "performance_schema"]
        self.Boundary = DataBoundary()
    # def init(self, conn, logSql):
    #     # tdSql.init(conn.cursor(), logSql)

    def preDefine(self):
        header = {'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='}
        sql_url = "http://127.0.0.1:6041/rest/sql"
        sqlt_url = "http://127.0.0.1:6041/rest/sqlt"
        sqlutc_url = "http://127.0.0.1:6041/rest/sqlutc"
        influx_url = "http://127.0.0.1:6041/influxdb/v1/write"
        telnet_url = "http://127.0.0.1:6041/opentsdb/v1/put/telnet"
        return header, sql_url, sqlt_url, sqlutc_url, influx_url, telnet_url

    def genTcpParam(self):
        MaxBytes = 1024*1024
        host ='127.0.0.1'
        port = 6046
        return MaxBytes, host, port

    def tcpClient(self, input):
        MaxBytes = tdCom.genTcpParam()[0]
        host = tdCom.genTcpParam()[1]
        port = tdCom.genTcpParam()[2]
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.send(input.encode())
        sock.close()

    def restApiPost(self, sql):
        requests.post(self.preDefine()[1], sql.encode("utf-8"), headers = self.preDefine()[0])

    def createDb(self, dbname="test", db_update_tag=0, api_type="taosc"):
        if api_type == "taosc":
            if db_update_tag == 0:
                tdSql.execute(f"drop database if exists {dbname}")
                tdSql.execute(f"create database if not exists {dbname} precision 'us'")
            else:
                tdSql.execute(f"drop database if exists {dbname}")
                tdSql.execute(f"create database if not exists {dbname} precision 'us' update 1")
        elif api_type == "restful":
            if db_update_tag == 0:
                self.restApiPost(f"drop database if exists {dbname}")
                self.restApiPost(f"create database if not exists {dbname} precision 'us'")
            else:
                self.restApiPost(f"drop database if exists {dbname}")
                self.restApiPost(f"create database if not exists {dbname} precision 'us' update 1")
        tdSql.execute(f'use {dbname}')

    def genUrl(self, url_type, dbname, precision):
        if url_type == "influxdb":
            if precision is None:
                url = self.preDefine()[4] + "?" + "db=" + dbname
            else:
                url = self.preDefine()[4] + "?" + "db=" + dbname + "&precision=" + precision
        elif url_type == "telnet":
            url = self.preDefine()[5] + "/" + dbname
        else:
            url = self.preDefine()[1]
        return url

    def schemalessApiPost(self, sql, url_type="influxdb", dbname="test", precision=None):
        if url_type == "influxdb":
            url = self.genUrl(url_type, dbname, precision)
        elif url_type == "telnet":
            url = self.genUrl(url_type, dbname, precision)
        res = requests.post(url, sql.encode("utf-8"), headers = self.preDefine()[0])
        return res

    def cleanTb(self, type="taosc", dbname="db"):
        '''
            type is taosc or restful
        '''
        query_sql = f"show {dbname}.stables"
        res_row_list = tdSql.query(query_sql, True)
        stb_list = map(lambda x: x[0], res_row_list)
        for stb in stb_list:
            if type == "taosc":
                tdSql.execute(f'drop table if exists {dbname}.`{stb}`')
                if not stb[0].isdigit():
                    tdSql.execute(f'drop table if exists {dbname}.{stb}')
            elif type == "restful":
                self.restApiPost(f"drop table if exists {dbname}.`{stb}`")
                if not stb[0].isdigit():
                    self.restApiPost(f"drop table if exists {dbname}.{stb}")

    def dateToTs(self, datetime_input):
        return int(time.mktime(time.strptime(datetime_input, "%Y-%m-%d %H:%M:%S.%f")))

    def genTs(self, precision="ms", ts="", protype="taosc", ns_tag=None):
        """
        protype = "taosc" or "restful"
        gen ts and datetime
        """
        if precision == "ns":
            if ts == "" or ts is None:
                ts = time.time_ns()
            else:
                ts = ts
            if ns_tag is None:
                dt = ts
            else:
                dt = datetime.fromtimestamp(ts // 1000000000)
                dt = dt.strftime('%Y-%m-%d %H:%M:%S') + '.' + str(int(ts % 1000000000)).zfill(9)
            if protype == "restful":
                dt = datetime.fromtimestamp(ts // 1000000000)
                dt = dt.strftime('%Y-%m-%d %H:%M:%S') + '.' + str(int(ts % 1000000000)).zfill(9)
        else:
            if ts == "" or ts is None:
                ts = time.time()
            else:
                ts = ts
            if precision == "ms" or precision is None:
                ts = int(round(ts * 1000))
                dt = datetime.fromtimestamp(ts // 1000)
                if protype == "taosc":
                    dt = dt.strftime('%Y-%m-%d %H:%M:%S') + '.' + str(int(ts % 1000)).zfill(3) + '000'
                elif protype == "restful":
                    dt = dt.strftime('%Y-%m-%d %H:%M:%S') + '.' + str(int(ts % 1000)).zfill(3)
                else:
                    pass
            elif precision == "us":
                ts = int(round(ts * 1000000))
                dt = datetime.fromtimestamp(ts // 1000000)
                dt = dt.strftime('%Y-%m-%d %H:%M:%S') + '.' + str(int(ts % 1000000)).zfill(6)
        return ts, dt

    def get_long_name(self, length=10, mode="letters"):
        """
        generate long name
        mode could be numbers/letters/letters_mixed/mixed
        """
        if mode == "numbers":
            population = string.digits
        elif mode == "letters":
            population = string.ascii_letters.lower()
        elif mode == "letters_mixed":
            population = string.ascii_letters.upper() + string.ascii_letters.lower()
        else:
            population = string.ascii_letters.lower() + string.digits
        return "".join(random.choices(population, k=length))

    def getLongName(self, len, mode = "mixed"):
        """
            generate long name
            mode could be numbers/letters/letters_mixed/mixed
        """
        if mode == "numbers":
            chars = ''.join(random.choice(string.digits) for i in range(len))
        elif mode == "letters":
            chars = ''.join(random.choice(string.ascii_letters.lower()) for i in range(len))
        elif mode == "letters_mixed":
            chars = ''.join(random.choice(string.ascii_letters.upper() + string.ascii_letters.lower()) for i in range(len))
        else:
            chars = ''.join(random.choice(string.ascii_letters.lower() + string.digits) for i in range(len))
        return chars

    def restartTaosd(self, index=1, db_name="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use {db_name}")

    def typeof(self, variate):
        v_type=None
        if type(variate) is int:
            v_type = "int"
        elif type(variate) is str:
            v_type = "str"
        elif type(variate) is float:
            v_type = "float"
        elif type(variate) is bool:
            v_type = "bool"
        elif type(variate) is list:
            v_type = "list"
        elif type(variate) is tuple:
            v_type = "tuple"
        elif type(variate) is dict:
            v_type = "dict"
        elif type(variate) is set:
            v_type = "set"
        return v_type

    def splitNumLetter(self, input_mix_str):
        nums, letters = "", ""
        for i in input_mix_str:
            if i.isdigit():
                nums += i
            elif i.isspace():
                pass
            else:
                letters += i
        return nums, letters

    def smlPass(self, func):
        smlChildTableName = "no"
        def wrapper(*args):
            # if tdSql.getVariable("smlChildTableName")[0].upper() == "ID":
            if smlChildTableName.upper() == "ID":
                return func(*args)
            else:
                pass
        return wrapper

    def close(self):
        self.cursor.close()

    ########################################################################################################################################
    # new common API
    ########################################################################################################################################
    def create_database(self,tsql, dbName='test',dropFlag=1,**kwargs):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))
        '''
        vgroups replica precision strict wal fsync comp cachelast single_stable buffer pagesize pages minrows maxrows duration keep retentions
        '''
        sqlString = f'create database if not exists {dbName} '

        dbParams = ""
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                if param == "precision":
                    dbParams += f'{param} "{value}" '
                else:
                    dbParams += f'{param} {value} '
            sqlString += f'{dbParams}'

        tdLog.debug("create db sql: %s"%sqlString)
        tsql.execute(sqlString)
        tdLog.debug("complete to create database %s"%(dbName))
        return

    # def create_stable(self,tsql, dbName,stbName,column_elm_list=None, tag_elm_list=None):
    #     colSchema = ''
    #     for i in range(columnDict['int']):
    #         colSchema += ', c%d int'%i
    #     tagSchema = ''
    #     for i in range(tagDict['int']):
    #         if i > 0:
    #             tagSchema += ','
    #         tagSchema += 't%d int'%i

    #     tsql.execute("create table if not exists %s.%s (ts timestamp %s) tags(%s)"%(dbName, stbName, colSchema, tagSchema))
    #     tdLog.debug("complete to create %s.%s" %(dbName, stbName))
    #     return

    # def create_ctables(self,tsql, dbName,stbName,ctbNum,tagDict):
    #     tsql.execute("use %s" %dbName)
    #     tagsValues = ''
    #     for i in range(tagDict['int']):
    #         if i > 0:
    #             tagsValues += ','
    #         tagsValues += '%d'%i

    #     pre_create = "create table"
    #     sql = pre_create
    #     #tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
    #     for i in range(ctbNum):
    #         sql += " %s_%d using %s tags(%s)"%(stbName,i,stbName,tagsValues)
    #         if (i > 0) and (i%100 == 0):
    #             tsql.execute(sql)
    #             sql = pre_create
    #     if sql != pre_create:
    #         tsql.execute(sql)

    #     tdLog.debug("complete to create %d child tables in %s.%s" %(ctbNum, dbName, stbName))
    #     return

    # def insert_data(self,tsql,dbName,stbName,ctbNum,rowsPerTbl,batchNum,startTs=0):
    #     tdLog.debug("start to insert data ............")
    #     tsql.execute("use %s" %dbName)
    #     pre_insert = "insert into "
    #     sql = pre_insert
    #     if startTs == 0:
    #         t = time.time()
    #         startTs = int(round(t * 1000))
    #     #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
    #     for i in range(ctbNum):
    #         sql += " %s_%d values "%(stbName,i)
    #         for j in range(rowsPerTbl):
    #             sql += "(%d, %d, %d)"%(startTs + j, j, j)
    #             if (j > 0) and ((j%batchNum == 0) or (j == rowsPerTbl - 1)):
    #                 tsql.execute(sql)
    #                 if j < rowsPerTbl - 1:
    #                     sql = "insert into %s_%d values " %(stbName,i)
    #                 else:
    #                     sql = "insert into "
    #     #end sql
    #     if sql != pre_insert:
    #         #print("insert sql:%s"%sql)
    #         tsql.execute(sql)
    #     tdLog.debug("insert data ............ [OK]")
    #     return

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def getClientCfgPath(self):
        buildPath = self.getBuildPath()

        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = buildPath + "/../sim/psim/cfg"
        tdLog.info("cfgPath: %s" % cfgPath)
        return cfgPath

    def newcon(self,host='localhost',port=6030,user='root',password='taosdata'):
        con=taos.connect(host=host, user=user, password=password, port=port)
        # print(con)
        return con

    def newcur(self,host='localhost',port=6030,user='root',password='taosdata'):
        cfgPath = self.getClientCfgPath()
        con=taos.connect(host=host, user=user, password=password, config=cfgPath, port=port)
        cur=con.cursor()
        # print(cur)
        return cur

    def newTdSql(self, host='localhost',port=6030,user='root',password='taosdata'):
        newTdSql = TDSql()
        cur = self.newcur(host=host,port=port,user=user,password=password)
        newTdSql.init(cur, False)
        return newTdSql

    ################################################################################################################
    # port from the common.py of new test frame
    ################################################################################################################
    def gen_default_tag_str(self):
        default_tag_str = ""
        for tag_type in self.full_type_list:
            if tag_type.lower() not in ["varchar", "binary", "nchar"]:
                default_tag_str += f" {self.default_tagname_prefix}{self.default_tag_index_start_num} {tag_type},"
            else:
                if tag_type.lower() in ["varchar", "binary"]:
                    default_tag_str += f" {self.default_tagname_prefix}{self.default_tag_index_start_num} {tag_type}({self.default_varchar_length}),"
                else:
                    default_tag_str += f" {self.default_tagname_prefix}{self.default_tag_index_start_num} {tag_type}({self.default_nchar_length}),"
            self.default_tag_index_start_num += 1
        if self.need_tagts:
            default_tag_str = self.default_tagts_name + " timestamp," + default_tag_str
        return default_tag_str[:-1].lstrip()

    def gen_default_column_str(self):
        self.default_column_index_start_num = 1
        default_column_str = ""
        for col_type in self.full_type_list:
            if col_type.lower() not in ["varchar", "binary", "nchar"]:
                default_column_str += f" {self.default_colname_prefix}{self.default_column_index_start_num} {col_type},"
            else:
                if col_type.lower() in ["varchar", "binary"]:
                    default_column_str += f" {self.default_colname_prefix}{self.default_column_index_start_num} {col_type}({self.default_varchar_length}),"
                else:
                    default_column_str += f" {self.default_colname_prefix}{self.default_column_index_start_num} {col_type}({self.default_nchar_length}),"
            self.default_column_index_start_num += 1
        default_column_str = self.default_colts_name + " timestamp," + default_column_str
        return default_column_str[:-1].lstrip()

    def gen_tag_type_str(self, tagname_prefix, tag_elm_list):
        tag_index_start_num = 1
        tag_type_str = ""
        if tag_elm_list is None:
            tag_type_str = self.gen_default_tag_str()
        else:
            for tag_elm in tag_elm_list:
                if "count" in tag_elm:
                    total_count = int(tag_elm["count"])
                else:
                    total_count = 1
                if total_count > 0:
                    for _ in range(total_count):
                        tag_type_str += f'{tagname_prefix}{tag_index_start_num} {tag_elm["type"]}, '
                        if tag_elm["type"] in ["varchar", "binary", "nchar"]:
                            tag_type_str = tag_type_str.rstrip()[:-1] + f'({tag_elm["len"]}), '
                        tag_index_start_num += 1
                else:
                    continue
            tag_type_str = tag_type_str.rstrip()[:-1]

        return tag_type_str

    def gen_column_type_str(self, colname_prefix, column_elm_list):
        column_index_start_num = 1
        column_type_str = ""
        if column_elm_list is None:
            column_type_str = self.gen_default_column_str()
        else:
            for column_elm in column_elm_list:
                if "count" in column_elm:
                    total_count = int(column_elm["count"])
                else:
                    total_count = 1
                if total_count > 0:
                    for _ in range(total_count):
                        column_type_str += f'{colname_prefix}{column_index_start_num} {column_elm["type"]}, '
                        if column_elm["type"] in ["varchar", "binary", "nchar"]:
                            column_type_str = column_type_str.rstrip()[:-1] + f'({column_elm["len"]}), '
                        column_index_start_num += 1
                else:
                    continue
            column_type_str = self.default_colts_name + " timestamp, " + column_type_str.rstrip()[:-1]
        return column_type_str

    def gen_random_type_value(self, type_name, binary_length, binary_type, nchar_length, nchar_type):
        if type_name.lower() == "tinyint":
            return random.randint(self.Boundary.TINYINT_BOUNDARY[0], self.Boundary.TINYINT_BOUNDARY[1])
        elif type_name.lower() == "smallint":
            return random.randint(self.Boundary.SMALLINT_BOUNDARY[0], self.Boundary.SMALLINT_BOUNDARY[1])
        elif type_name.lower() == "int":
            return random.randint(self.Boundary.INT_BOUNDARY[0], self.Boundary.INT_BOUNDARY[1])
        elif type_name.lower() == "bigint":
            return random.randint(self.Boundary.BIGINT_BOUNDARY[0], self.Boundary.BIGINT_BOUNDARY[1])
        elif type_name.lower() == "tinyint unsigned":
            return random.randint(self.Boundary.UTINYINT_BOUNDARY[0], self.Boundary.UTINYINT_BOUNDARY[1])
        elif type_name.lower() == "smallint unsigned":
            return random.randint(self.Boundary.USMALLINT_BOUNDARY[0], self.Boundary.USMALLINT_BOUNDARY[1])
        elif type_name.lower() == "int unsigned":
            return random.randint(self.Boundary.UINT_BOUNDARY[0], self.Boundary.UINT_BOUNDARY[1])
        elif type_name.lower() == "bigint unsigned":
            return random.randint(self.Boundary.UBIGINT_BOUNDARY[0], self.Boundary.UBIGINT_BOUNDARY[1])
        elif type_name.lower() == "float":
            return random.uniform(self.Boundary.FLOAT_BOUNDARY[0], self.Boundary.FLOAT_BOUNDARY[1])
        elif type_name.lower() == "double":
            return random.uniform(self.Boundary.FLOAT_BOUNDARY[0], self.Boundary.FLOAT_BOUNDARY[1])
        elif type_name.lower() == "binary":
            return f'{self.get_long_name(binary_length, binary_type)}'
        elif type_name.lower() == "varchar":
            return self.get_long_name(binary_length, binary_type)
        elif type_name.lower() == "nchar":
            return self.get_long_name(nchar_length, nchar_type)
        elif type_name.lower() == "bool":
            return random.choice(self.Boundary.BOOL_BOUNDARY)
        elif type_name.lower() == "timestamp":
            return self.genTs()[0]
        else:
            pass

    def gen_tag_value_list(self, tag_elm_list):
        tag_value_list = list()
        if tag_elm_list is None:
            tag_value_list = list(map(lambda i: self.gen_random_type_value(i, self.default_varchar_length, self.default_varchar_datatype, self.default_nchar_length, self.default_nchar_datatype), self.full_type_list))
        else:
            for tag_elm in tag_elm_list:
                if "count" in tag_elm:
                    total_count = int(tag_elm["count"])
                else:
                    total_count = 1
                if total_count > 0:
                    for _ in range(total_count):
                        if tag_elm["type"] in ["varchar", "binary", "nchar"]:
                            tag_value_list.append(self.gen_random_type_value(tag_elm["type"], tag_elm["len"], self.default_varchar_datatype, tag_elm["len"], self.default_nchar_datatype))
                        else:
                            tag_value_list.append(self.gen_random_type_value(tag_elm["type"], "", "", "", ""))
                else:
                    continue
        return tag_value_list

    def gen_column_value_list(self, column_elm_list, ts_value=None):
        if ts_value is None:
            ts_value = self.genTs()[0]

        column_value_list = list()
        column_value_list.append(ts_value)
        if column_elm_list is None:
            column_value_list = list(map(lambda i: self.gen_random_type_value(i, self.default_varchar_length, self.default_varchar_datatype, self.default_nchar_length, self.default_nchar_datatype), self.full_type_list))
        else:
            for column_elm in column_elm_list:
                if "count" in column_elm:
                    total_count = int(column_elm["count"])
                else:
                    total_count = 1
                if total_count > 0:
                    for _ in range(total_count):
                        if column_elm["type"] in ["varchar", "binary", "nchar"]:
                            column_value_list.append(self.gen_random_type_value(column_elm["type"], column_elm["len"], self.default_varchar_datatype, column_elm["len"], self.default_nchar_datatype))
                        else:
                            column_value_list.append(self.gen_random_type_value(column_elm["type"], "", "", "", ""))
                else:
                    continue
        # column_value_list = [self.ts_value] + self.column_value_list
        return column_value_list

    def create_stable(self, tsql, dbname=None, stbname="stb", column_elm_list=None, tag_elm_list=None,
                     count=1, default_stbname_prefix="stb", **kwargs):
        colname_prefix = 'c'
        tagname_prefix = 't'
        stbname_index_start_num = 1
        stb_params = ""
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                stb_params += f'{param} "{value}" '
        column_type_str = self.gen_column_type_str(colname_prefix, column_elm_list)
        tag_type_str = self.gen_tag_type_str(tagname_prefix, tag_elm_list)

        if int(count) <= 1:
            create_stable_sql = f'create table {dbname}.{stbname} ({column_type_str}) tags ({tag_type_str}) {stb_params};'
            tdLog.info("create stb sql: %s"%create_stable_sql)
            tsql.execute(create_stable_sql)
        else:
            for _ in range(count):
                create_stable_sql = f'create table {dbname}.{default_stbname_prefix}{stbname_index_start_num} ({column_type_str}) tags ({tag_type_str}) {stb_params};'
                stbname_index_start_num += 1
                tsql.execute(create_stable_sql)

    def create_ctable(self, tsql, dbname=None, stbname=None, tag_elm_list=None, count=1, default_ctbname_prefix="ctb", **kwargs):
        ctbname_index_start_num = 0
        ctb_params = ""
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                ctb_params += f'{param} "{value}" '
        tag_value_list = self.gen_tag_value_list(tag_elm_list)
        tag_value_str = ""
        # tag_value_str = ", ".join(str(v) for v in self.tag_value_list)
        for tag_value in tag_value_list:
            if isinstance(tag_value, str):
                tag_value_str += f'"{tag_value}", '
            else:
                tag_value_str += f'{tag_value}, '
        tag_value_str = tag_value_str.rstrip()[:-1]

        if int(count) <= 1:
            create_ctable_sql = f'create table {dbname}.{default_ctbname_prefix}{ctbname_index_start_num}  using {dbname}.{stbname} tags ({tag_value_str}) {ctb_params};'
            tsql.execute(create_ctable_sql)
        else:
            for _ in range(count):
                create_ctable_sql = f'create table {dbname}.{default_ctbname_prefix}{ctbname_index_start_num} using {dbname}.{stbname} tags ({tag_value_str}) {ctb_params};'
                ctbname_index_start_num += 1
                tdLog.info("create ctb sql: %s"%create_ctable_sql)
                tsql.execute(create_ctable_sql)

    def create_table(self, tsql, dbname=None, tbname="ntb", column_elm_list=None, count=1, **kwargs):
        tbname_index_start_num = 1
        tbname_prefix="ntb"

        tb_params = ""
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                tb_params += f'{param} "{value}" '
        column_type_str = self.gen_column_type_str(tbname_prefix, column_elm_list)

        if int(count) <= 1:
            create_table_sql = f'create table {dbname}.{tbname} ({column_type_str}) {tb_params};'
            tsql.execute(create_table_sql)
        else:
            for _ in range(count):
                create_table_sql = f'create table {dbname}.{tbname_prefix}{tbname_index_start_num} ({column_type_str}) {tb_params};'
                tbname_index_start_num += 1
                tsql.execute(create_table_sql)

    def insert_rows(self, tsql, dbname=None, tbname=None, column_ele_list=None, start_ts_value=None, count=1):
        if start_ts_value is None:
            start_ts_value = self.genTs()[0]

        column_value_list = self.gen_column_value_list(column_ele_list, start_ts_value)
        # column_value_str = ", ".join(str(v) for v in self.column_value_list)
        column_value_str = ""
        for column_value in column_value_list:
            if isinstance(column_value, str):
                column_value_str += f'"{column_value}", '
            else:
                column_value_str += f'{column_value}, '
        column_value_str = column_value_str.rstrip()[:-1]
        if int(count) <= 1:
            insert_sql = f'insert into {self.tb_name} values ({column_value_str});'
            tsql.execute(insert_sql)
        else:
            for num in range(count):
                column_value_list = self.gen_column_value_list(column_ele_list, f'{start_ts_value}+{num}s')
                # column_value_str = ", ".join(str(v) for v in column_value_list)
                column_value_str = ''
                idx = 0
                for column_value in column_value_list:
                    if isinstance(column_value, str) and idx != 0:
                        column_value_str += f'"{column_value}", '
                    else:
                        column_value_str += f'{column_value}, '
                        idx += 1
                column_value_str = column_value_str.rstrip()[:-1]
                insert_sql = f'insert into {dbname}.{tbname} values ({column_value_str});'
                tsql.execute(insert_sql)
    def getOneRow(self, location, containElm):
        res_list = list()
        if 0 <= location < tdSql.queryRows:
            for row in tdSql.queryResult:
                if row[location] == containElm:
                    res_list.append(row)
            return res_list
        else:
            tdLog.exit(f"getOneRow out of range: row_index={location} row_count={self.query_row}")

    def killProcessor(self, processorName):
        if (platform.system().lower() == 'windows'):
            os.system("TASKKILL /F /IM %s.exe"%processorName)
        else:
            os.system("unset LD_PRELOAD; pkill %s " % processorName)


def is_json(msg):
    if isinstance(msg, str):
        try:
            json.loads(msg)
            return True
        except:
            return False
    else:
        return False

def get_path(tool="taosd"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files or ("%s.exe"%tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        if (len(paths) == 0):
                return ""
        return paths[0]

def dict2toml(in_dict: dict, file:str):
    if not isinstance(in_dict, dict):
        return ""
    with open(file, 'w') as f:
        toml.dump(in_dict, f)

tdCom = TDCom()
