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
import re
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
        self.default_varchar_length = 6
        self.default_nchar_length = 6
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
        self.tbname = str()
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
        self.white_list = ["statsd", "node_exporter", "collectd", "icinga2", "tcollector", "information_schema", "performance_schema"]
        self.case_name = str()
        self.des_table_suffix = "_output"
        self.stream_suffix = "_stream"
        self.range_count = 5
        self.default_interval = 5
        self.stream_timeout = 60
        self.create_stream_sleep = 0.5
        self.record_history_ts = str()
        self.precision = "ms"
        self.date_time = self.genTs(precision=self.precision)[0]
        self.subtable = True
        self.partition_tbname_alias = "ptn_alias" if self.subtable else ""
        self.partition_col_alias = "pcol_alias" if self.subtable else ""
        self.partition_tag_alias = "ptag_alias" if self.subtable else ""
        self.partition_expression_alias = "pexp_alias" if self.subtable else ""
        self.des_table_suffix = "_output"
        self.stream_suffix = "_stream"
        self.subtable_prefix = "prefix_" if self.subtable else ""
        self.subtable_suffix = "_suffix" if self.subtable else ""
        self.downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "apercentile(c6, 50)", "avg(c7)", "count(c8)", "spread(c1)",
            "stddev(c2)", "hyperloglog(c11)", "timediff(1, 0, 1h)", "timezone()", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")', "min(t1)", "max(t2)", "sum(t3)",
            "first(t4)", "last(t5)", "apercentile(t6, 50)", "avg(t7)", "count(t8)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        self.stb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.downsampling_function_list)))
        self.tb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.downsampling_function_list[0:15])))
        self.stb_source_select_str = ','.join(self.downsampling_function_list)
        self.tb_source_select_str = ','.join(self.downsampling_function_list[0:15])
        self.fill_function_list = ["min(c1)", "max(c2)", "sum(c3)", "apercentile(c6, 50)", "avg(c7)", "count(c8)", "spread(c1)",
            "stddev(c2)", "hyperloglog(c11)", "timediff(1, 0, 1h)", "timezone()", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")', "min(t1)", "max(t2)", "sum(t3)",
            "first(t4)", "last(t5)", "apercentile(t6, 50)", "avg(t7)", "count(t8)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        self.fill_stb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.fill_function_list)))
        self.fill_stb_source_select_str = ','.join(self.fill_function_list)
        self.fill_tb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.fill_function_list[0:13])))
        self.fill_tb_source_select_str = ','.join(self.fill_function_list[0:13])
        self.ext_tb_source_select_str = ','.join(self.downsampling_function_list[0:13])
        self.stream_case_when_tbname = "tbname"

        self.update = True
        self.disorder = True
        if self.disorder:
            self.update = False
        self.partition_by_downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "count(c8)", "spread(c1)",
        "stddev(c2)", "hyperloglog(c11)", "min(t1)", "max(t2)", "sum(t3)", "first(t4)", "last(t5)", "count(t8)", "spread(t1)", "stddev(t2)"]

        self.stb_data_filter_sql = f'ts >= {self.date_time}+1s and c1 = 1 or c2 > 1 and c3 != 4 or c4 <= 3 and c9 <> 0 or c10 is not Null or c11 is Null or \
                c12 between "na" and "nchar4" and c11 not between "bi" and "binary" and c12 match "nchar[19]" and c12 nmatch "nchar[25]" or c13 = True or \
                c5 in (1, 2, 3) or c6 not in (6, 7) and c12 like "nch%" and c11 not like "bina_" and c6 < 10 or c12 is Null or c8 >= 4 and t1 = 1 or t2 > 1 \
                and t3 != 4 or c4 <= 3 and t9 <> 0 or t10 is not Null or t11 is Null or t12 between "na" and "nchar4" and t11 not between "bi" and "binary" \
                or t12 match "nchar[19]" or t12 nmatch "nchar[25]" or t13 = True or t5 in (1, 2, 3) or t6 not in (6, 7) and t12 like "nch%" \
                and t11 not like "bina_" and t6 <= 10 or t12 is Null or t8 >= 4'
        self.tb_data_filter_sql = self.stb_data_filter_sql.partition(" and t1")[0]

        self.filter_source_select_elm = "*"
        self.stb_filter_des_select_elm = "ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13"
        self.partitial_stb_filter_des_select_elm = ",".join(self.stb_filter_des_select_elm.split(",")[:3])
        self.exchange_stb_filter_des_select_elm = ",".join([self.stb_filter_des_select_elm.split(",")[0], self.stb_filter_des_select_elm.split(",")[2], self.stb_filter_des_select_elm.split(",")[1]])
        self.partitial_ext_tb_source_select_str = ','.join(self.downsampling_function_list[0:2])
        self.tb_filter_des_select_elm = self.stb_filter_des_select_elm.partition(", t1")[0]
        self.tag_filter_des_select_elm = self.stb_filter_des_select_elm.partition("c13, ")[2]
        self.partition_by_stb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.partition_by_downsampling_function_list)))
        self.partition_by_stb_source_select_str = ','.join(self.partition_by_downsampling_function_list)
        self.exchange_tag_filter_des_select_elm = ",".join([self.stb_filter_des_select_elm.partition("c13, ")[2].split(",")[0], self.stb_filter_des_select_elm.partition("c13, ")[2].split(",")[2], self.stb_filter_des_select_elm.partition("c13, ")[2].split(",")[1]])
        self.partitial_tag_filter_des_select_elm = ",".join(self.stb_filter_des_select_elm.partition("c13, ")[2].split(",")[:3])
        self.partitial_tag_stb_filter_des_select_elm = "ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, t1, t3, t2, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13"
        self.cast_tag_filter_des_select_elm = "t5,t11,t13"
        self.cast_tag_stb_filter_des_select_elm = "ts, t1, t2, t3, t4, cast(t1 as TINYINT UNSIGNED), t6, t7, t8, t9, t10, cast(t2 as varchar(256)), t12, cast(t3 as bool)"
        self.tag_count = len(self.tag_filter_des_select_elm.split(","))
        self.state_window_range = list()
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
        if platform.system().lower() == 'windows':
            win_sep = "\\"
            buildPath = buildPath.replace(win_sep,'/')

        return buildPath
    
    def getTaosdPath(self, dnodeID="dnode1"):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        taosdPath = buildPath + "/../sim/" + dnodeID
        tdLog.info("taosdPath: %s" % taosdPath)
        return taosdPath


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

    def gen_tag_col_str(self, gen_type, data_type, count):
        """
        gen multi tags or cols by gen_type
        """
        return ','.join(map(lambda i: f'{gen_type}{i} {data_type}', range(count)))

    # stream
    def create_stream(self, stream_name, des_table, source_sql, trigger_mode=None, watermark=None, max_delay=None, ignore_expired=None, ignore_update=None, subtable_value=None, fill_value=None, fill_history_value=None, stb_field_name_value=None, tag_value=None, use_exist_stb=False, use_except=False):
        """create_stream

        Args:
            stream_name (str): stream_name
            des_table (str): target stable
            source_sql (str): stream sql
            trigger_mode (str, optional): at_once/window_close/max_delay. Defaults to None.
            watermark (str, optional): watermark time. Defaults to None.
            max_delay (str, optional): max_delay time. Defaults to None.
            ignore_expired (int, optional): ignore expired data. Defaults to None.
            ignore_update (int, optional): ignore update data. Defaults to None.
            subtable_value (str, optional): subtable. Defaults to None.
            fill_value (str, optional): fill. Defaults to None.
            fill_history_value (int, optional): 0/1. Defaults to None.
            stb_field_name_value (str, optional): existed stb. Defaults to None.
            tag_value (str, optional): custom tag. Defaults to None.
            use_exist_stb (bool, optional): use existed stb tag. Defaults to False.
            use_except (bool, optional): Exception tag. Defaults to False.

        Returns:
            str: stream
        """
        if subtable_value is None:
            subtable = ""
        else:
            subtable = f'subtable({subtable_value})'

        if fill_value is None:
            fill = ""
        else:
            fill = f'fill({fill_value})'

        if fill_history_value is None:
            fill_history = ""
        else:
            fill_history = f'fill_history {fill_history_value}'

        if use_exist_stb:
            if stb_field_name_value is None:
                stb_field_name = ""
            else:
                stb_field_name = f'({stb_field_name_value})'

            if tag_value is None:
                tags = ""
            else:
                tags = f'tags({tag_value})'
        else:
            stb_field_name = ""
            tags = ""


        if trigger_mode is None:
            stream_options = ""
            if watermark is not None:
                stream_options = f'watermark {watermark}'
            if ignore_expired:
                stream_options += f" ignore expired {ignore_expired}"
            else:
                stream_options += f" ignore expired 0"
            if ignore_update:
                stream_options += f" ignore update {ignore_update}"
            else:
                stream_options += f" ignore update 0"
            if not use_except:
                tdSql.execute(f'create stream if not exists {stream_name} trigger at_once {stream_options} {fill_history} into {des_table} {subtable} as {source_sql} {fill};')
                time.sleep(self.create_stream_sleep)
                return None
            else:
                return f'create stream if not exists {stream_name} {stream_options} {fill_history} into {des_table} {subtable} as {source_sql} {fill};'
        else:
            if watermark is None:
                if trigger_mode == "max_delay":
                    stream_options = f'trigger {trigger_mode} {max_delay}'
                else:
                    stream_options = f'trigger {trigger_mode}'
            else:
                if trigger_mode == "max_delay":
                    stream_options = f'trigger {trigger_mode} {max_delay} watermark {watermark}'
                else:
                    stream_options = f'trigger {trigger_mode} watermark {watermark}'
            if ignore_expired:
                stream_options += f" ignore expired {ignore_expired}"
            else:
                stream_options += f" ignore expired 0"

            if ignore_update:
                stream_options += f" ignore update {ignore_update}"
            else:
                stream_options += f" ignore update 0"
            if not use_except:
                tdSql.execute(f'create stream if not exists {stream_name} {stream_options} {fill_history} into {des_table}{stb_field_name} {tags} {subtable} as {source_sql} {fill};')
                time.sleep(self.create_stream_sleep)
                return None
            else:
                return f'create stream if not exists {stream_name} {stream_options} {fill_history} into {des_table}{stb_field_name} {tags} {subtable} as {source_sql} {fill};'

    def pause_stream(self, stream_name, if_exist=True, if_not_exist=False):
        """pause_stream

        Args:
            stream_name (str): stream_name
            if_exist (bool, optional): Defaults to True.
            if_not_exist (bool, optional): Defaults to False.
        """
        if_exist_value = "if exists" if if_exist else ""
        if_not_exist_value = "if not exists" if if_not_exist else ""
        tdSql.execute(f'pause stream {if_exist_value} {if_not_exist_value} {stream_name}')

    def resume_stream(self, stream_name, if_exist=True, if_not_exist=False, ignore_untreated=False):
        """resume_stream

        Args:
            stream_name (str): stream_name
            if_exist (bool, optional): Defaults to True.
            if_not_exist (bool, optional): Defaults to False.
            ignore_untreated (bool, optional): Defaults to False.
        """
        if_exist_value = "if exists" if if_exist else ""
        if_not_exist_value = "if not exists" if if_not_exist else ""
        ignore_untreated_value = "ignore untreated" if ignore_untreated else ""
        tdSql.execute(f'resume stream {if_exist_value} {if_not_exist_value} {ignore_untreated_value} {stream_name}')

    def drop_all_streams(self):
        """drop all streams
        """
        tdSql.query("show streams")
        stream_name_list = list(map(lambda x: x[0], tdSql.queryResult))
        for stream_name in stream_name_list:
            tdSql.execute(f'drop stream if exists {stream_name};')

    def drop_db(self, dbname="test"):
        """drop a db

        Args:
            dbname (str, optional): Defaults to "test".
        """
        if dbname[0].isdigit():
            tdSql.execute(f'drop database if exists `{dbname}`')
        else:
            tdSql.execute(f'drop database if exists {dbname}')

    def drop_all_db(self):
        """drop all databases
        """
        tdSql.query("show databases;")
        db_list = list(map(lambda x: x[0], tdSql.queryResult))
        for dbname in db_list:
            if dbname not in self.white_list and "telegraf" not in dbname:
                tdSql.execute(f'drop database if exists `{dbname}`')

    def time_cast(self, time_value, split_symbol="+"):
        """cast bigint to timestamp

        Args:
            time_value (bigint): ts
            split_symbol (str, optional): split sympol. Defaults to "+".

        Returns:
            _type_: timestamp
        """
        ts_value = str(time_value).split(split_symbol)[0]
        if split_symbol in str(time_value):
            ts_value_offset = str(time_value).split(split_symbol)[1]
        else:
            ts_value_offset = "0s"
        return f'cast({ts_value} as timestamp){split_symbol}{ts_value_offset}'

    def clean_env(self):
        """drop all streams and databases
        """
        self.drop_all_streams()
        self.drop_all_db()

    def set_precision_offset(self, precision):
        if precision == "ms":
            self.offset = 1000
        elif precision == "us":
            self.offset = 1000000
        elif precision == "ns":
            self.offset = 1000000000
        else:
            pass

    def genTs(self, precision="ms", ts="", protype="taosc", ns_tag=None):
        """generate ts

        Args:
            precision (str, optional): db precision. Defaults to "ms".
            ts (str, optional): input ts. Defaults to "".
            protype (str, optional): "taosc" or "restful". Defaults to "taosc".
            ns_tag (_type_, optional): use ns. Defaults to None.

        Returns:
            timestamp, datetime: timestamp and datetime
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

    def sgen_column_type_str(self, column_elm_list):
        """generage column type str

        Args:
            column_elm_list (list): column_elm_list
        """
        self.column_type_str = ""
        if column_elm_list is None:
            self.column_type_str = self.gen_default_column_str()
        else:
            for column_elm in column_elm_list:
                if "count" in column_elm:
                    total_count = int(column_elm["count"])
                else:
                    total_count = 1
                if total_count > 0:
                    for _ in range(total_count):
                        self.column_type_str += f'{self.default_colname_prefix}{self.default_column_index_start_num} {column_elm["type"]}, '
                        if column_elm["type"] in ["varchar", "binary", "nchar"]:
                            self.column_type_str = self.column_type_str.rstrip()[:-1] + f'({column_elm["len"]}), '
                        self.default_column_index_start_num += 1
                else:
                    continue
            self.column_type_str = self.default_colts_name + " timestamp, " + self.column_type_str.rstrip()[:-1]

    def sgen_tag_type_str(self, tag_elm_list):
        """generage tag type str

        Args:
            tag_elm_list (list): tag_elm_list
        """
        self.tag_type_str = ""
        if tag_elm_list is None:
            self.tag_type_str = self.gen_default_tag_str()
        else:
            for tag_elm in tag_elm_list:
                if "count" in tag_elm:
                    total_count = int(tag_elm["count"])
                else:
                    total_count = 1
                if total_count > 0:
                    for _ in range(total_count):
                        self.tag_type_str += f'{self.default_tagname_prefix}{self.default_tag_index_start_num} {tag_elm["type"]}, '
                        if tag_elm["type"] in ["varchar", "binary", "nchar"]:
                            self.tag_type_str = self.tag_type_str.rstrip()[:-1] + f'({tag_elm["len"]}), '
                        self.default_tag_index_start_num += 1
                else:
                    continue
            self.tag_type_str = self.tag_type_str.rstrip()[:-1]
            if self.need_tagts:
                self.tag_type_str = self.default_tagts_name + " timestamp, " + self.tag_type_str

    def sgen_tag_value_list(self, tag_elm_list, ts_value=None):
        """generage tag value str

        Args:
            tag_elm_list (list): _description_
            ts_value (timestamp, optional): Defaults to None.
        """
        if self.need_tagts:
            self.ts_value = self.genTs()[0]
        if ts_value is not None:
            self.ts_value = ts_value

        if tag_elm_list is None:
            self.tag_value_list = list(map(lambda i: self.gen_random_type_value(i, self.default_varchar_length, self.default_varchar_datatype, self.default_nchar_length, self.default_nchar_datatype), self.full_type_list))
        else:
            for tag_elm in tag_elm_list:
                if "count" in tag_elm:
                    total_count = int(tag_elm["count"])
                else:
                    total_count = 1
                if total_count > 0:
                    for _ in range(total_count):
                        if tag_elm["type"] in ["varchar", "binary", "nchar"]:
                            self.tag_value_list.append(self.gen_random_type_value(tag_elm["type"], tag_elm["len"], self.default_varchar_datatype, tag_elm["len"], self.default_nchar_datatype))
                        else:
                            self.tag_value_list.append(self.gen_random_type_value(tag_elm["type"], "", "", "", ""))
                else:
                    continue
        # if self.need_tagts and self.ts_value is not None and len(str(self.ts_value)) > 0:
        if self.need_tagts:
            self.tag_value_list = [self.ts_value] + self.tag_value_list

    def screateDb(self, dbname="test", drop_db=True, **kwargs):
        """create database

        Args:
            dbname (str, optional): Defaults to "test".
            drop_db (bool, optional): Defaults to True.
        """
        tdLog.info("creating db ...")
        db_params = ""
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                if param == "precision":
                    db_params += f'{param} "{value}" '
                else:
                    db_params += f'{param} {value} '
        if drop_db:
            self.drop_db(dbname)
        tdSql.execute(f'create database if not exists {dbname} {db_params}')
        tdSql.execute(f'use {dbname}')

    def screate_stable(self, dbname=None, stbname="stb", use_name="table", column_elm_list=None, tag_elm_list=None,
                     need_tagts=False, count=1, default_stbname_prefix="stb", default_stbname_index_start_num=1,
                     default_column_index_start_num=1, default_tag_index_start_num=1, **kwargs):
        """_summary_

        Args:
            dbname (str, optional): Defaults to None.
            stbname (str, optional): Defaults to "stb".
            use_name (str, optional): stable/table, Defaults to "table".
            column_elm_list (list, optional): use for sgen_column_type_str(), Defaults to None.
            tag_elm_list (list, optional): use for sgen_tag_type_str(), Defaults to None.
            need_tagts (bool, optional): tag use timestamp, Defaults to False.
            count (int, optional): stable count, Defaults to 1.
            default_stbname_prefix (str, optional): Defaults to "stb".
            default_stbname_index_start_num (int, optional): Defaults to 1.
            default_column_index_start_num (int, optional): Defaults to 1.
            default_tag_index_start_num (int, optional): Defaults to 1.
        """
        tdLog.info("creating stable ...")
        if dbname is not None:
            self.dbname = dbname
        self.need_tagts = need_tagts
        self.default_stbname_prefix = default_stbname_prefix
        self.default_stbname_index_start_num = default_stbname_index_start_num
        self.default_column_index_start_num = default_column_index_start_num
        self.default_tag_index_start_num = default_tag_index_start_num
        stb_params = ""
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                stb_params += f'{param} "{value}" '
        self.sgen_column_type_str(column_elm_list)
        self.sgen_tag_type_str(tag_elm_list)
        if self.dbname is not None:
            stb_name = f'{self.dbname}.{stbname}'
        else:
            stb_name = stbname
        if int(count) <= 1:
            create_stable_sql = f'create {use_name} {stb_name} ({self.column_type_str}) tags ({self.tag_type_str}) {stb_params};'
            tdSql.execute(create_stable_sql)
        else:
            for _ in range(count):
                create_stable_sql = f'create {use_name} {self.dbname}.{default_stbname_prefix}{default_stbname_index_start_num} ({self.column_type_str}) tags ({self.tag_type_str}) {stb_params};'
                default_stbname_index_start_num += 1
                tdSql.execute(create_stable_sql)

    def screate_ctable(self, dbname=None, stbname=None, ctbname="ctb", use_name="table", tag_elm_list=None, ts_value=None, count=1, default_varchar_datatype="letters", default_nchar_datatype="letters", default_ctbname_prefix="ctb", default_ctbname_index_start_num=1, **kwargs):
        """_summary_

        Args:
            dbname (str, optional): Defaults to None.
            stbname (str, optional): Defaults to None.
            ctbname (str, optional): Defaults to "ctb".
            use_name (str, optional): Defaults to "table".
            tag_elm_list (list, optional): use for sgen_tag_type_str(), Defaults to None.
            ts_value (timestamp, optional): Defaults to None.
            count (int, optional): ctb count, Defaults to 1.
            default_varchar_datatype (str, optional): Defaults to "letters".
            default_nchar_datatype (str, optional): Defaults to "letters".
            default_ctbname_prefix (str, optional): Defaults to "ctb".
            default_ctbname_index_start_num (int, optional): Defaults to 1.
        """
        tdLog.info("creating childtable ...")
        self.default_varchar_datatype = default_varchar_datatype
        self.default_nchar_datatype = default_nchar_datatype
        self.default_ctbname_prefix = default_ctbname_prefix
        self.default_ctbname_index_start_num = default_ctbname_index_start_num
        ctb_params = ""
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                ctb_params += f'{param} "{value}" '
        self.sgen_tag_value_list(tag_elm_list, ts_value)
        tag_value_str = ""
        # tag_value_str = ", ".join(str(v) for v in self.tag_value_list)
        for tag_value in self.tag_value_list:
            if isinstance(tag_value, str):
                tag_value_str += f'"{tag_value}", '
            else:
                tag_value_str += f'{tag_value}, '
        tag_value_str = tag_value_str.rstrip()[:-1]
        if dbname is not None:
            self.dbname = dbname
            ctb_name = f'{self.dbname}.{ctbname}'
        else:
            ctb_name = ctbname
        if stbname is not None:
            stb_name = stbname
        if int(count) <= 1:
            create_ctable_sql = f'create {use_name} {ctb_name} using {stb_name} tags ({tag_value_str}) {ctb_params};'
            tdSql.execute(create_ctable_sql)
        else:
            for _ in range(count):
                create_stable_sql = f'create {use_name} {self.dbname}.{default_ctbname_prefix}{default_ctbname_index_start_num} using {self.stb_name} tags ({tag_value_str}) {ctb_params};'
                default_ctbname_index_start_num += 1
                tdSql.execute(create_stable_sql)

    def sgen_column_value_list(self, column_elm_list, need_null, ts_value=None):
        """_summary_

        Args:
            column_elm_list (list): gen_random_type_value()
            need_null (bool): if insert null
            ts_value (timestamp, optional): Defaults to None.
        """
        self.column_value_list = list()
        self.ts_value = self.genTs()[0]
        if ts_value is not None:
            self.ts_value = ts_value

        if column_elm_list is None:
            self.column_value_list = list(map(lambda i: self.gen_random_type_value(i, self.default_varchar_length, self.default_varchar_datatype, self.default_nchar_length, self.default_nchar_datatype), self.full_type_list))
        else:
            for column_elm in column_elm_list:
                if "count" in column_elm:
                    total_count = int(column_elm["count"])
                else:
                    total_count = 1
                if total_count > 0:
                    for _ in range(total_count):
                        if column_elm["type"] in ["varchar", "binary", "nchar"]:
                            self.column_value_list.append(self.gen_random_type_value(column_elm["type"], column_elm["len"], self.default_varchar_datatype, column_elm["len"], self.default_nchar_datatype))
                        else:
                            self.column_value_list.append(self.gen_random_type_value(column_elm["type"], "", "", "", ""))
                else:
                    continue
        if need_null:
            for i in range(int(len(self.column_value_list)/2)):
                index_num = random.randint(0, len(self.column_value_list)-1)
                self.column_value_list[index_num] = None
        self.column_value_list = [self.ts_value] + self.column_value_list

    def screate_table(self, dbname=None, tbname="tb", use_name="table", column_elm_list=None,
                    count=1, default_tbname_prefix="tb", default_tbname_index_start_num=1,
                    default_column_index_start_num=1, **kwargs):
        """create ctable

        Args:
            dbname (str, optional): Defaults to None.
            tbname (str, optional): Defaults to "tb".
            use_name (str, optional): Defaults to "table".
            column_elm_list (list, optional): Defaults to None.
            count (int, optional): Defaults to 1.
            default_tbname_prefix (str, optional): Defaults to "tb".
            default_tbname_index_start_num (int, optional): Defaults to 1.
            default_column_index_start_num (int, optional): Defaults to 1.
        """
        tdLog.info("creating table ...")
        if dbname is not None:
            self.dbname = dbname
        self.default_tbname_prefix = default_tbname_prefix
        self.default_tbname_index_start_num = default_tbname_index_start_num
        self.default_column_index_start_num = default_column_index_start_num
        tb_params = ""
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                tb_params += f'{param} "{value}" '
        self.sgen_column_type_str(column_elm_list)
        if self.dbname is not None:
            tb_name = f'{self.dbname}.{tbname}'
        else:
            tb_name = tbname
        if int(count) <= 1:
            create_table_sql = f'create {use_name} {tb_name} ({self.column_type_str}) {tb_params};'
            tdSql.execute(create_table_sql)
        else:
            for _ in range(count):
                create_table_sql = f'create {use_name} {self.dbname}.{default_tbname_prefix}{default_tbname_index_start_num} ({self.column_type_str}) {tb_params};'
                default_tbname_index_start_num += 1
                tdSql.execute(create_table_sql)

    def sinsert_rows(self, dbname=None, tbname=None, column_ele_list=None, ts_value=None, count=1, need_null=False):
        """insert rows

        Args:
            dbname (str, optional): Defaults to None.
            tbname (str, optional): Defaults to None.
            column_ele_list (list, optional): Defaults to None.
            ts_value (timestamp, optional): Defaults to None.
            count (int, optional): Defaults to 1.
            need_null (bool, optional): Defaults to False.
        """
        tdLog.info("stream inserting ...")
        if dbname is not None:
            self.dbname = dbname
            if tbname is not None:
                self.tbname = f'{self.dbname}.{tbname}'
        else:
            if tbname is not None:
                self.tbname = tbname

        self.sgen_column_value_list(column_ele_list, need_null, ts_value)
        # column_value_str = ", ".join(str(v) for v in self.column_value_list)
        column_value_str = ""
        for column_value in self.column_value_list:
            if column_value is None:
                column_value_str += 'Null, '
            elif isinstance(column_value, str) and "+" not in column_value and "-" not in column_value:
                column_value_str += f'"{column_value}", '
            else:
                column_value_str += f'{column_value}, '
        column_value_str = column_value_str.rstrip()[:-1]
        if int(count) <= 1:
            insert_sql = f'insert into {self.tbname} values ({column_value_str});'
            tdSql.execute(insert_sql)
        else:
            for num in range(count):
                ts_value = self.genTs()[0]
                self.sgen_column_value_list(column_ele_list, need_null, f'{ts_value}+{num}s')
                column_value_str = ""
                for column_value in self.column_value_list:
                    if column_value is None:
                        column_value_str += 'Null, '
                    elif isinstance(column_value, str) and "+" not in column_value:
                        column_value_str += f'"{column_value}", '
                    else:
                        column_value_str += f'{column_value}, '
                column_value_str = column_value_str.rstrip()[:-1]
                insert_sql = f'insert into {self.tbname} values ({column_value_str});'
                tdSql.execute(insert_sql)

    def sdelete_rows(self, dbname=None, tbname=None, start_ts=None, end_ts=None, ts_key=None):
        """delete rows

        Args:
            dbname (str, optional): Defaults to None.
            tbname (str, optional): Defaults to None.
            start_ts (timestamp, optional): range start. Defaults to None.
            end_ts (timestamp, optional): range end. Defaults to None.
            ts_key (str, optional): timestamp column name. Defaults to None.
        """
        if dbname is not None:
            self.dbname = dbname
            if tbname is not None:
                self.tbname = f'{self.dbname}.{tbname}'
        else:
            if tbname is not None:
                self.tbname = tbname
        if ts_key is None:
            ts_col_name = self.default_colts_name
        else:
            ts_col_name = ts_key

        base_del_sql = f'delete from {self.tbname} '
        if end_ts is not None:
            if ":" in start_ts and "-" in start_ts:
                start_ts = f"{start_ts}"
            if ":" in end_ts and "-" in end_ts:
                end_ts = f"{end_ts}"
            base_del_sql += f'where {ts_col_name} between {start_ts} and {end_ts};'
        else:
            if start_ts is not None:
                if ":" in start_ts and "-" in start_ts:
                    start_ts = f"{start_ts}"
                base_del_sql += f'where {ts_col_name} = {start_ts};'
        tdSql.execute(base_del_sql)

    def check_stream_field_type(self, sql, input_function):
        """confirm stream field

        Args:
            sql (str): input sql
            input_function (str): scalar
        """
        tdSql.query(sql)
        res = tdSql.queryResult
        if input_function in ["acos", "asin", "atan", "cos", "log", "pow", "sin", "sqrt", "tan"]:
            tdSql.checkEqual(res[1][1], "DOUBLE")
            tdSql.checkEqual(res[2][1], "DOUBLE")
        elif input_function in ["lower", "ltrim", "rtrim", "upper"]:
            tdSql.checkEqual(res[1][1], "VARCHAR")
            tdSql.checkEqual(res[2][1], "VARCHAR")
            tdSql.checkEqual(res[3][1], "NCHAR")
        elif input_function in ["char_length", "length"]:
            tdSql.checkEqual(res[1][1], "BIGINT")
            tdSql.checkEqual(res[2][1], "BIGINT")
            tdSql.checkEqual(res[3][1], "BIGINT")
        elif input_function in ["concat", "concat_ws"]:
            tdSql.checkEqual(res[1][1], "VARCHAR")
            tdSql.checkEqual(res[2][1], "NCHAR")
            tdSql.checkEqual(res[3][1], "NCHAR")
            tdSql.checkEqual(res[4][1], "NCHAR")
        elif input_function in ["substr"]:
            tdSql.checkEqual(res[1][1], "VARCHAR")
            tdSql.checkEqual(res[2][1], "VARCHAR")
            tdSql.checkEqual(res[3][1], "VARCHAR")
            tdSql.checkEqual(res[4][1], "NCHAR")
        else:
            tdSql.checkEqual(res[1][1], "INT")
            tdSql.checkEqual(res[2][1], "DOUBLE")

    def round_handle(self, input_list):
        """round list elem

        Args:
            input_list (list): input value list

        Returns:
            _type_: round list
        """
        tdLog.info("round rows ...")
        final_list = list()
        for i in input_list:
            tmpl = list()
            for j in i:
                if type(j) != datetime and type(j) != str:
                    tmpl.append(round(j, 1))
                else:
                    tmpl.append(j)
            final_list.append(tmpl)
        return final_list

    def float_handle(self, input_list):
        """float list elem

        Args:
            input_list (list): input value list

        Returns:
            _type_: float list
        """
        tdLog.info("float rows ...")
        final_list = list()
        for i in input_list:
            tmpl = list()
            for j_i,j_v in enumerate(i):
                if type(j_v) != datetime and j_v is not None and str(j_v).isdigit() and j_i <= 12:
                    tmpl.append(float(j_v))
                else:
                    tmpl.append(j_v)
            final_list.append(tuple(tmpl))
        return final_list

    def str_ts_trans_bigint(self, str_ts):
        """trans str ts to bigint

        Args:
            str_ts (str): human-date

        Returns:
            bigint: bigint-ts
        """
        tdSql.query(f'select cast({str_ts} as bigint)')
        return tdSql.queryResult[0][0]

    def cast_query_data(self, query_data):
        """cast query-result for existed-stb

        Args:
            query_data (list): query data list

        Returns:
            list: new list after cast
        """
        tdLog.info("cast query data ...")
        col_type_list = self.column_type_str.split(',')
        tag_type_list = self.tag_type_str.split(',')
        col_tag_type_list = col_type_list + tag_type_list
        nl = list()
        for query_data_t in query_data:
            query_data_l = list(query_data_t)
            for i,v in enumerate(query_data_l):
                if v is not None:
                    if " ".join(col_tag_type_list[i].strip().split(" ")[1:]) == "nchar(6)":
                        tdSql.query(f'select cast("{v}" as binary(6))')
                    else:
                        tdSql.query(f'select cast("{v}" as {" ".join(col_tag_type_list[i].strip().split(" ")[1:])})')
                    query_data_l[i] = tdSql.queryResult[0][0]
                else:
                    query_data_l[i] = v
            nl.append(tuple(query_data_l))
        return nl

    def trans_time_to_s(self, runtime):
        """trans time to s

        Args:
            runtime (str): 1d/1h/1m...

        Returns:
            int: second
        """
        if "d" in str(runtime).lower():
            d_num = re.findall("\d+\.?\d*", runtime.replace(" ", ""))[0]
            s_num = float(d_num) * 24 * 60 * 60
        elif "h" in str(runtime).lower():
            h_num = re.findall("\d+\.?\d*", runtime.replace(" ", ""))[0]
            s_num = float(h_num) * 60 * 60
        elif "m" in str(runtime).lower():
            m_num = re.findall("\d+\.?\d*", runtime.replace(" ", ""))[0]
            s_num = float(m_num) * 60
        elif "s" in str(runtime).lower():
            s_num = re.findall("\d+\.?\d*", runtime.replace(" ", ""))[0]
        else:
            s_num = 60
        return int(s_num)

    def check_query_data(self, sql1, sql2, sorted=False, fill_value=None, tag_value_list=None, defined_tag_count=None, partition=True, use_exist_stb=False, subtable=None, reverse_check=False):
        """confirm query result

        Args:
            sql1 (str): select ....
            sql2 (str): select ....
            sorted (bool, optional): if sort result list. Defaults to False.
            fill_value (str, optional): fill. Defaults to None.
            tag_value_list (list, optional): Defaults to None.
            defined_tag_count (int, optional): Defaults to None.
            partition (bool, optional): Defaults to True.
            use_exist_stb (bool, optional): Defaults to False.
            subtable (str, optional): Defaults to None.
            reverse_check (bool, optional): not equal. Defaults to False.

        Returns:
            bool: False if failed
        """
        tdLog.info("checking query data ...")
        if tag_value_list:
            dvalue = len(self.tag_type_str.split(',')) - defined_tag_count
        tdSql.query(sql1)
        res1 = tdSql.queryResult
        tdSql.query(sql2)
        res2 = self.cast_query_data(tdSql.queryResult) if tag_value_list or use_exist_stb else tdSql.queryResult
        tdSql.sql = sql1
        new_list = list()
        if tag_value_list:
            res1 = self.float_handle(res1)
            res2 = self.float_handle(res2)
            for i,v in enumerate(res2):
                if i < len(tag_value_list):
                    if partition:
                        new_list.append(tuple(list(v)[:-(dvalue+defined_tag_count)] + list(tag_value_list[i]) + [None]*dvalue))
                    else:
                        new_list.append(tuple(list(v)[:-(dvalue+defined_tag_count)] + [None]*len(self.tag_type_str.split(','))))
                    res2 = new_list
        else:
            if use_exist_stb:
                res1 = self.float_handle(res1)
                res2 = self.float_handle(res2)
                for i,v in enumerate(res2):
                    new_list.append(tuple(list(v)[:-(13)] + [None]*len(self.tag_type_str.split(','))))
                res2 = new_list

        latency = 0
        if sorted:
            res1.sort()
            res2.sort()
        if fill_value == "LINEAR":
            res1 = self.round_handle(res1)
            res2 = self.round_handle(res2)
        if not reverse_check:
            while res1 != res2:
                tdLog.info("query retrying ...")
                new_list = list()
                tdSql.query(sql1)
                res1 = tdSql.queryResult
                tdSql.query(sql2)
                # res2 = tdSql.queryResult
                res2 = self.cast_query_data(tdSql.queryResult) if tag_value_list or use_exist_stb else tdSql.queryResult
                tdSql.sql = sql1

                if tag_value_list:
                    res1 = self.float_handle(res1)
                    res2 = self.float_handle(res2)
                    for i,v in enumerate(res2):
                        if i < len(tag_value_list):
                            if partition:
                                new_list.append(tuple(list(v)[:-(dvalue+defined_tag_count)] + list(tag_value_list[i]) + [None]*dvalue))
                            else:
                                new_list.append(tuple(list(v)[:-(dvalue+defined_tag_count)] + [None]*len(self.tag_type_str.split(','))))
                            res2 = new_list
                else:
                    if use_exist_stb:
                        res1 = self.float_handle(res1)
                        res2 = self.float_handle(res2)
                        for i,v in enumerate(res2):
                            new_list.append(tuple(list(v)[:-(13)] + [None]*len(self.tag_type_str.split(','))))
                        res2 = new_list
                if sorted or tag_value_list:
                    res1.sort()
                    res2.sort()
                if fill_value == "LINEAR":
                    res1 = self.round_handle(res1)
                    res2 = self.round_handle(res2)
                if latency < self.stream_timeout:
                    latency += 0.2
                    time.sleep(0.2)
                else:
                    if latency == 0:
                        return False
                    tdSql.checkEqual(res1, res2)
                    # tdSql.checkEqual(res1, res2) if not reverse_check else tdSql.checkNotEqual(res1, res2)
        else:
            while res1 == res2:
                tdLog.info("query retrying ...")
                new_list = list()
                tdSql.query(sql1)
                res1 = tdSql.queryResult
                tdSql.query(sql2)
                # res2 = tdSql.queryResult
                res2 = self.cast_query_data(tdSql.queryResult) if tag_value_list or use_exist_stb else tdSql.queryResult
                tdSql.sql = sql1

                if tag_value_list:
                    res1 = self.float_handle(res1)
                    res2 = self.float_handle(res2)
                    for i,v in enumerate(res2):
                        if i < len(tag_value_list):
                            if partition:
                                new_list.append(tuple(list(v)[:-(dvalue+defined_tag_count)] + list(tag_value_list[i]) + [None]*dvalue))
                            else:
                                new_list.append(tuple(list(v)[:-(dvalue+defined_tag_count)] + [None]*len(self.tag_type_str.split(','))))
                            res2 = new_list
                else:
                    if use_exist_stb:
                        res1 = self.float_handle(res1)
                        res2 = self.float_handle(res2)
                        for i,v in enumerate(res2):
                            new_list.append(tuple(list(v)[:-(13)] + [None]*len(self.tag_type_str.split(','))))
                        res2 = new_list
                if sorted or tag_value_list:
                    res1.sort()
                    res2.sort()
                if fill_value == "LINEAR":
                    res1 = self.round_handle(res1)
                    res2 = self.round_handle(res2)
                if latency < self.stream_timeout:
                    latency += 0.5
                    time.sleep(0.5)
                else:
                    if latency == 0:
                        return False
                    tdSql.checkNotEqual(res1, res2)
                    # tdSql.checkEqual(res1, res2) if not reverse_check else tdSql.checkNotEqual(res1, res2)

    def check_stream_res(self, sql, expected_res, max_delay):
        """confirm stream result

        Args:
            sql (str): select ...
            expected_res (str): expected result
            max_delay (int): max_delay value

        Returns:
            bool: False if failed
        """
        tdSql.query(sql)
        latency = 0

        while tdSql.queryRows != expected_res:
            tdSql.query(sql)
            if latency < self.stream_timeout:
                latency += 0.2
                time.sleep(0.2)
            else:
                if max_delay is not None:
                    if latency == 0:
                        return False
                tdSql.checkEqual(tdSql.queryRows, expected_res)

    def check_stream(self, sql1, sql2, expected_count, max_delay=None):
        """confirm stream

        Args:
            sql1 (str): select ...
            sql2 (str): select ...
            expected_count (int): expected_count
            max_delay (int, optional): max_delay value. Defaults to None.
        """
        self.check_stream_res(sql1, expected_count, max_delay)
        self.check_query_data(sql1, sql2)

    def cal_watermark_window_close_session_endts(self, start_ts, watermark=None, session=None):
        """cal endts for close window

        Args:
            start_ts (epoch time): self.date_time
            watermark (int, optional): > session. Defaults to None.
            session (int, optional): Defaults to None.

        Returns:
            int: as followed
        """
        if watermark is not None:
            return start_ts + watermark*self.offset + 1
        else:
            return start_ts + session*self.offset + 1

    def cal_watermark_window_close_interval_endts(self, start_ts, interval, watermark=None):
        """cal endts for close window

        Args:
            start_ts (epoch time): self.date_time
            interval (int): [s]
            watermark (int, optional): [s]. Defaults to None.

        Returns:
            _type_: _description_
        """
        if watermark is not None:
            return int(start_ts/self.offset)*self.offset + (interval - (int(start_ts/self.offset))%interval)*self.offset + watermark*self.offset
        else:
            return int(start_ts/self.offset)*self.offset + (interval - (int(start_ts/self.offset))%interval)*self.offset

    def update_delete_history_data(self, delete):
        """update and delete history data

        Args:
            delete (bool): True/False
        """
        self.sinsert_rows(tbname=self.ctb_name, ts_value=self.record_history_ts)
        self.sinsert_rows(tbname=self.tb_name, ts_value=self.record_history_ts)
        if delete:
            self.sdelete_rows(tbname=self.ctb_name, start_ts=self.time_cast(self.record_history_ts, "-"))
            self.sdelete_rows(tbname=self.tb_name, start_ts=self.time_cast(self.record_history_ts, "-"))

    def prepare_data(self, interval=None, watermark=None, session=None, state_window=None, state_window_max=127, interation=3, range_count=None, precision="ms", fill_history_value=0, ext_stb=None):
        """prepare stream data

        Args:
            interval (int, optional): Defaults to None.
            watermark (int, optional): Defaults to None.
            session (int, optional): Defaults to None.
            state_window (str, optional): Defaults to None.
            state_window_max (int, optional): Defaults to 127.
            interation (int, optional): Defaults to 3.
            range_count (int, optional): Defaults to None.
            precision (str, optional): Defaults to "ms".
            fill_history_value (int, optional): Defaults to 0.
            ext_stb (bool, optional): Defaults to None.
        """
        self.clean_env()
        self.dataDict = {
            "stb_name" : f"{self.case_name}_stb",
            "ctb_name" : f"{self.case_name}_ct1",
            "tb_name" : f"{self.case_name}_tb1",
            "ext_stb_name" : f"ext_{self.case_name}_stb",
            "ext_ctb_name" : f"ext_{self.case_name}_ct1",
            "ext_tb_name" : f"ext_{self.case_name}_tb1",
            "interval" : interval,
            "watermark": watermark,
            "session": session,
            "state_window": state_window,
            "state_window_max": state_window_max,
            "iteration": interation,
            "range_count": range_count,
            "start_ts": 1655903478508,
        }
        if range_count is not None:
            self.range_count = range_count
        if precision is not None:
            self.precision = precision
        self.set_precision_offset(self.precision)

        self.stb_name = self.dataDict["stb_name"]
        self.ctb_name = self.dataDict["ctb_name"]
        self.tb_name = self.dataDict["tb_name"]
        self.ext_stb_name = self.dataDict["ext_stb_name"]
        self.ext_ctb_name = self.dataDict["ext_ctb_name"]
        self.ext_tb_name = self.dataDict["ext_tb_name"]
        self.stb_stream_des_table = f'{self.stb_name}{self.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.des_table_suffix}'
        self.ext_stb_stream_des_table = f'{self.ext_stb_name}{self.des_table_suffix}'
        self.ext_ctb_stream_des_table = f'{self.ext_ctb_name}{self.des_table_suffix}'
        self.ext_tb_stream_des_table = f'{self.ext_tb_name}{self.des_table_suffix}'
        self.date_time = self.genTs(precision=self.precision)[0]

        self.screateDb(dbname=self.dbname, precision=self.precision)
        if ext_stb:
            self.screate_stable(dbname=self.dbname, stbname=self.ext_stb_stream_des_table)
            self.screate_ctable(dbname=self.dbname, stbname=self.ext_stb_stream_des_table, ctbname=self.ext_ctb_stream_des_table)
            self.screate_table(dbname=self.dbname, tbname=self.ext_tb_stream_des_table)
        self.screate_stable(dbname=self.dbname, stbname=self.stb_name)
        self.screate_ctable(dbname=self.dbname, stbname=self.stb_name, ctbname=self.ctb_name)
        self.screate_table(dbname=self.dbname, tbname=self.tb_name)
        if fill_history_value == 1:
            for i in range(self.range_count):
                ts_value = str(self.date_time)+f'-{self.default_interval*(i+1)}s'
                self.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
                self.sinsert_rows(tbname=self.tb_name, ts_value=ts_value)
                if i == 1:
                    self.record_history_ts = ts_value

    def get_subtable(self, tbname_pre):
        tdSql.query(f'show tables')
        tbname_list = list(map(lambda x:x[0], tdSql.queryResult))
        for tbname in tbname_list:
            if tbname_pre in tbname:
                return tbname

    def get_subtable_wait(self, tbname_pre):
        tbname = self.get_subtable(tbname_pre)
        latency = 0
        while tbname is None:
            tbname = self.get_subtable(tbname_pre)
            if latency < self.stream_timeout:
                latency += 1
                time.sleep(1)
        return tbname

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
