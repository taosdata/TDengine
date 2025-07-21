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
import os
from datetime import datetime
from .rest import TDRest
from .sql import TDSql
from .file import read_yaml
# import boundary
from .boundary import DataBoundary
from .sml_types import TDSmlProtocolType, TDSmlTimestampType
import numpy as np
import threading
import socket
import json
import re
from copy import deepcopy
from psutil import cpu_count
from .benchmark import gen_benchmark_json
import threadpool
from apscheduler.schedulers.background import BackgroundScheduler
import json

class TDCom:
    # log file name
    taostest_log_file_name = "test.log"
    # taostest install url variable
    taostest_install_url_variable = "TAOSTEST_INSTALL_URL"
    # default taostest install url
    taostest_install_url_default = "--extra-index-url http://192.168.1.131:8080/simple --trusted-host 192.168.1.131 taostest"
    # package server host variable
    package_server_host_variable = "PACKAGE_SERVER_HOST"
    # default package server host
    package_server_host_default = "192.168.1.131"
    # package server username variable
    package_server_username_variable = "PACKAGE_SERVER_USERNAME"
    # default package server username
    package_server_username_default = "root"
    # package server password variable
    package_server_password_variable = "PACKAGE_SERVER_PASSWORD"
    # default package server password
    package_server_password_default = "tbase125!"
    # package server path variable
    package_server_root_variable = "PACKAGE_SERVER_ROOT"
    # default package server root
    package_server_root_default = "/nas/TDengine"
    # docker image name variable
    docker_image_name_variable = "DOCKER_IMAGE_NAME"
    # default docker image name
    docker_image_name_default = "tdengine-ci:0.1"
    # container test root
    container_test_root = "/home/test_root"
    # entry point file name
    entry_point_file_name = "entrypoint.sh"
    # container entry point
    container_entry_point = "/home/entrypoint.sh"
    # container TDinternal source directory
    container_tdinternal_source_directory = "/home/TDinternal"
    # docker compose file name
    docker_compose_file_name = "docker-compose.yml"
    # system core pattern file
    system_core_pattern_file = "/proc/sys/kernel/core_pattern"
    # taostest container name
    taostest_container_name = "taostest"
    # default docker network name
    docker_network_name_default = "taosnet"
    # taostest log dir variable
    taostest_log_dir_variable = "TAOSTEST_LOG_DIR"
    # taostest enable sql recording variable
    # taostest_enable_sql_recording_variable = "TAOSTEST_SQL_RECORDING_ENABLED"
    taostest_database_replicas_variable = "DATABASE_REPLICAS"
    taostest_database_cachemodel_variable = "DATABASE_CACHEMODEL"
    # taosc query policy
    taostest_query_policy_variable = "DATABASE_QUERY_POLICY"

    def __init__(self, tdSql: TDSql, env_setting=None):
        self.tdSql = tdSql
        if env_setting is not None:
            for settings in env_setting["settings"]:
                if settings["name"].lower() == "taosadapter":
                    self.taosadapter_fqdn = random.choice(settings["fqdn"])
            self.tdRest = TDRest(env_setting=env_setting)
        else:
            self.tdRest = TDRest()
        self.api_type = "taosc"
        self.sml_type = None
        self.utc = None
        self.env_setting = env_setting
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
        self.default_tagts_name = "tag_ts"
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
        self.boundary_config = read_yaml(os.path.join(os.path.abspath(os.path.dirname(__file__)), "boundary.yaml"))
        self.Boundary = DataBoundary()
        self.white_list = ["statsd", "node_exporter", "collectd", "icinga2", "tcollector", "information_schema", "performance_schema"]
        self.stream_latency_log = None
        self.stream_timeout = 12

        self._type = "insert"
        self.thread : int   = 1 ,
        self._resultfile : str = None
        self._res_list: list = []
        self.result_dir : str = None
        self.json_file : str = None

    def get_components_setting(self, env_settings, component_name):
        for env_setting in env_settings:
            if env_setting["name"].lower() == component_name:
                return env_setting

    def set_sml_specified_value(self):
        env_setting = self.env_setting["settings"]
        for setting in env_setting:
            if setting["name"] == "taospy":
                if "smlChildTableName" in setting["spec"]["config"]:
                    self.smlChildTableName_value = setting["spec"]["config"]["smlChildTableName"].upper()
                if "defaultJSONStrType" in setting["spec"]["config"]:
                    self.defaultJSONStrType_value = setting["spec"]["config"]["defaultJSONStrType"].upper()
                if "smlTagName" in setting["spec"]["config"]:
                    self.smlTagNullName_value = setting["spec"]["config"]["smlTagName"]

    def gen_tcp_param(self):
        MaxBytes = 1024*1024
        host = self.taosadapter_fqdn
        port = 6046
        return MaxBytes, host, port

    def tcp_client(self, input):
        # MaxBytes = self.gen_tcp_param()[0]
        host = self.gen_tcp_param()[1]
        port = self.gen_tcp_param()[2]
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.send(input.encode())
        sock.close()

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

    def trans_time_to_s(self, runtime):
        if "d" in str(runtime).lower():
            d_num = re.findall(r"\d+\.?\d*", runtime.replace(" ", ""))[0]
            s_num = float(d_num) * 24 * 60 * 60
        elif "h" in str(runtime).lower():
            h_num = re.findall(r"\d+\.?\d*", runtime.replace(" ", ""))[0]
            s_num = float(h_num) * 60 * 60
        elif "m" in str(runtime).lower():
            m_num = re.findall(r"\d+\.?\d*", runtime.replace(" ", ""))[0]
            s_num = float(m_num) * 60
        elif "s" in str(runtime).lower():
            s_num = re.findall(r"\d+\.?\d*", runtime.replace(" ", ""))[0]
        else:
            s_num = 60
        return int(s_num)

    def gen_tag_col_str(self, gen_type, data_type, count):
        """
        gen multi tags or cols by gen_type
        """
        return ','.join(map(lambda i: f'{gen_type}{i} {data_type}', range(count)))

    def drop_all_db(self):
        self.tdSql.query("show databases;")
        db_list = list(map(lambda x: x[0], self.tdSql.query_data))
        for dbname in db_list:
            if dbname not in self.white_list and "telegraf" not in dbname:
                self.tdSql.execute(f'drop database if exists `{dbname}`')
    def drop_db(self, dbname="test"):
        if self.api_type == "taosc":
            if dbname[0].isdigit():
                self.tdSql.execute(f'drop database if exists `{dbname}`')
            else:
                self.tdSql.execute(f'drop database if exists {dbname}')
        elif self.api_type == "restful":
            if dbname[0].isdigit():
                self.tdRest.restApiPost(f"drop database if exists `{dbname}`")
            else:
                self.tdRest.restApiPost(f"drop database if exists {dbname}")
    def createDb(self, dbname="test", drop_db=True, **kwargs):
        db_params = ""
        replica_specified = False
        cachemodel_specified = False
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                if param == "precision":
                    db_params += f'{param} "{value}" '
                else:
                    db_params += f'{param} {value} '
                if param == "replica":
                    replica_specified = True
        if not replica_specified:
            if TDCom.taostest_database_replicas_variable in os.environ:
                db_params = db_params + ' replica ' + os.environ[TDCom.taostest_database_replicas_variable] + ' '
        if not cachemodel_specified:
            if TDCom.taostest_database_cachemodel_variable in os.environ:
                db_params = db_params + ' cachemodel ' + f'"{os.environ[TDCom.taostest_database_cachemodel_variable]}"' + ' '
        if self.api_type == "taosc":
            if drop_db:
                self.drop_db(dbname)
            self.tdSql.execute(f'create database if not exists {dbname} {db_params}')
            self.tdSql.execute(f'use {dbname}')
        elif self.api_type == "restful":
            if drop_db:
                self.drop_db(dbname)
            self.tdRest.restApiPost(f"create database if not exists {dbname} {db_params}")

    def get_db_list(self):
        self.tdSql.query('show databases')
        return list(filter(None, list(map(lambda x:x[0] if x[0] != "information_schema" and x[0] != "performance_schema" else None, self.tdSql.query_data))))

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

    def gen_tag_type_str(self, tag_elm_list):
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

    def gen_column_type_str(self, column_elm_list):
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
        else:
            pass

    def gen_tag_value_list(self, tag_elm_list, ts_value=None):
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

    # def trans_tag_value_list(self, tag_elm_list):
    #     if self.need_tagts:
    #         self.tag_value_list[0] = self.genTs(self.tag_value_list[0])[1]
    #     if tag_elm_list is None:
    #         for self.full_type_list


    def gen_column_value_list(self, column_elm_list, need_null, ts_value=None):
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

    def create_stable(self, dbname=None, stbname="stb", use_name="table", column_elm_list=None, tag_elm_list=None,
                     need_tagts=False, count=1, default_stbname_prefix="stb", default_stbname_index_start_num=1, 
                     default_column_index_start_num=1, default_tag_index_start_num=1, **kwargs):
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
        self.gen_column_type_str(column_elm_list)
        self.gen_tag_type_str(tag_elm_list)
        if self.dbname is not None:
            self.stb_name = f'{self.dbname}.{stbname}'
        else:
            self.stb_name = stbname
        if int(count) <= 1:
            create_stable_sql = f'create {use_name} {self.stb_name} ({self.column_type_str}) tags ({self.tag_type_str}) {stb_params};'
            self.tdSql.execute(create_stable_sql)
        else:
            for _ in range(count):
                create_stable_sql = f'create {use_name} {self.dbname}.{default_stbname_prefix}{default_stbname_index_start_num} ({self.column_type_str}) tags ({self.tag_type_str}) {stb_params};'
                default_stbname_index_start_num += 1
                self.tdSql.execute(create_stable_sql)

    def create_ctable(self, dbname=None, stbname=None, ctbname="ctb", use_name="table", tag_elm_list=None, ts_value=None, count=1, default_varchar_datatype="letters", default_nchar_datatype="letters", default_ctbname_prefix="ctb", default_ctbname_index_start_num=1, **kwargs):
        self.default_varchar_datatype = default_varchar_datatype
        self.default_nchar_datatype = default_nchar_datatype
        self.default_ctbname_prefix = default_ctbname_prefix
        self.default_ctbname_index_start_num = default_ctbname_index_start_num
        ctb_params = ""
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                ctb_params += f'{param} "{value}" '
        self.gen_tag_value_list(tag_elm_list, ts_value)
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
            self.ctb_name = f'{self.dbname}.{ctbname}'
        else:
            self.ctb_name = ctbname
        if stbname is not None:
            self.stb_name = stbname
            if dbname is not None:
                 self.stb_name = f'{self.dbname}.{stbname}'
        if int(count) <= 1:
            create_ctable_sql = f'create {use_name} {self.ctb_name} using {self.stb_name} tags ({tag_value_str}) {ctb_params};'
            self.tdSql.execute(create_ctable_sql)
        else:
            for _ in range(count):
                create_stable_sql = f'create {use_name} {self.dbname}.{default_ctbname_prefix}{default_ctbname_index_start_num} using {self.stb_name} tags ({tag_value_str}) {ctb_params};'
                default_ctbname_index_start_num += 1
                self.tdSql.execute(create_stable_sql)

    def create_table(self, dbname=None, tbname="tb", use_name="table", column_elm_list=None,
                    count=1, default_tbname_prefix="tb", default_tbname_index_start_num=1, 
                    default_column_index_start_num=1, **kwargs):
        if dbname is not None:
            self.dbname = dbname
        self.default_tbname_prefix = default_tbname_prefix
        self.default_tbname_index_start_num = default_tbname_index_start_num
        self.default_column_index_start_num = default_column_index_start_num
        tb_params = ""
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                tb_params += f'{param} "{value}" '
        self.gen_column_type_str(column_elm_list)
        if self.dbname is not None:
            self.tb_name = f'{self.dbname}.{tbname}'
        else:
            self.tb_name = tbname
        if int(count) <= 1:
            create_table_sql = f'create {use_name} {self.tb_name} ({self.column_type_str}) {tb_params};'
            self.tdSql.execute(create_table_sql)
        else:
            for _ in range(count):
                create_table_sql = f'create {use_name} {self.dbname}.{default_tbname_prefix}{default_tbname_index_start_num} ({self.column_type_str}) {tb_params};'
                default_tbname_index_start_num += 1
                self.tdSql.execute(create_table_sql)

    def insert_rows(self, dbname=None, tbname=None, column_ele_list=None, ts_value=None, count=1, need_null=False):
        if dbname is not None:
            self.dbname = dbname
            if tbname is not None:
                self.tb_name = f'{self.dbname}.{tbname}'
        else:
            if tbname is not None:
                self.tb_name = tbname

        self.gen_column_value_list(column_ele_list, need_null, ts_value)
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
            insert_sql = f'insert into {self.tb_name} values ({column_value_str});'
            self.tdSql.execute(insert_sql)
        else:
            for num in range(count):
                ts_value = self.genTs()[0]
                self.gen_column_value_list(column_ele_list, need_null, f'{ts_value}+{num}s')
                column_value_str = ""
                for column_value in self.column_value_list:
                    if column_value is None:
                        column_value_str += 'Null, '
                    elif isinstance(column_value, str) and "+" not in column_value:
                        column_value_str += f'"{column_value}", '
                    else:
                        column_value_str += f'{column_value}, '
                column_value_str = column_value_str.rstrip()[:-1] 
                insert_sql = f'insert into {self.tb_name} values ({column_value_str});'
                self.tdSql.execute(insert_sql)

    def delete_rows(self, dbname=None, tbname=None, start_ts=None, end_ts=None, ts_key=None):
        if dbname is not None:
            self.dbname = dbname
            if tbname is not None:
                self.tb_name = f'{self.dbname}.{tbname}'
        else:
            if tbname is not None:
                self.tb_name = tbname
        if ts_key is None:
            ts_col_name = self.default_colts_name
        else:
            ts_col_name = ts_key

        base_del_sql = f'delete from {self.tb_name} '
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
        self.tdSql.execute(base_del_sql)

    def create_stream(self, stream_name, des_table, source_sql, trigger_mode=None, watermark=None, max_delay=None, ignore_expired=None, ignore_update=None, subtable_value=None, fill_value=None, fill_history_value=None, stb_field_name_value=None, tag_value=None, use_exist_stb=False, use_except=False):
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
                self.tdSql.execute(f'create stream if not exists {stream_name} trigger at_once {stream_options} {fill_history} into {des_table} {subtable} as {source_sql} {fill};')
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
                self.tdSql.execute(f'create stream if not exists {stream_name} {stream_options} {fill_history} into {des_table}{stb_field_name} {tags} {subtable} as {source_sql} {fill};')
                return None
            else:
                return f'create stream if not exists {stream_name} {stream_options} {fill_history} into {des_table}{stb_field_name} {tags} {subtable} as {source_sql} {fill};'

    def pause_stream(self, stream_name, if_exist=True, if_not_exist=False):
        if_exist_value = "if exists" if if_exist else ""
        if_not_exist_value = "if not exists" if if_not_exist else ""
        self.tdSql.execute(f'pause stream {if_exist_value} {if_not_exist_value} {stream_name}')

    def resume_stream(self, stream_name, if_exist=True, if_not_exist=False, ignore_untreated=False):
        if_exist_value = "if exists" if if_exist else ""
        if_not_exist_value = "if not exists" if if_not_exist else ""
        ignore_untreated_value = "ignore untreated" if ignore_untreated else ""
        self.tdSql.execute(f'resume stream {if_exist_value} {if_not_exist_value} {ignore_untreated_value} {stream_name}')

    def drop_stream(self, stream_name):
        self.tdSql.execute(f'drop stream if exists {stream_name};')

    def drop_all_streams(self):
        self.tdSql.query("show streams")
        stream_name_list = list(map(lambda x: x[0], self.tdSql.query_data))
        for stream_name in stream_name_list:
            self.tdSql.execute(f'drop stream if exists {stream_name};')

    def get_vgroup_id_list(self, dbname):
        self.tdSql.query(f'show {dbname}.vgroups;')
        return list(map(lambda x:x[0], self.tdSql.query_data))


    def create_sma(self, sma_name, stb_name, function_value, interval_value=None, sliding_value=None, max_delay_value=None, watermark_value=None, sma_type="tsma"):
        if interval_value is None:
            interval = ""
        else:
            interval = f'interval({interval_value})'

        if sliding_value is None:
            sliding = ""
        else:
            sliding = f'sliding({sliding_value})'

        if max_delay_value is None:
            max_delay = ""
        else:
            max_delay = f'max_delay {max_delay_value}'

        if watermark_value is None:
            watermark = ""
        else:
            watermark = f'watermark {watermark_value}'
        options = f'{interval} {sliding} {max_delay} {watermark}'
        self.tdSql.execute(f'create sma index if not exists {sma_name} on {stb_name} function({function_value}) {options};')

    def drop_sma(self, sma_name):
        self.tdSql.execute(f'drop index if exists {sma_name};')

    def drop_all_smas(self):
        self.tdSql.query("show streams")
        sma_name_list = list(map(lambda x: x[0], self.tdSql.query_data))
        for sma_name in sma_name_list:
            self.tdSql.execute(f'drop index if exists {sma_name};')

    def create_udf(self, udf_name, lib_path, bufsize, outputtype, aggregate=False):
        if aggregate:
            self.tdSql.execute(f'create aggregate function {udf_name} as "{lib_path}" outputtype {outputtype} bufSize {bufsize};')
        else:
            self.tdSql.execute(f'create function {udf_name} as "{lib_path}" outputtype {outputtype};')

    def drop_udf(self, udf_name):
        self.tdSql.execute(f'drop function if exists {udf_name};')

    def drop_all_udfs(self):
        self.tdSql.query("show functions")
        udf_name_list = list(map(lambda x: x[0], self.tdSql.query_data))
        for udf_name in udf_name_list:
            self.tdSql.execute(f'drop function if exists {udf_name};')

    def write_latency(self, msg):
        with open(self.stream_latency_log, 'a') as f:
            f.write(f'{msg}\n')

    def check_stream_res(self, sql, expected_res, max_delay):
        self.tdSql.query(sql)
        latency = 0
        if self.tdSql.query_row == expected_res:
            self.write_latency(latency)

        while self.tdSql.query_row != expected_res:
            self.tdSql.query(sql, record=False)
            if latency < self.stream_timeout:
                latency += 0.2
                time.sleep(0.2)
            else:
                if max_delay is not None:
                    if latency == 0:
                        return False
                self.tdSql.checkEqual(self.tdSql.query_row, expected_res)
            if self.tdSql.query_row == expected_res:
                self.write_latency(latency)
                return latency

    def check_tsma_res(self, sql, expected_res, max_delay=None):
        self.tdSql.query(sql)
        latency = 0

        while self.tdSql.query_data != expected_res:
            self.tdSql.query(sql, record=False)
            if latency < self.stream_timeout:
                latency += 0.2
                time.sleep(0.2)
            else:
                if max_delay is not None:
                    if latency == 0:
                        return False
                self.tdSql.checkEqual(self.tdSql.query_data, expected_res)

    def round_handle(self, input_list):
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

    def cast_query_data(self, query_data):
        col_type_list = self.column_type_str.split(',')
        tag_type_list = self.tag_type_str.split(',')
        col_tag_type_list = col_type_list + tag_type_list
        nl = list()
        for query_data_t in query_data:
            query_data_l = list(query_data_t)
            for i,v in enumerate(query_data_l):
                if v is not None:
                    if " ".join(col_tag_type_list[i].strip().split(" ")[1:]) == "nchar(256)":
                        self.tdSql.query(f'select cast("{v}" as binary(256))')
                    else:
                        self.tdSql.query(f'select cast("{v}" as {" ".join(col_tag_type_list[i].strip().split(" ")[1:])})')
                    query_data_l[i] = self.tdSql.query_data[0][0]
                else:
                    query_data_l[i] = v
            nl.append(tuple(query_data_l))
        return nl

    def check_query_data(self, sql1, sql2, sorted=False, fill_value=None, tag_value_list=None, defined_tag_count=None, partition=True, use_exist_stb=False, subtable=None, reverse_check=False):
        if tag_value_list:
            dvalue = len(self.tag_type_str.split(',')) - defined_tag_count
        self.tdSql.query(sql1)
        res1 = self.tdSql.query_data
        self.tdSql.query(sql2)
        res2 = self.cast_query_data(self.tdSql.query_data) if tag_value_list or use_exist_stb else self.tdSql.query_data
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
                new_list = list()
                self.tdSql.query(sql1, record=False)
                res1 = self.tdSql.query_data
                self.tdSql.query(sql2, record=False)
                # res2 = self.tdSql.query_data
                res2 = self.cast_query_data(self.tdSql.query_data) if tag_value_list or use_exist_stb else self.tdSql.query_data

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
                    self.tdSql.checkEqual(res1, res2)
                    # self.tdSql.checkEqual(res1, res2) if not reverse_check else self.tdSql.checkNotEqual(res1, res2)
        else:
            while res1 == res2:
                new_list = list()
                self.tdSql.query(sql1, record=False)
                res1 = self.tdSql.query_data
                self.tdSql.query(sql2, record=False)
                # res2 = self.tdSql.query_data
                res2 = self.cast_query_data(self.tdSql.query_data) if tag_value_list or use_exist_stb else self.tdSql.query_data

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
                    self.tdSql.checkNotEqual(res1, res2)
                    # self.tdSql.checkEqual(res1, res2) if not reverse_check else self.tdSql.checkNotEqual(res1, res2)

    def check_stream_field_type(self, sql, input_function):
        self.tdSql.query(sql)
        res = self.tdSql.query_data
        if input_function in ["acos", "asin", "atan", "cos", "log", "pow", "sin", "sqrt", "tan"]:
            self.tdSql.checkEqual(res[1][1], "DOUBLE")
            self.tdSql.checkEqual(res[2][1], "DOUBLE")
        elif input_function in ["lower", "ltrim", "rtrim", "upper"]:
            self.tdSql.checkEqual(res[1][1], "VARCHAR")
            self.tdSql.checkEqual(res[2][1], "VARCHAR")
            self.tdSql.checkEqual(res[3][1], "NCHAR")
        elif input_function in ["char_length", "length"]:
            self.tdSql.checkEqual(res[1][1], "BIGINT")
            self.tdSql.checkEqual(res[2][1], "BIGINT")
            self.tdSql.checkEqual(res[3][1], "BIGINT")
        elif input_function in ["concat", "concat_ws"]:
            self.tdSql.checkEqual(self.tdSql.query_data[1][1], "VARCHAR")
            self.tdSql.checkEqual(self.tdSql.query_data[2][1], "NCHAR")
            self.tdSql.checkEqual(self.tdSql.query_data[3][1], "NCHAR")
            self.tdSql.checkEqual(self.tdSql.query_data[4][1], "NCHAR")
        elif input_function in ["substr"]:
            self.tdSql.checkEqual(res[1][1], "VARCHAR")
            self.tdSql.checkEqual(res[2][1], "VARCHAR")
            self.tdSql.checkEqual(res[3][1], "VARCHAR")
            self.tdSql.checkEqual(res[4][1], "NCHAR")
        else:
            self.tdSql.checkEqual(res[1][1], "INT")
            self.tdSql.checkEqual(res[2][1], "DOUBLE")

    def check_stream(self, sql1, sql2, expected_count, max_delay=None):
        self.write_latency(f'sql: {sql1}')
        self.check_stream_res(sql1, expected_count, max_delay)
        self.check_query_data(sql1, sql2)

    def gen_symbol_list(self):
        """
        define symbol list
        """
        return [' ', '~', '`', '!', '@', '#', '$', '¥', '%', '^', '&', '*', '(', ')',
                '-', '+', '=', '{', '「', '[', ']', '}', '」', '、', '|', '\\', ':',
                ';', '\'', '\"', ',', '<', '《', '.', '>', '》', '/', '?']

    def gen_ts_support_unit_list(self):
        """
        define support ts unit list
        """
        return ["b", "u", "a", "s", "m", "h", "d", "w"]

    def schemalessPost(self, sql, url_type="influxdb", dbname="test", precision=None):
        """
        schemaless post
        """
        if url_type == "influxdb":
            url = self.genUrl(url_type, dbname, precision)
        elif url_type == "telnet":
            url = self.genUrl(url_type, dbname, precision)
        res = requests.post(url, sql.encode("utf-8"), headers=self.preDefine()[0])
        return res

    def cleanTb(self, connect_type="taosc", dbname=None):
        """
        connect_type is taosc or restful
        """
        if connect_type == "taosc":
            for query_sql in ['show stables;', 'show tables;']:
                self.tdSql.query(query_sql)
                stb_list = map(lambda x: x[0], self.tdSql.query_data)
                for stb in stb_list:
                    self.tdSql.execute(f'drop table if exists `{stb}`;')
                    
        elif connect_type == "restful":
            for query_sql in [f'show {dbname}.stables;', f'show {dbname}.tables;']:
                self.tdRest.request(query_sql)
                stb_list = map(lambda x: x[0], self.tdRest.resp["data"])
                for stb in stb_list:
                    self.tdRest.request(f"drop table if exists {dbname}.`{stb}`;")
                    

    def date_to_ts(self, datetime_input):
        return int(time.mktime(time.strptime(datetime_input, "%Y-%m-%d %H:%M:%S.%f")))

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

    def typeof(self, variate):
        return type(variate).__name__

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

    def str_trans(self, words):
        temp_word = words
        for i in range(2 ** len(words)):
            int_bin = str(bin(i)).replace("0b", "")
            for i in range(len(words) - len(int_bin)):
                int_bin = "".join(("0", int_bin))
            for index, i in enumerate(int_bin):
                if i == '1':
                    words_lst = list(words)
                    words_lst[index] = words_lst[index].upper()
                    words = "".join(words_lst)
            yield words
            words = temp_word

    def time_cast(self, time_value, split_symbol="+"):
        ts_value = str(time_value).split(split_symbol)[0]
        if split_symbol in str(time_value):
            ts_value_offset = str(time_value).split(split_symbol)[1]
        else:
            ts_value_offset = "0s"
        return f'cast({ts_value} as timestamp){split_symbol}{ts_value_offset}'

    def str_ts_trans_bigint(self, str_ts):
        self.tdSql.query(f'select cast({str_ts} as bigint)')
        return self.tdSql.query_data[0][0]

    # schemaless
    def time_trans(self, time_value, ts_type=None):
        if "restful" in self.sml_type:
            self.utc = True
        else:
            self.utc = None
        # TDSmlTimestampType.HOUR.value, TDSmlTimestampType.MINUTE.value, TDSmlTimestampType.SECOND.value, TDSmlTimestampType.MICRO_SECOND.value, TDSmlTimestampType.NANO_SECOND.value
        if self.sml_type == "opentsdb_json" or self.sml_type == "opentsdb_json_restful":
            if type(time_value) is int:
                if time_value != 0:
                    if len(str(time_value)) == 13:
                        time_value = str(int(time_value) - 8 * 60 * 60 * 1000) if self.utc else time_value
                        ts = int(time_value)/1000
                    elif len(str(time_value)) == 10:
                        time_value = str(int(time_value) - 8 * 60 * 60) if self.utc else time_value
                        ts = int(time_value)/1
                    else:
                        time_value = str(int(time_value) - 8 * 60 * 60 * 1000000) if self.utc else time_value
                        ts = time_value/1000000
                else:
                    ts = time.time()
                    if self.utc is not None:
                        ts -= 8 * 60 * 60
            elif type(time_value) is dict:
                t_value = deepcopy(time_value)
                if time_value["type"].lower() == "ns":
                    t_value["value"] = int(t_value["value"]) - 8 * 60 * 60 * 1000000000 if self.utc else t_value["value"]
                    ts = t_value["value"]/1000000000
                elif time_value["type"].lower() == "us":
                    t_value["value"] = int(t_value["value"]) - 8 * 60 * 60 * 1000000 if self.utc else t_value["value"]
                    ts = t_value["value"]/1000000
                elif time_value["type"].lower() == "ms":
                    t_value["value"] = int(t_value["value"]) - 8 * 60 * 60 * 1000 if self.utc else t_value["value"]
                    ts = t_value["value"]/1000
                elif time_value["type"].lower() == "s":
                    t_value["value"] = int(t_value["value"]) - 8 * 60 * 60 if self.utc else t_value["value"]
                    ts = t_value["value"]/1
                else:
                    t_value["value"] = int(t_value["value"]) - 8 * 60 * 60 * 1000000 if self.utc else t_value["value"]
                    ts = t_value["value"]/1000000
            else:
                pass
        elif self.sml_type == "influxdb_restful":
            if int(time_value) == 0:
                ts = time.time()
                if self.utc is not None:
                    ts -= 8 * 60 * 60
            else:
                if ts_type == "ns" or ts_type is None:
                    time_value = str(int(time_value) - 8 * 60 * 60 * 1000000000) if self.utc else time_value
                    ts = int(''.join(list(filter(str.isdigit, time_value))))/1000000000
                elif ts_type == "u":
                    time_value = str(int(time_value) - 8 * 60 * 60 * 1000000) if self.utc else time_value
                    ts = int(''.join(list(filter(str.isdigit, time_value))))/1000000
                elif ts_type == "ms":
                    time_value = str(int(time_value) - 8 * 60 * 60 * 1000) if self.utc else time_value
                    ts = int(''.join(list(filter(str.isdigit, time_value))))/1000
                elif ts_type == "s":
                    time_value = str(int(time_value) - 8 * 60 * 60) if self.utc else time_value
                    ts = int(''.join(list(filter(str.isdigit, time_value))))/1
        else:
            if int(time_value) == 0:
                ts = time.time()
                if self.utc is not None:
                    ts -= 8 * 60 * 60
            else:
                if (ts_type == TDSmlTimestampType.NANO_SECOND.value or ts_type is None) and self.sml_type == "influxdb":
                    time_value = str(int(time_value) - 8 * 60 * 60 * 1000000000) if self.utc else time_value
                    ts = int(''.join(list(filter(str.isdigit, time_value)))) / 1000000000
                elif ts_type == TDSmlTimestampType.MICRO_SECOND.value:

                    time_value = str(int(time_value) - 8 * 60 * 60 * 1000000) if self.utc else time_value
                    ts = int(''.join(list(filter(str.isdigit, time_value)))) / 1000000
                elif (ts_type == TDSmlTimestampType.MILLI_SECOND.value or ts_type is None) and (self.sml_type == "opentsdb_telnet" or self.sml_type == "telnet-tcp" or self.sml_type == "opentsdb_telnet_restful"  or self.sml_type == "influxdb"):
                    time_value = str(int(time_value) - 8 * 60 * 60 * 1000) if self.utc else time_value
                    ts = int(''.join(list(filter(str.isdigit, time_value)))) / 1000
                elif ts_type == TDSmlTimestampType.SECOND.value:
                    time_value = str(int(time_value) - 8 * 60 * 60) if self.utc else time_value
                    ts = int(''.join(list(filter(str.isdigit, time_value)))) / 1
        ulsec = repr(ts).split('.')[1][:6]
        if len(ulsec) < 6 and int(ulsec) != 0:
            ulsec = int(ulsec) * (10 ** (6 - len(ulsec)))
        elif int(ulsec) == 0:
            ulsec *= 6
            # * follow two rows added for tsCheckCase
            if self.sml_type == "opentsdb_json_restful":
                td_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) + ".000000"
            else:
                td_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
            return td_ts
        #td_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
        td_ts = time.strftime("%Y-%m-%d %H:%M:%S.{}".format(ulsec), time.localtime(ts))
        return td_ts
        #return repr(datetime.datetime.strptime(td_ts, "%Y-%m-%d %H:%M:%S.%f"))

    def date_to_ts(self, datetime_input):
        return int(time.mktime(time.strptime(datetime_input, "%Y-%m-%d %H:%M:%S.%f")))

    def get_type_value(self, value, vtype="col"):
        """
        vtype must be col or tag
        """
        if vtype == "col":
            if value.lower().endswith("i8"):
                td_type = "TINYINT"
                td_tag_value = ''.join(list(value)[:-2])
            elif value.lower().endswith("i16"):
                td_type = "SMALLINT"
                td_tag_value = ''.join(list(value)[:-3])
            elif value.lower().endswith("i32"):
                td_type = "INT"
                td_tag_value = ''.join(list(value)[:-3])
            elif value.lower().endswith("i64"):
                td_type = "BIGINT"
                td_tag_value = ''.join(list(value)[:-3])
            elif value.lower().lower().endswith("u64"):
                td_type = "BIGINT UNSIGNED"
                td_tag_value = ''.join(list(value)[:-3])
            elif value.lower().endswith("f32"):
                td_type = "FLOAT"
                td_tag_value = ''.join(list(value)[:-3])
                td_tag_value = '{}'.format(np.float32(td_tag_value))
            elif value.lower().endswith("f64"):
                td_type = "DOUBLE"
                td_tag_value = ''.join(list(value)[:-3])
                if "e" in value.lower():
                    td_tag_value = str(float(td_tag_value))
            elif value.lower().startswith('l"'):
                td_type = "NCHAR"
                td_tag_value = ''.join(list(value)[2:-1])
            elif value.startswith('"') and value.endswith('"'):
                td_type = "VARCHAR"
                td_tag_value = ''.join(list(value)[1:-1])
            elif value.lower() == "t" or value.lower() == "true":
                td_type = "BOOL"
                td_tag_value = "True"
            elif value.lower() == "f" or value.lower() == "false":
                td_type = "BOOL"
                td_tag_value = "False"
            elif value.isdigit():
                td_type = "DOUBLE"
                td_tag_value = str(float(value))
            else:
                td_type = "DOUBLE"
                if "e" in value.lower():
                    td_tag_value = str(float(value))
                else:
                    td_tag_value = value
        elif vtype == "tag":
            td_type = "NCHAR"
            td_tag_value = str(value)
        return td_type, td_tag_value

    def type_trans(self, type_list):
        if self.sml_type == "opentsdb_json":
            type_list = list(map(lambda i: i["type"] if type(i) is dict else i, type_list))
        return list(map(lambda i: i.upper(), type_list))

    def influxdb_input_handle(self, input_sql, ts_type):
        input_sql_split_list = input_sql.split(" ")

        stb_tag_list = input_sql_split_list[0].split(',')
        stb_col_list = input_sql_split_list[1].split(',')
        time_value = self.time_trans(input_sql_split_list[2], ts_type)

        stb_name = stb_tag_list[0]
        stb_tag_list.pop(0)

        tag_name_list = []
        tag_value_list = []
        td_tag_value_list = []
        td_tag_type_list = []

        col_name_list = []
        col_value_list = []
        td_col_value_list = []
        td_col_type_list = []
        for elm in stb_tag_list:
            if self.smlChildTableName_value == "ID":
                if "id=" in elm.lower():
                    tb_name = elm.split('=')[1]
                else:
                    tag_name_list.append(elm.split("=")[0].lower())
                    tag_value_list.append(elm.split("=")[1])
                    tb_name = ""
                    td_tag_value_list.append(self.get_type_value(elm.split("=")[1], "tag")[1])
                    td_tag_type_list.append(self.get_type_value(elm.split("=")[1], "tag")[0])
            else:
                if "id=" in elm.lower():
                    tb_name = elm.split('=')[1]
                    tag_name_list.append(elm.split("=")[0])
                    td_tag_value_list.append(tb_name)
                    td_tag_type_list.append("NCHAR")
                else:
                    tag_name_list.append(elm.split("=")[0])
                    tag_value_list.append(elm.split("=")[1])
                    tb_name = ""
                    td_tag_value_list.append(self.get_type_value(elm.split("=")[1], "tag")[1])
                    td_tag_type_list.append(self.get_type_value(elm.split("=")[1], "tag")[0])

        for elm in stb_col_list:
            col_name_list.append(elm.split("=")[0])
            col_value_list.append(elm.split("=")[1])
            td_col_value_list.append(self.get_type_value(elm.split("=")[1])[1])
            td_col_type_list.append(self.get_type_value(elm.split("=")[1])[0])

        final_field_list = []
        final_field_list.extend(col_name_list)
        final_field_list.extend(tag_name_list)

        final_type_list = []
        final_type_list.append("TIMESTAMP")
        final_type_list.extend(td_col_type_list)
        final_type_list.extend(td_tag_type_list)
        final_type_list = self.type_trans(final_type_list)

        final_value_list = []
        final_value_list.append(time_value)
        final_value_list.extend(td_col_value_list)
        final_value_list.extend(td_tag_value_list)
        return final_value_list, final_field_list, final_type_list, stb_name, tb_name

    def opentsdb_telnet_input_handle(self, input_sql, ts_type):
        input_sql_split_list = input_sql.split(" ")
        if self.sml_type == "telnet-tcp":
            input_sql_split_list.pop(0)
        stb_name = input_sql_split_list[0]
        stb_tag_list = input_sql_split_list[3:]
        stb_tag_list[-1] = stb_tag_list[-1].strip()
        stb_col_value = input_sql_split_list[2]
        if self.sml_type == "telnet-tcp":
            ts_value = self.time_trans(input_sql_split_list[1], ts_type)
        else:
            ts_value = self.time_trans(input_sql_split_list[1], ts_type)
            if "restful" in self.sml_type:
                # while ts_value.endswith("0"):
                #     ts_value = ts_value[:-1]
                #     if ts_value.endswith("."):
                #         ts_value = ts_value[:-1]
                ts_value += "Z"
                ts_value = ts_value.replace(" ", "T")

        tag_name_list = []
        tag_value_list = []
        td_tag_value_list = []
        td_tag_type_list = []

        col_name_list = []
        col_value_list = []
        td_col_value_list = []
        td_col_type_list = []
        self.set_sml_specified_value()

        for elm in stb_tag_list:
            if self.smlChildTableName_value == "ID":
                if "id=" in elm.lower():
                    tb_name = elm.split('=')[1]
                else:
                    tag_name_list.append(elm.split("=")[0].lower())
                    tag_value_list.append(elm.split("=")[1])
                    tb_name = ""
                    td_tag_value_list.append(self.get_type_value(elm.split("=")[1], "tag")[1])
                    td_tag_type_list.append(self.get_type_value(elm.split("=")[1], "tag")[0])
            else:
                if "id" == elm.split("=")[0].lower():
                    tag_name_list.insert(0, elm.split("=")[0])
                    tag_value_list.insert(0, elm.split("=")[1])
                    td_tag_value_list.insert(0, self.get_type_value(elm.split("=")[1], "tag")[1])
                    td_tag_type_list.insert(0, self.get_type_value(elm.split("=")[1], "tag")[0])
                else:
                    tag_name_list.append(elm.split("=")[0])
                    tag_value_list.append(elm.split("=")[1])
                    tb_name = ""
                    td_tag_value_list.append(self.get_type_value(elm.split("=")[1], "tag")[1])
                    td_tag_type_list.append(self.get_type_value(elm.split("=")[1], "tag")[0])

        col_name_list.append('_value')
        col_value_list.append(stb_col_value)

        td_col_value_list.append(self.get_type_value(stb_col_value)[1])
        td_col_type_list.append(self.get_type_value(stb_col_value)[0])

        final_field_list = []
        final_field_list.extend(col_name_list)
        final_field_list.extend(tag_name_list)

        final_type_list = []
        final_type_list.append("TIMESTAMP")
        final_type_list.extend(td_col_type_list)
        final_type_list.extend(td_tag_type_list)
        final_type_list = self.type_trans(final_type_list)

        final_value_list = []
        final_value_list.append(ts_value)
        final_value_list.extend(td_col_value_list)
        final_value_list.extend(td_tag_value_list)
        return final_value_list, final_field_list, final_type_list, stb_name, tb_name

    def opentsdb_json_input_handle(self, input_json):
        stb_name = input_json["metric"]
        stb_tag_dict = input_json["tags"]
        stb_col_dict = input_json["value"]
        ts_value = self.time_trans(input_json["timestamp"])
        if self.utc is not None:
            # while ts_value.endswith("0"):
            #     ts_value = ts_value[:-1]
            #     if ts_value.endswith("."):
            #         ts_value = ts_value[:-1]
            ts_value += "Z"
            ts_value = ts_value.replace(" ", "T")
        tag_name_list = []
        tag_value_list = []
        td_tag_value_list = []
        td_tag_type_list = []

        col_name_list = []
        col_value_list = []
        td_col_value_list = []
        td_col_type_list = []

        # handle tag
        for key,value in stb_tag_dict.items():
            if "id" == key.lower():
                tb_name = value
            else:
                if type(value) is dict:
                    tag_value_list.append(str(value["value"]))
                    td_tag_value_list.append(str(value["value"]))
                    tag_name_list.append(key.lower())
                    if value["type"].lower() == "binary":
                        td_tag_type_list.append("VARCHAR")
                    else:
                        td_tag_type_list.append(value["type"].upper())
                    tb_name = ""
                else:
                    tag_value_list.append(str(value))
                    # td_tag_value_list.append(str(value))
                    tag_name_list.append(key.lower())
                    tb_name = ""

                    if type(value) is bool:
                        td_tag_type_list.append("BOOL")
                        td_tag_value_list.append(str(value))
                    elif type(value) is int:
                        td_tag_type_list.append("DOUBLE")
                        if self.sml_type == "opentsdb_json_restful":
                        # td_tag_type_list.append("BIGINT")
                            td_tag_value_list.append(str(value))
                        else:
                            td_tag_value_list.append(str(float(value)))
                    elif type(value) is float:
                        td_tag_type_list.append("DOUBLE")
                        td_tag_value_list.append(str(float(value)))
                    elif type(value) is str:
                        td_tag_type_list.append("VARCHAR")
                        td_tag_value_list.append(str(value))
                        # if self.defaultJSONStrType_value == "NCHAR":
                        #     td_tag_type_list.append("NCHAR")
                        #     td_tag_value_list.append(str(value))
                        # else:
                        #     td_tag_type_list.append("VARCHAR")
                        #     td_tag_value_list.append(str(value))

        # handle col
        if type(stb_col_dict) is dict:
            if stb_col_dict["type"].lower() == "bool":
                bool_value = f'{stb_col_dict["value"]}'
                col_value_list.append(bool_value)
                td_col_type_list.append(stb_col_dict["type"].upper())
                col_name_list.append("_value")
                td_col_value_list.append(str(stb_col_dict["value"]))
            else:
                col_value_list.append(stb_col_dict["value"])
                if stb_col_dict["type"].lower() == "binary":
                    td_col_type_list.append("VARCHAR")
                else:
                    td_col_type_list.append(stb_col_dict["type"].upper())
                col_name_list.append("_value")
                td_col_value_list.append(str(stb_col_dict["value"]))
        else:
            col_name_list.append("_value")
            col_value_list.append(str(stb_col_dict))
            # td_col_value_list.append(str(stb_col_dict))
            if type(stb_col_dict) is bool:
                td_col_type_list.append("BOOL")
                td_col_value_list.append(str(stb_col_dict))
            elif type(stb_col_dict) is int:
                td_col_type_list.append("DOUBLE")
                td_col_value_list.append(str(float(stb_col_dict)))
            elif type(stb_col_dict) is float:
                td_col_type_list.append("DOUBLE")
                td_col_value_list.append(str(float(stb_col_dict)))
            elif type(stb_col_dict) is str:
                td_col_type_list.append("VARCHAR")
                td_col_value_list.append(str(stb_col_dict))
                # if self.defaultJSONStrType_value == "NCHAR":
                #     td_col_type_list.append("NCHAR")
                #     td_col_value_list.append(str(stb_col_dict))
                # else:
                #     td_col_type_list.append("VARCHAR")
                #     td_col_value_list.append(str(stb_col_dict))

        final_field_list = []
        final_field_list.extend(col_name_list)
        final_field_list.extend(tag_name_list)

        final_type_list = []
        final_type_list.append("TIMESTAMP")
        final_type_list.extend(td_col_type_list)
        final_type_list.extend(td_tag_type_list)
        final_type_list = self.type_trans(final_type_list)

        final_value_list = []
        final_value_list.append(ts_value)
        final_value_list.extend(td_col_value_list)
        final_value_list.extend(td_tag_value_list)
        return final_value_list, final_field_list, final_type_list, stb_name, tb_name

    def influxdb_restful_input_handle(self, input_sql, ts_type, t_blank_tag=None):
        tb_name = ""
        input_sql_split_list = input_sql.split(" ")

        stb_tag_list = input_sql_split_list[0].split(',')
        stb_col_list = input_sql_split_list[1].split(',')
        ts_value = self.time_trans(input_sql_split_list[2], ts_type)

        # while ts_value.endswith("0"):
        #     ts_value = ts_value[:-1]
        #     if ts_value.endswith("."):
        #         ts_value = ts_value[:-1]
        ts_value += "Z"
        ts_value = ts_value.replace(" ", "T")

        stb_name = stb_tag_list[0]
        stb_tag_list.pop(0)

        tag_name_list = []
        tag_value_list = []
        td_tag_value_list = []
        td_tag_type_list = []

        col_name_list = []
        col_value_list = []
        td_col_value_list = []
        td_col_type_list = []

        for elm in stb_tag_list:
            if self.smlChildTableName_value == "ID":
                if "id=" in elm.lower():
                    tb_name = elm.split('=')[1]
                else:
                    tag_name_list.append(elm.split("=")[0])
                    tag_value_list.append(elm.split("=")[1])
                    td_tag_value_list.append(self.get_type_value(elm.split("=")[1], "tag")[1])
                    td_tag_type_list.append(self.get_type_value(elm.split("=")[1], "tag")[0])
            else:
                if "id" == elm.split("=")[0].lower():
                    tag_name_list.insert(0, elm.split("=")[0])
                    tag_value_list.insert(0, elm.split("=")[1])
                    td_tag_value_list.insert(0, self.get_type_value(elm.split("=")[1], "tag")[1])
                    td_tag_type_list.insert(0, self.get_type_value(elm.split("=")[1], "tag")[0])
                else:
                    tag_name_list.append(elm.split("=")[0])
                    tag_value_list.append(elm.split("=")[1])
                    td_tag_value_list.append(self.get_type_value(elm.split("=")[1], "tag")[1])
                    td_tag_type_list.append(self.get_type_value(elm.split("=")[1], "tag")[0])

        for elm in stb_col_list:
            col_name_list.append(elm.split("=")[0])
            col_value_list.append(elm.split("=")[1])
            td_col_value_list.append(self.get_type_value(elm.split("=")[1])[1])
            td_col_type_list.append(self.get_type_value(elm.split("=")[1])[0])

        final_field_list = []
        final_field_list.extend(col_name_list)
        final_field_list.extend(tag_name_list)

        final_type_list = []
        final_type_list.append("TIMESTAMP")
        final_type_list.extend(td_col_type_list)
        final_type_list.extend(td_tag_type_list)
        final_type_list = self.type_trans(final_type_list)

        final_value_list = []
        final_value_list.append(ts_value)
        final_value_list.extend(td_col_value_list)
        final_value_list.extend(td_tag_value_list)
        if t_blank_tag is not None:
            final_field_list.append(self.smlTagNullName_value)
            final_value_list.append('None')
            final_type_list.append(10)
        return final_value_list, final_field_list, final_type_list, stb_name, tb_name

    def gen_ts_col_value(self, value, t_type=None, value_type="obj"):
        if value_type == "obj":
            if t_type == None:
                ts_col_value = value
            else:
                ts_col_value = {"value": value, "type":	t_type}
        elif value_type == "default":
            ts_col_value = value
        return ts_col_value

    def gen_tag_value(self, t0_type="bool", t0_value="", t1_type="tinyint", t1_value=127, t2_type="smallint", t2_value=32767,
                    t3_type="int", t3_value=2147483647, t4_type="bigint", t4_value=9223372036854775807,
                    t5_type="float", t5_value=11.12345027923584, t6_type="double", t6_value=22.123456789,
                    t7_type="binary", t7_value="binaryTagValue", t8_type="nchar", t8_value="ncharTagValue", value_type="obj"):
        if t0_value == "":
            t0_value = True
        if value_type == "obj":
            tag_value = {
                "t0": {"value": t0_value, "type": t0_type},
                "t1": {"value": t1_value, "type": t1_type},
                "t2": {"value": t2_value, "type": t2_type},
                "t3": {"value": t3_value, "type": t3_type},
                "t4": {"value": t4_value, "type": t4_type},
                "t5": {"value": t5_value, "type": t5_type},
                "t6": {"value": t6_value, "type": t6_type},
                "t7": {"value": t7_value, "type": t7_type},
                "t8": {"value": t8_value, "type": t8_type}
            }
        elif value_type == "default":
            # t5_value = t6_value
            tag_value = {
                "t0": t0_value,
                "t1": t1_value,
                "t2": t2_value,
                "t3": t3_value,
                "t4": t4_value,
                "t5": t5_value,
                "t6": t6_value,
                "t7": t7_value,
                "t8": t8_value
            }
        return tag_value

    def gen_full_type_json(self, ts_value="", col_value="", tag_value="", stb_name="", tb_name="",
                        id_noexist_tag=None, id_change_tag=None, id_upper_tag=None, id_mixul_tag=None, id_double_tag=None,
                        t_add_tag=None, t_mul_tag=None, c_multi_tag=None, c_blank_tag=None, t_blank_tag=None,
                        chinese_tag=None, multi_field_tag=None, point_trans_tag=None, value_type="obj"):
        if value_type == "obj":
            if stb_name == "":
                stb_name = self.get_long_name()
            if tb_name == "":
                tb_name = f'{stb_name}_{random.randint(0, 65535)}_{random.randint(0, 65535)}'
            if ts_value == "":
                ts_value = self.gen_ts_col_value(1626006833639000000, "ns")
            if col_value == "":
                col_value = self.gen_ts_col_value(random.choice([True, False]), "bool")
            if tag_value == "":
                tag_value = self.gen_tag_value()
            # if id_upper_tag is not None:
            #     id = "ID"
            # else:
            #     id = "id"
            # if id_mixul_tag is not None:
            #     id = random.choice(["iD", "Id"])
            # else:
            #     id = "id"
            # if id_noexist_tag is None:
            #     tag_value[id] = tb_name
            sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if id_noexist_tag is not None:
                if t_add_tag is not None:
                    tag_value["t9"] = {"value": "ncharTagValue", "type": "nchar"}
                    sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if id_change_tag is not None:
                tag_value.pop('t8')
                tag_value["t8"] = {"value": "ncharTagValue", "type": "nchar"}
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if id_double_tag is not None:
                tag_value["ID"] = f'"{tb_name}_2"'
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if t_add_tag is not None:
                tag_value["t10"] = {"value": "ncharTagValue", "type": "nchar"}
                tag_value["t11"] = {"value": True, "type": "bool"}
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if t_mul_tag is not None:
                tag_value.pop('t8')
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if c_multi_tag is not None:
                col_value = [{"value": True, "type": "bool"}, {"value": False, "type": "bool"}]
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if t_blank_tag is not None:
                tag_value = ""
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if chinese_tag is not None:
                tag_value = {"t0": {"value": "涛思数据", "type": "nchar"}}
                col_value = {"value": "涛思数据", "type": "nchar"}
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if c_blank_tag is not None:
                sql_json.pop("value")
            if multi_field_tag is not None:
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value, "tags2": tag_value}
            if point_trans_tag is not None:
                sql_json = {"metric": ".point.trans.test", "timestamp": ts_value, "value": col_value, "tags": tag_value}

        elif value_type == "default":
            if stb_name == "":
                stb_name = self.get_long_name()
            if tb_name == "":
                tb_name = f'{stb_name}_{random.randint(0, 65535)}_{random.randint(0, 65535)}'
            if ts_value == "":
                ts_value = 1626006834
            if col_value == "":
                col_value = random.choice([True, False])
            if tag_value == "":
                tag_value = self.gen_tag_value(value_type=value_type)
            # if id_upper_tag is not None:
            #     id = "ID"
            # else:
            #     id = "id"
            # if id_mixul_tag is not None:
            #     id = "iD"
            # else:
            #     id = "id"
            # if id_noexist_tag is None:
            #     tag_value[id] = tb_name
            sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if id_noexist_tag is not None:
                if t_add_tag is not None:
                    tag_value["t9"] = {"value": "ncharTagValue", "type": "nchar"}
                    sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if id_change_tag is not None:
                tag_value.pop('t7')
                tag_value["t7"] = {"value": "ncharTagValue", "type": "nchar"}
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if id_double_tag is not None:
                tag_value["ID"] = f'"{tb_name}_2"'
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if t_add_tag is not None:
                tag_value["t10"] = {"value": "ncharTagValue", "type": "nchar"}
                tag_value["t11"] = True
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if t_mul_tag is not None:
                tag_value.pop('t7')
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if c_multi_tag is not None:
                col_value = True,False
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if t_blank_tag is not None:
                tag_value = ""
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if c_blank_tag is not None:
                sql_json.pop("value")
            if multi_field_tag is not None:
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value, "tags2": tag_value}
            if point_trans_tag is not None:
                sql_json = {"metric": ".point.trans.test", "timestamp": ts_value, "value": col_value, "tags": tag_value}
        return sql_json, stb_name

    def gen_mul_tag_col_dict(self, gen_type, count=1, value_type="obj"):
        """
        gen_type must be tag/col
        """
        tag_dict = dict()
        col_dict = dict()
        if value_type == "obj":
            if gen_type == "tag":
                for i in range(0, count):
                    tag_dict[f't{i}'] = {'value': True, 'type': 'bool'}
                return tag_dict
            if gen_type == "col":
                col_dict = {'value': True, 'type': 'bool'}
                return col_dict
        elif value_type == "default":
            if gen_type == "tag":
                for i in range(0, count):
                    tag_dict[f't{i}'] = True
                return tag_dict
            if gen_type == "col":
                col_dict = True
                return col_dict

    def gen_long_json(self, tag_count, value_type="obj"):
        stb_name = self.get_long_name()
        # tb_name = f'{stb_name}_1'
        tag_dict = self.gen_mul_tag_col_dict("tag", tag_count, value_type)
        col_dict = self.gen_mul_tag_col_dict("col", 1, value_type)
        # tag_dict["id"] = tb_name
        ts_dict = {'value': 1626006833639000000, 'type': 'ns'}
        long_json = {"metric": stb_name, "timestamp": ts_dict, "value": col_dict, "tags": tag_dict}
        return long_json, stb_name

    def gen_influxdb_line(self, stb_name, tb_name, id, t0, t1, t2, t3, t4, t5, t6, t7, t8, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9,
            ts, id_noexist_tag, id_change_tag, id_double_tag, ct_add_tag, ct_am_tag, ct_ma_tag, ct_min_tag, c_multi_tag, t_multi_tag, c_blank_tag, t_blank_tag, chinese_tag):
        input_sql = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if id_noexist_tag is not None:
            input_sql = f'{stb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
            if ct_add_tag is not None:
                input_sql = f'{stb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8},t9={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if id_change_tag is not None:
            input_sql = f'{stb_name},t0={t0},t1={t1},{id}={tb_name},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if id_double_tag is not None:
            input_sql = f'{stb_name},{id}=\"{tb_name}_1\",t0={t0},t1={t1},{id}=\"{tb_name}_2\",t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if ct_add_tag is not None:
            input_sql = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8},t11={t1},t10={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9},c11={c8},c10={t0} {ts}'
        if ct_am_tag is not None:
            input_sql = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9},c11={c8},c10={t0} {ts}'
            if id_noexist_tag is not None:
                    input_sql = f'{stb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9},c11={c8},c10={t0} {ts}'
        if ct_ma_tag is not None:
            input_sql = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8},t11={t1},t10={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6} {ts}'
            if id_noexist_tag is not None:
                input_sql = f'{stb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8},t11={t1},t10={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6} {ts}'
        if ct_min_tag is not None:
            input_sql = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6} {ts}'
        if c_multi_tag is not None:
            input_sql = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} c10={c9} {ts}'
        if t_multi_tag is not None:
            input_sql = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} t9={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if c_blank_tag is not None:
            input_sql = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} {ts}'
        if t_blank_tag is not None:
            input_sql = f'{stb_name} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if chinese_tag is not None:
            input_sql = f'{stb_name},to=L"涛思数据" c0=L"涛思数据" {ts}'
        return input_sql

    def gen_opentsdb_telnet_line(self, stb_name, tb_name, id, value, t0, t1, t2, t3, t4, t5, t6, t7, t8, ts,
            id_noexist_tag, id_change_tag,id_double_tag, t_add_tag, t_mul_tag, point_trans_tag, tcp_keyword_tag,
            c_multi_tag, multi_field_tag, c_blank_tag, t_blank_tag, chinese_tag):
        input_sql = f'{stb_name} {ts} {value} {id}={tb_name} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8}'
        if id_noexist_tag is not None:
            input_sql = f'{stb_name} {ts} {value} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8}'
            if t_add_tag is not None:
                input_sql = f'{stb_name} {ts} {value} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8} t9={t8}'
        if id_change_tag is not None:
            input_sql = f'{stb_name} {ts} {value} t0={t0} {id}={tb_name} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8}'
        if id_double_tag is not None:
            input_sql = f'{stb_name} {ts} {value} {id}=\"{tb_name}_1\" t0={t0} t1={t1} {id}=\"{tb_name}_2\" t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8}'
        if t_add_tag is not None:
            input_sql = f'{stb_name} {ts} {value} {id}={tb_name} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8} t11={t1} t10={t8}'
        if t_mul_tag is not None:
            input_sql = f'{stb_name} {ts} {value} {id}={tb_name} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6}'
            if id_noexist_tag is not None:
                input_sql = f'{stb_name} {ts} {value} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6}'
        if c_multi_tag is not None:
            input_sql = f'{stb_name} {ts} {value} {value} {id}={tb_name} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6}'
        if c_blank_tag is not None:
            input_sql = f'{stb_name} {ts} {id}={tb_name} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8}'
        if t_blank_tag is not None:
            input_sql = f'{stb_name} {ts} {value}'
        if chinese_tag is not None:
            input_sql = f'{stb_name} {ts} L"涛思数据" t0={t0} t1=L"涛思数据"'
        if multi_field_tag is not None:
            input_sql = f'{stb_name} {ts} {value} {id}={tb_name} t0={t0} {value}'
        if point_trans_tag is not None:
            input_sql = f'.point.trans.test {ts} {value} t0={t0}'
        if tcp_keyword_tag is not None:
            input_sql = f'put {ts} {value} t0={t0}'
        if self.sml_type == "telnet-tcp":
            input_sql = 'put ' + input_sql + '\n'
        return input_sql

    def gen_full_type_sql(self, stb_name="", tb_name="", value="", t0="", t1="127i8", t2="32767i16", t3="2147483647i32",
            t4="9223372036854775807i64", t5="11.12345f32", t6="22.123456789f64", t7="\"binaryTagValue\"",
            t8="L\"ncharTagValue\"", c0="", c1="127i8", c2="32767i16", c3="2147483647i32",
            c4="9223372036854775807i64", c5="11.12345f32", c6="22.123456789f64", c7="\"binaryColValue\"",
            c8="L\"ncharColValue\"", c9="7u64", ts=None,
            id_noexist_tag=None, id_change_tag=None, id_upper_tag=None, id_mixul_tag=None, id_double_tag=None,
            ct_add_tag=None, ct_am_tag=None, ct_ma_tag=None, ct_min_tag=None, c_multi_tag=None, t_multi_tag=None,
            c_blank_tag=None, t_blank_tag=None, chinese_tag=None, t_add_tag=None, t_mul_tag=None, point_trans_tag=None,
            tcp_keyword_tag=None, multi_field_tag=None):
        if stb_name == "":
            stb_name = self.get_long_name()
        if tb_name == "":
            tb_name = f'{stb_name}_{random.randint(0, 65535)}_{random.randint(0, 65535)}'
        if t0 == "":
            t0 = "t"
        if c0 == "":
            c0 = random.choice(["f", "F", "false", "False", "t", "T", "true", "True"])
        if value == "":
            # value = random.choice(["f", "F", "false", "False", "t", "T", "true", "True", "TRUE", "FALSE"])
            value = random.choice(["1.11111", "2.2222222", "3.333333333333", "5.555555555555"])
        if id_upper_tag is not None:
            id = "ID"
        else:
            id = "id"
        if id_mixul_tag is not None:
            id = random.choice(["iD", "Id"])
        else:
            id = "id"
        if self.sml_type == "influxdb" or self.sml_type == "influxdb_restful":
            if ts is None:
                ts = "1626006833639000000"
            input_sql = self.gen_influxdb_line(stb_name, tb_name, id, t0, t1, t2, t3, t4, t5, t6, t7, t8, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, ts,
                id_noexist_tag, id_change_tag, id_double_tag, ct_add_tag, ct_am_tag, ct_ma_tag, ct_min_tag, c_multi_tag, t_multi_tag, c_blank_tag, t_blank_tag, chinese_tag)
        elif self.sml_type == "opentsdb_telnet" or self.sml_type == "opentsdb_telnet_restful" or self.sml_type == "telnet-tcp":
            if ts is None:
                ts = "1626006833641"
            input_sql = self.gen_opentsdb_telnet_line(stb_name, tb_name, id, value, t0, t1, t2, t3, t4, t5, t6, t7, t8, ts,
                id_noexist_tag, id_change_tag,id_double_tag, t_add_tag, t_mul_tag, point_trans_tag, tcp_keyword_tag,
                c_multi_tag, multi_field_tag, c_blank_tag, t_blank_tag, chinese_tag)
        return input_sql, stb_name

    def gen_multi_tag_col_str(self, gen_type, count):
        """
        gen_type must be "tag"/"col"
        """
        if gen_type == "tag":
            if self.sml_type == "influxdb" or self.sml_type == "influxdb_restful":
                return ','.join(map(lambda i: f't{i}=f', range(count))) + " "
            elif self.sml_type == "opentsdb_telnet" or self.sml_type == "opentsdb_telnet_restful":
                return ' '.join(map(lambda i: f't{i}=f', range(count)))
        if gen_type == "col":
            return ','.join(map(lambda i: f'c{i}=t', range(count))) + " "

    def gen_long_sql(self, tag_count, col_count):
        stb_name = self.get_long_name()
        tb_name = f'{stb_name}_1'
        tag_str = self.gen_multi_tag_col_str("tag", tag_count)
        col_str = self.gen_multi_tag_col_str("col", col_count)
        if self.sml_type == "influxdb" or self.sml_type == "influxdb_restful":
            ts = "1626006833640000000"
            long_sql = stb_name + ',' + f'id={tb_name}' + ',' + tag_str + col_str + ts
        elif self.sml_type == "opentsdb_telnet" or self.sml_type == "opentsdb_telnet_restful":
            ts = "1626006833641"
            long_sql = stb_name + ' ' + ts + ' ' + "t" + ' ' + tag_str
        return long_sql, stb_name

    def get_no_id_tbname(self, stb_name, dbname=None):
        query_sql = f"select tbname from {stb_name}"
        # if dbname is not None:
        #     query_sql = f"select tbname from {dbname}.{stb_name}"
        if self.sml_type == "influxdb_restful" or self.sml_type == "opentsdb_json_restful":
            query_sql = f"select tbname from {dbname}.{stb_name}"
            tb_name = self.res_handle(query_sql, stb_name, dbname)[0][0]
        else:
            tb_name = self.res_handle(query_sql, stb_name)[0][0]
        return tb_name

    def res_handle(self, query_sql, stb_name):
        self.tdSql.execute('reset query cache')
        if self.sml_type == "telnet-tcp":
            time.sleep(2)
        self.tdSql.query(f'describe {stb_name}')
        col_info = self.tdSql.getColNameList(True)
        self.tdSql.query(query_sql)
        row_info = self.tdSql.query_data
        res_row_list = []
        sub_list = []
        for row_mem in row_info:
            for i in row_mem:
                if "11.1234" in str(i) and str(i) != "11.12345f32" and str(i) != "11.12345027923584F32":
                    sub_list.append("11.12345027923584")
                elif "22.1234" in str(i) and str(i) != "22.123456789f64" and str(i) != "22.123456789F64":
                    sub_list.append("22.123456789")
                else:
                    sub_list.append(str(i))
            res_row_list.append(sub_list)
        res_field_list_without_ts = col_info[0][1:]
        res_type_list = col_info[1]
        return res_row_list, res_field_list_without_ts, res_type_list

    def restful_res_handle(self, query_sql, stb_name, dbname):
        self.tdRest.request(f'describe {dbname}.{stb_name}')
        col_info = self.tdRest.getColNameList(True)
        self.tdRest.request(query_sql)
        row_info = self.tdRest.resp["data"]
        res_row_list = []
        sub_list = []
        for row_mem in row_info:
            for i in row_mem:
                if "11.1234" in str(i) and str(i) != "11.12345f32":
                        sub_list.append("11.12345027923584")
                elif "22.1234" in str(i) and str(i) != "22.123456789f64":
                    sub_list.append("22.123456789")
                else:
                    sub_list.append(str(i))
            res_row_list.append(sub_list)
        res_field_list_without_ts = col_info[0][1:]
        res_type_list = col_info[1]
        return res_row_list, res_field_list_without_ts, res_type_list

    def sml_insert(self, input_sql, ts_type, precision, dbname=None):
        if self.sml_type == "influxdb":
            if precision == None:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, ts_type)
            else:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, precision)
        elif self.sml_type == "opentsdb_telnet":
            if precision == None:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, ts_type)
            else:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, precision)
        elif self.sml_type == "opentsdb_json":
            if precision == None:
                self.tdSql._conn.schemaless_insert([json.dumps(input_sql)], TDSmlProtocolType.JSON.value, None)
        elif self.sml_type == "influxdb_restful":
            if precision == None:
                self.tdRest.schemalessApiPost(sql=input_sql, precision=ts_type, dbname=dbname)
            else:
                self.tdRest.schemalessApiPost(sql=input_sql, precision=precision, dbname=dbname)
        elif self.sml_type == "opentsdb_json_restful":
            self.tdRest.schemalessApiPost(json.dumps(input_sql), url_type="json", precision=None, dbname=dbname)
        elif self.sml_type == "opentsdb_telnet_restful":
            self.tdRest.schemalessApiPost(input_sql, url_type="telnet", precision=None, dbname=dbname)

    def check_res(self, input_sql, stb_name, query_sql="select * from", ts_type=None, condition="", ts=None, id=True, none_check_tag=None, precision=None, protocol=None, t_blank_tag=None, dbname=None):
        if self.sml_type == "influxdb":
            expect_list = self.influxdb_input_handle(input_sql, ts_type)
        elif self.sml_type == "opentsdb_telnet" or self.sml_type == "telnet-tcp" or self.sml_type == "opentsdb_telnet_restful":
            expect_list = self.opentsdb_telnet_input_handle(input_sql, ts_type)
        elif self.sml_type == "opentsdb_json" or self.sml_type == "opentsdb_json_restful":
            expect_list = self.opentsdb_json_input_handle(input_sql)
        elif self.sml_type == "influxdb_restful":
            expect_list = self.influxdb_restful_input_handle(input_sql, ts_type, t_blank_tag)

        if self.sml_type == "telnet-tcp":
            self.tcp_client(input_sql)
        else:
            self.sml_insert(input_sql, ts_type, precision, dbname=dbname)
        if self.sml_type == "influxdb_restful" or self.sml_type == "opentsdb_json_restful" or self.sml_type == "opentsdb_telnet_restful":
            query_sql = f"{query_sql} {dbname}.{stb_name} {condition}"
            res_row_list, res_field_list_without_ts, res_type_list = self.restful_res_handle(query_sql, stb_name, dbname)
        else:
            query_sql = f"{query_sql} {stb_name} {condition}"
            res_row_list, res_field_list_without_ts, res_type_list = self.res_handle(query_sql, stb_name)
        if ts == 0:
            res_ts = self.date_to_ts(res_row_list[0][0])
            current_time = time.time()
            if current_time - res_ts < 60:
                self.tdSql.checkEqual(res_row_list[0][1:], expect_list[0][1:])
            else:
                self.tdSql.checkEqual(res_row_list[0], expect_list[0])
        else:
            if none_check_tag is not None:
                none_index_list = [i for i,x in enumerate(res_row_list[0]) if x=="None"]
                none_index_list.reverse()
                for j in none_index_list:
                    res_row_list[0].pop(j)
                    expect_list[0].pop(j)
            self.tdSql.checkEqual(sorted(res_row_list[0]), sorted(expect_list[0]))
        self.tdSql.checkEqual(sorted(res_field_list_without_ts), sorted(expect_list[1]))
        self.tdSql.checkEqual(res_type_list, expect_list[2])

    def gen_sql_list(self, count=5, stb_name="", tb_name=""):
        """
        stb --> supertable
        tb  --> table
        ts  --> timestamp, same default
        col --> column, same default
        tag --> tag, same default
        d   --> different
        s   --> same
        a   --> add
        m   --> minus
        """
        d_stb_d_tb_list = list()
        s_stb_s_tb_list = list()
        s_stb_s_tb_a_col_a_tag_list = list()
        s_stb_s_tb_m_col_m_tag_list = list()
        s_stb_d_tb_list = list()
        s_stb_d_tb_a_col_m_tag_list = list()
        s_stb_d_tb_a_tag_m_col_list = list()
        s_stb_s_tb_d_ts_list = list()
        s_stb_s_tb_d_ts_a_col_m_tag_list = list()
        s_stb_s_tb_d_ts_a_tag_m_col_list = list()
        s_stb_d_tb_d_ts_list = list()
        s_stb_d_tb_d_ts_a_col_m_tag_list = list()
        s_stb_d_tb_d_ts_a_tag_m_col_list = list()
        for i in range(count):
            d_stb_d_tb_list.append(self.gen_full_type_sql(c0="t"))
            s_stb_s_tb_list.append(self.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name, c7=f'"{self.get_long_name()}"'))
            s_stb_s_tb_a_col_a_tag_list.append(self.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name, c7=f'"{self.get_long_name()}"', ct_add_tag=True))
            s_stb_s_tb_m_col_m_tag_list.append(self.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name, c7=f'"{self.get_long_name()}"', ct_min_tag=True))
            s_stb_d_tb_list.append(self.gen_full_type_sql(stb_name=stb_name, t7=f'"{self.get_long_name()}"', c7=f'"{self.get_long_name()}"', id_noexist_tag=True))
            s_stb_d_tb_a_col_m_tag_list.append(self.gen_full_type_sql(stb_name=stb_name, t7=f'"{self.get_long_name()}"', c7=f'"{self.get_long_name()}"', id_noexist_tag=True, ct_am_tag=True))
            s_stb_d_tb_a_tag_m_col_list.append(self.gen_full_type_sql(stb_name=stb_name, t7=f'"{self.get_long_name()}"', c7=f'"{self.get_long_name()}"', id_noexist_tag=True, ct_ma_tag=True))
            s_stb_s_tb_d_ts_list.append(self.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name, t7=f'"{self.get_long_name()}"', c7=f'"{self.get_long_name()}"', ts=0))
            s_stb_s_tb_d_ts_a_col_m_tag_list.append(self.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name, c7=f'"{self.get_long_name()}"', ts=0, ct_am_tag=True))
            s_stb_s_tb_d_ts_a_tag_m_col_list.append(self.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name, t7=f'"{self.get_long_name()}"', c7=f'"{self.get_long_name()}"', ts=0, ct_ma_tag=True))
            s_stb_d_tb_d_ts_list.append(self.gen_full_type_sql(stb_name=stb_name, t7=f'"{self.get_long_name()}"', c7=f'"{self.get_long_name()}"', id_noexist_tag=True, ts=0))
            s_stb_d_tb_d_ts_a_col_m_tag_list.append(self.gen_full_type_sql(stb_name=stb_name, t7=f'"{self.get_long_name()}"', c7=f'"{self.get_long_name()}"', id_noexist_tag=True, ts=0, ct_am_tag=True))
            s_stb_d_tb_d_ts_a_tag_m_col_list.append(self.gen_full_type_sql(stb_name=stb_name, t7=f'"{self.get_long_name()}"', c7=f'"{self.get_long_name()}"', id_noexist_tag=True, ts=0, ct_ma_tag=True))
        return d_stb_d_tb_list, s_stb_s_tb_list, s_stb_s_tb_a_col_a_tag_list, s_stb_s_tb_m_col_m_tag_list, \
            s_stb_d_tb_list, s_stb_d_tb_a_col_m_tag_list, s_stb_d_tb_a_tag_m_col_list, s_stb_s_tb_d_ts_list, \
            s_stb_s_tb_d_ts_a_col_m_tag_list, s_stb_s_tb_d_ts_a_tag_m_col_list, s_stb_d_tb_d_ts_list, \
            s_stb_d_tb_d_ts_a_col_m_tag_list, s_stb_d_tb_d_ts_a_tag_m_col_list

    def gen_json_list(self, count=5, stb_name="", tb_name="", value_type="obj"):
        """
            stb --> supertable
            tb  --> table
            ts  --> timestamp, same default
            col --> column, same default
            tag --> tag, same default
            d   --> different
            s   --> same
            a   --> add
            m   --> minus
        """
        d_stb_d_tb_list = list()
        s_stb_s_tb_list = list()
        s_stb_s_tb_a_tag_list = list()
        s_stb_s_tb_m_tag_list = list()
        s_stb_d_tb_list = list()
        s_stb_d_tb_m_tag_list = list()
        s_stb_d_tb_a_tag_list = list()
        s_stb_s_tb_d_ts_list = list()
        s_stb_s_tb_d_ts_m_tag_list = list()
        s_stb_s_tb_d_ts_a_tag_list = list()
        s_stb_d_tb_d_ts_list = list()
        s_stb_d_tb_d_ts_m_tag_list = list()
        s_stb_d_tb_d_ts_a_tag_list = list()
        for i in range(count):
            d_stb_d_tb_list.append(self.gen_full_type_json(col_value=self.gen_ts_col_value(value=True, t_type="bool", value_type=value_type), tag_value=self.gen_tag_value(t0_value=True, value_type=value_type)))
            s_stb_s_tb_list.append(self.gen_full_type_json(stb_name=stb_name, tb_name=tb_name, col_value=self.gen_ts_col_value(value=self.get_long_name(), t_type="binary", value_type=value_type), tag_value=self.gen_tag_value(value_type=value_type)))
            s_stb_s_tb_a_tag_list.append(self.gen_full_type_json(stb_name=stb_name, tb_name=tb_name, col_value=self.gen_ts_col_value(value=self.get_long_name(), t_type="binary", value_type=value_type), tag_value=self.gen_tag_value(value_type=value_type), t_add_tag=True))
            s_stb_s_tb_m_tag_list.append(self.gen_full_type_json(stb_name=stb_name, tb_name=tb_name, col_value=self.gen_ts_col_value(value=self.get_long_name(), t_type="binary", value_type=value_type), tag_value=self.gen_tag_value(value_type=value_type), t_mul_tag=True))
            s_stb_d_tb_list.append(self.gen_full_type_json(stb_name=stb_name, col_value=self.gen_ts_col_value(value=self.get_long_name(), t_type="binary", value_type=value_type), tag_value=self.gen_tag_value(t7_value=self.get_long_name(), value_type=value_type), id_noexist_tag=True))
            s_stb_d_tb_m_tag_list.append(self.gen_full_type_json(stb_name=stb_name, col_value=self.gen_ts_col_value(value=self.get_long_name(), t_type="binary", value_type=value_type), tag_value=self.gen_tag_value(t7_value=self.get_long_name(), value_type=value_type), id_noexist_tag=True, t_mul_tag=True))
            s_stb_d_tb_a_tag_list.append(self.gen_full_type_json(stb_name=stb_name, col_value=self.gen_ts_col_value(value=self.get_long_name(), t_type="binary", value_type=value_type), tag_value=self.gen_tag_value(t7_value=self.get_long_name(), value_type=value_type), id_noexist_tag=True, t_add_tag=True))
            s_stb_s_tb_d_ts_list.append(self.gen_full_type_json(stb_name=stb_name, tb_name=tb_name, col_value=self.gen_ts_col_value(value=self.get_long_name(), t_type="binary", value_type=value_type), tag_value=self.gen_tag_value(t7_value=self.get_long_name(), value_type=value_type), ts_value = self.gen_ts_col_value(1626006833639000000, "ns")))
            s_stb_s_tb_d_ts_m_tag_list.append(self.gen_full_type_json(stb_name=stb_name, tb_name=tb_name, col_value=self.gen_ts_col_value(value=self.get_long_name(), t_type="binary", value_type=value_type), tag_value=self.gen_tag_value(t7_value=self.get_long_name(), value_type=value_type), ts_value = self.gen_ts_col_value(1626006833639000000, "ns"), t_mul_tag=True))
            s_stb_s_tb_d_ts_a_tag_list.append(self.gen_full_type_json(stb_name=stb_name, tb_name=tb_name, col_value=self.gen_ts_col_value(value=self.get_long_name(), t_type="binary", value_type=value_type), tag_value=self.gen_tag_value(t7_value=self.get_long_name(), value_type=value_type), ts_value = self.gen_ts_col_value(1626006833639000000, "ns"), t_add_tag=True))
            s_stb_d_tb_d_ts_list.append(self.gen_full_type_json(stb_name=stb_name, col_value=self.gen_ts_col_value(value=self.get_long_name(), t_type="binary", value_type=value_type), tag_value=self.gen_tag_value(t7_value=self.get_long_name(), value_type=value_type), id_noexist_tag=True, ts_value = self.gen_ts_col_value(1626006833639000000, "ns")))
            s_stb_d_tb_d_ts_m_tag_list.append(self.gen_full_type_json(stb_name=stb_name, col_value=self.gen_ts_col_value(value=self.get_long_name(), t_type="binary", value_type=value_type), tag_value=self.gen_tag_value(t7_value=self.get_long_name(), value_type=value_type), id_noexist_tag=True, ts_value = self.gen_ts_col_value(0, "ns"), t_mul_tag=True))
            s_stb_d_tb_d_ts_a_tag_list.append(self.gen_full_type_json(stb_name=stb_name, col_value=self.gen_ts_col_value(value=self.get_long_name(), t_type="binary", value_type=value_type), tag_value=self.gen_tag_value(t7_value=self.get_long_name(), value_type=value_type), id_noexist_tag=True, ts_value = self.gen_ts_col_value(0, "ns"), t_add_tag=True))

        return d_stb_d_tb_list, s_stb_s_tb_list, s_stb_s_tb_a_tag_list, s_stb_s_tb_m_tag_list, \
            s_stb_d_tb_list, s_stb_d_tb_m_tag_list, s_stb_d_tb_a_tag_list, s_stb_s_tb_d_ts_list, \
            s_stb_s_tb_d_ts_m_tag_list, s_stb_s_tb_d_ts_a_tag_list, s_stb_d_tb_d_ts_list, \
            s_stb_d_tb_d_ts_m_tag_list, s_stb_d_tb_d_ts_a_tag_list

    def gen_multi_thread_sql(self, sql_list):
        tlist = list()
        if self.sml_type == "influxdb":
            sml_type_value = TDSmlProtocolType.LINE.value
        elif self.sml_type == "opentsdb_telnet":
            sml_type_value = TDSmlProtocolType.TELNET.value
        elif self.sml_type == "opentsdb_json":
            sml_type_value = TDSmlProtocolType.JSON.value
        for insert_sql in sql_list:
            if self.sml_type == "opentsdb_json":
                t = threading.Thread(target=self.tdSql._conn.schemaless_insert,args=([json.dumps(insert_sql[0])], sml_type_value, None))
            else:
                t = threading.Thread(target=self.tdSql._conn.schemaless_insert,args=([insert_sql[0]], sml_type_value, None))
            tlist.append(t)
        return tlist

    def multi_thread_run(self, tlist):
        for t in tlist:
            t.start()
        for t in tlist:
            t.join()

    def delete_end_zero(self,ts):
        if self.sml_type is None:
            while ts.endswith("0"):
                ts = ts[:-1]
                if ts.endswith("."):
                    ts = ts[:-1]
            sp_ts = ts.split(".")
            if len(sp_ts[1]) < 3:
                ts = str(ts) + (3 - len(sp_ts[1])) * "0"
            elif 3 < len(sp_ts[1]) < 6:
                ts = str(ts) + (6 - len(sp_ts[1])) * "0"
            elif 6 < len(sp_ts[1]) < 9:
                ts = str(ts) + (9 - len(sp_ts[1])) * "0"
            else:
                pass
        return ts

    @classmethod
    def download_pkg(self, version, remote, host):
        # set  package server host to default value
        server = TDCom.package_server_host_default
        if TDCom.package_server_host_variable in os.environ:
            # get package server host
            server = os.getenv(TDCom.package_server_host_variable)

        # set package server username to default value
        user = TDCom.package_server_username_default
        if TDCom.package_server_host_variable in os.environ:
            # get package server username
            user = os.getenv(TDCom.package_server_username_variable)

        # set package server password to default value
        password = TDCom.package_server_password_default
        if TDCom.package_server_password_variable in os.environ:
            # get package server password
            password = os.getenv(TDCom.package_server_password_variable)

        # set package server root to default value
        rootPath = TDCom.package_server_root_default
        if TDCom.package_server_root_variable in os.environ:
            # get package server root
            rootPath = os.getenv(TDCom.package_server_root_variable)

        # get package from server
        filePath = f"{rootPath}/v{version}/enterprise"
        fileName = f"TDengine-enterprise-server-{version}-Linux-x64.tar.gz"
        remote.get(server, f"{filePath}/{fileName}", "/tmp/", password)
        # push package to host
        filePath = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        filePath = f"taostest-install-{filePath}"
        pkgDir = f"TDengine-enterprise-server-{version}"
        remote.put(host, f"/tmp/{fileName}", f"/tmp/{filePath}")
        # extract package && install
        cmds = [f"cd /tmp/{filePath}", f"tar xzf {fileName}", f"cd {pkgDir}", f"./install.sh -e no"]
        result = remote.cmd(host, cmds)
        # remove local file
        result = remote.cmd(host, [f"rm -rf /tmp/{filePath}"])

    @classmethod
    def install_with_pkg(self, remote, host, package):
        # push package to host
        filePath = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        filePath = f"taostest-install-{filePath}"
        remote.put(host, package, f"/tmp/{filePath}")
        # extract package && install
        fileName = os.path.basename(package)
        cmds = [f"cd /tmp/{filePath}", f"tar xzf {fileName}", f"rm -f {fileName}", f"ls | tail -n1"]
        pkgDir = remote.cmd(host, cmds)
        if not pkgDir is None:
            pkgDir = pkgDir.strip()
            cmds = [f"cd /tmp/{filePath}/{pkgDir}", f"./install.sh -e no"]
            result = remote.cmd(host, cmds)
        # remove local file
        result = remote.cmd(host, [f"rm -rf /tmp/{filePath}"])

    def drop_remote_ports(self, remote, host, port_list, rule="OUTPUT", policy="tcp"):
        for port in port_list:
            remote.cmd(host, [f'iptables -I {rule} -p {policy} --dport {port} -j DROP'])

    def accept_remote_ports(self, remote, host, port_list, rule="OUTPUT", policy="tcp"):
        for port in port_list:
            remote.cmd(host, [f'iptables -I {rule} -p {policy} --dport {port} -j ACCEPT'])
    
    def clean_remote_iptables(self, remote, host):
        remote.cmd(host, [f'iptables -F'])


    def schemacfg(self, insert_type):
        int_type_list = ["tinyint", "smallint", "int", "bigint", "tinyint unsigned", "smallint unsigned", "int unsigned", "bigint unsigned", "float", "double"]
        # if insert_type in int_type_list:

    def setDBinfo(self,
                  name: str = "db_test",
                  drop: str = "yes",
                  **kwargs
                  ):
        res = {
            "name": name,
            "drop": drop,
        }
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                res[param] = value
        return res

    def setStreamDBinfo(self,
                  name: str = "stream_db",
                  drop: str = "yes",
                  **kwargs
                  ):
        res = {
            "name": name,
            "drop": drop,
        }
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                res[param] = value
        return {"dbinfo": res}

    def setStbinfo(self,
        columns             : list = [] ,
        tags                : list = [] ,
        name : str = "stb" ,
        child_table_exists  : str = "no" ,
        childtable_count    : int = 1 ,
        childtable_prefix   : str = "ctb" ,
        auto_create_table   : str = "no",
        escape_character    : str = "no",
        batch_create_tbl_num: int = 10 ,
        data_source         : str = "rand" ,
        insert_mode         : str = "taosc" ,
        rollup              : str = None ,
        interlace_rows      : str = 0 ,
        line_protocol       : str = None,
        tcp_transfer        : str = "no",
        insert_rows         : int = 10 ,
        partial_col_num     : int = 0,
        rows_per_tbl        : int = 0 ,
        max_sql_len         : int = 1024000 ,
        disorder_ratio      : int = 0 ,
        disorder_range      : int = 1000 ,
        timestamp_step      : int = 10 ,
        keep_trying         : int = 0 ,
        trying_interval     : int = 0 ,
        start_timestamp     : str = f"{datetime.now():%F %X}" ,
        sample_format       : str = "csv" ,
        sample_file         : str = "./sample.csv" ,
        tags_file           : str = "",
        **kwargs
    ):
        stb = {
            "name": name,
            "child_table_exists": child_table_exists,
            "childtable_count": childtable_count,
            "childtable_prefix": childtable_prefix,
            "escape_character": escape_character,
            "auto_create_table": auto_create_table,
            "batch_create_tbl_num": batch_create_tbl_num,
            "data_source": data_source,
            "insert_mode": insert_mode,
            "rollup": rollup,
            "interlace_rows": interlace_rows,
            "line_protocol":line_protocol,
            "tcp_transfer":tcp_transfer,
            "insert_rows": insert_rows,
            "partial_col_num": partial_col_num,
            "rows_per_tbl": rows_per_tbl,
            "max_sql_len": max_sql_len,
            "disorder_ratio": disorder_ratio,
            "disorder_range": disorder_range,
            "keep_trying": keep_trying,
            "timestamp_step": timestamp_step,
            "trying_interval": trying_interval,
            "start_timestamp": start_timestamp,
            "sample_format": sample_format,
            "sample_file": sample_file,
            "tags_file": tags_file,
            "columns": columns,
            "tags": tags,
        }
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                stb[param] = value
        return stb

    def setStreams(self,
        stream_name : str = None ,
        stream_stb  : str = None ,
        trigger_mode    : str = None ,
        watermark   : str = None ,
        source_sql    : str = None,
        drop: str = None
    ):
        stream_dict = {
            "stream_name": stream_name,
            "stream_stb": stream_stb,
            "trigger_mode": trigger_mode,
            "watermark": watermark,
            "source_sql": source_sql,
            "drop": drop
        }
        return stream_dict

    def setDatabases(self,
                     dbinfo: dict = {},
                     super_tables: list = []
                     ):
        dbinfo = dbinfo if dbinfo else self.setDBinfo()
        super_tables = super_tables if super_tables else [self.setStbinfo()]

        database_dict = {
            "dbinfo": dbinfo,
            "super_tables": super_tables
        }
        return database_dict

    def setJsoninfo(self,
       cfgdir                   : str           = "/etc/taos",
       host                     : str           = "127.0.0.1",
       port                     : int           = 6030,
       rest_port                : int           = 6041,
       user                     : str           = "root",
       password                 : str           = "taosdata",
       thread_count             : int           = cpu_count(),
       create_table_thread_count  : int         = cpu_count(),
       result_file              : str           = f'/tmp/taosBenchmark_{datetime.now():%F_%X}.log',
       confirm_parameter_prompt : str           = "no",
       insert_interval          : int           = 0,
       num_of_records_per_req   : int           = 100,
       prepare_rand             : int           = 10000,
       max_sql_len              : int           = 1024000,
       databases                : list          = [],
       stream_db                : dict          = None,
       streams                  : list          = None,
       chinese                  : str           = "no",
       test_log                 : str           = "/root/testlog/"
    ):

        """
        description:
            This function provides a way to create insert json file that does not involve database
        param :
            { **kargs : cau use instance.setJsoninfo(attribute=value)}
        return
            a dict that be used to generate json-file
        """

        self.cfgdir = cfgdir
        self.host = host
        self.user = user
        self.port = port
        self.rest_port = rest_port
        self.password = password
        self.thread = thread_count
        self.create_table_thread_count = create_table_thread_count
        self.result_file = result_file
        self.confirm_parameter_prompt = confirm_parameter_prompt
        self.insert_interval = insert_interval
        self.num_of_records_per_req = num_of_records_per_req
        self.max_sql_len = max_sql_len

        databases = databases if databases else [self.setDatabases()]
        if stream_db:
            databases.append(stream_db)

        json_info = {
            "filetype": self._type ,
            "cfgdir": self.cfgdir ,
            "host": self.host ,
            "port": self.port ,
            "rest_port": self.rest_port ,
            "user": self.user ,
            "password": self.password ,
            "thread_count": self.thread ,
            "create_table_thread_count": self.create_table_thread_count ,
            "result_file": self.result_file,
            "confirm_parameter_prompt": self.confirm_parameter_prompt,
            "insert_interval": self.insert_interval,
            "num_of_records_per_req": self.num_of_records_per_req,
            "max_sql_len": self.max_sql_len,
            "databases": databases ,
            "insert_interval": insert_interval,
            "prepare_rand": prepare_rand,
            "chinese": chinese,
            "test_log": test_log
        }
        json_info["streams"] = [streams] if streams else False
        return json_info

    def set_specified_table_query(self,
        query_interval    : int = 1,
        concurrent        : int = 1,
        sqls              : list = [],
        ):
        specified_table_query_dict = {
            "query_interval": query_interval,
            "concurrent": concurrent,
            "sqls": sqls
        }
        return specified_table_query_dict

    def set_super_table_query(self,
        stblname          : str = "meters",
        query_interval    : int = 1,
        threads           : int = cpu_count(),
        sqls              : list = [],
        ):
        super_table_query_dict = {
            "stblname": stblname,
            "query_interval": query_interval,
            "threads": threads,
            "sqls": sqls
        }
        return super_table_query_dict

    def setQueryJsoninfo(self,
       filetype                     : str           = "query",
       cfgdir                       : str           = "/etc/taos",
       host                         : str           = "127.0.0.1",
       port                         : int           = 6030,
       user                         : str           = "root",
       password                     : str           = "taosdata",
       confirm_parameter_prompt     : str           = "no",
       database                     : str           = "test",
       query_times                  : int           = 2,
       query_mode                   : str           = "taosc",
       specified_table_query        : dict          = dict(),
       super_table_query            : dict          = None,
       test_log                     : str           = "/root/testlog/"
    ):

        """
        description:
            This function provides a way to create query json file
        """

        self.filetype = filetype
        self.cfgdir = cfgdir
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.confirm_parameter_prompt = confirm_parameter_prompt
        self.database = database
        self.query_times = query_times
        self.query_mode = query_mode

        specified_table_query = specified_table_query if specified_table_query else [self.set_specified_table_query()]
        json_info = {
            "filetype": self.filetype ,
            "cfgdir": self.cfgdir ,
            "host": self.host ,
            "port": self.port ,
            "user": self.user ,
            "password": self.password ,
            "confirm_parameter_prompt": self.confirm_parameter_prompt,
            "databases": self.database,
            "query_times": self.query_times,
            "query_mode": self.query_mode,
            "specified_table_query": specified_table_query,
            "test_log": test_log
        }
        if super_table_query:
            json_info["super_table_query"] = super_table_query
        return json_info

    def genBenchmarkJson(self, result_dir:str=None, json_name: str=None, base_config=None,  **kwargs):
        result_dir = result_dir if result_dir else self.result_dir
        json_name = json_name if json_name else self.json_file
        base_config = base_config if  base_config else self.setJsoninfo()
        return gen_benchmark_json(result_dir, json_name, base_config, **kwargs)

    def put_file(self, remote, iplist: list, json_data: list, file_name: list, run_log_dir: str):
        """
        description: This method is used to put zhe file to target machine.

        param remote: remote function
        param iplist: the list of fqdn from env.yaml
        param json_data: the data from jsonfile
        param file_name: json file list
        param run_log_dir: global run_log_dir
                """

        # create test_log and put task json files on target machine
        for i in range(len(iplist)):
            remote.cmd(iplist[i], [f'mkdir {json_data[i]["test_log"]}'])
            remote.put(iplist[i], run_log_dir + "/" + str(file_name[i]), json_data[i]["test_log"])

    def threads_run_taosBenchmark(self, remote, iplist, json_data, file_name: list, env_setting, run_log_dir: str, sleep_time=1):
        """
        description: This method is used to start several threads to run taosBenchmark ,and then
                    get the result file to local machine
        param remote: remote function
        param iplist: the list of fqdn from env.yaml
        param json_data: the data from jsonfile
        param file_name: json file list
        param env_setting: global env_setting
        param run_log_dir: global run_log_dir
        return: result_files,use
        """
        t = []
        for i in range(len(iplist)):
            t.append(threading.Thread(target=remote.cmd,
                                      args=(
                                          iplist[i],
                                          [
                                              f'taosBenchmark -c {env_setting[0]["spec"]["config_dir"]} -f {json_data[i]["test_log"]}{file_name[i]} 2>&1 | tee /tmp/{i}.log '])))

            t[i].start()
            time.sleep(sleep_time)

        for i in t:
            i.join()

        result_files = []

        for i in range(len(t)):
            # rename result file
            filename = run_log_dir + '/' + str(i) + "-" + iplist[i]
            result_files.append(filename)
            # get result_files and remove test_log
            remote.get(iplist[i], f"/tmp/{i}.log", filename)
            # remote.cmd(iplist[i], [f'rm -rf {json_data[i]["test_log"]}'])

        return result_files

    def thread_pool(self, input_list, func, thread_count):
        pool = threadpool.ThreadPool(thread_count)
        requests = threadpool.makeRequests(func, input_list)
        [pool.putRequest(req) for req in requests]
        pool.wait()

    def multi_thread_query(self, tbname, query_sql_list, concurrent):
        if not query_sql_list:
            query_sql_list = [f'select last_row(*) from {tbname}',
                              f'select count(*) from {tbname}',
                              f'select count(*) from {tbname} where c1 = 1',
                              f'select avg(c1), max(c2), min(c1) from {tbname}',
                              f'select avg(c1), max(c2), min(c1) from {tbname} where c1 = 1',
                              f'select avg(c1), max(c2), min(c1) from {tbname} interval(10s)',
                              f'select avg(c1), max(c2), min(c1) from {tbname} group by tbname limit 10000',
                              f'select last(*) from {tbname} group by tbname limit 10000',
                              f'select last_row(*) from {tbname} group by tbname limit 10000']
        self.thread_pool(query_sql_list, self.tdSql.query, concurrent)


    def add_back_ground_scheduler(self, func, trigger, seconds, max_instances, args):
        scheduler = BackgroundScheduler()
        scheduler.add_job(func, trigger, seconds=seconds, max_instances=max_instances, args=args)
        scheduler.start()

    def load_json(self, json_file):
        with open(json_file, "r") as f:
          return json.load(f)
      
    def dump_json(self, out_file, json_info):
        with open(out_file, mode="w", encoding="utf8") as f:
            json.dump(json_info, f, ensure_ascii=False, indent=2)
