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
import os
import time
import datetime
import inspect
import psutil
import shutil
import json
from util.log import *
from multiprocessing import cpu_count


# TODO: fully test the function. Handle exceptions.
#       Handle json format not accepted by taosdemo

### How to use TaosdemoCfg:
#   Before you start: 
#       Make sure you understand how is taosdemo's JSON file structured. Because the python used does
#       not support directory in directory for self objects, the config is being tear to different parts.
#       Please make sure you understand which directory represent which part of which type of the file 
#       This module will reassemble the parts when creating the JSON file.
#
#   Basic use example
#   step 1:use self.append_sql_stb() to append the insert/query/subscribe directory into the module
#       you can append many insert/query/subscribe directory, but pay attention about taosdemo's limit
#   step 2:use alter function to alter the specific config 
#   step 3:use the generation function to generate the files
#
#   step 1 and step 2 can be replaced with using import functions
class TDTaosdemoCfg:
    def __init__(self):
        self.insert_cfg = {
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "thread_count": cpu_count(),
            "create_table_thread_count": cpu_count(),
            "result_file": "./insert_res.txt",
            "confirm_parameter_prompt": "no",
            "insert_interval": 0,
            "num_of_records_per_req": 32766,
            "max_sql_len": 32766,
            "databases": None
        }

        self.db = {
            "name": 'db',
            "drop": 'yes',
            "replica": 1,
            "duration": 10,
            "cache": 16,
            "blocks": 6,
            "precision": "ms",
            "keep": 3650,
            "minRows": 100,
            "maxRows": 4096,
            "comp": 2,
            "walLevel": 1,
            "cachelast": 0,
            "quorum": 1,
            "fsync": 3000,
            "update": 0
        }

        self.query_cfg = {
            "filetype": "query",
            "cfgdir": "/etc/taos",
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "confirm_parameter_prompt": "no",
            "databases": "db",
            "query_times": 2,
            "query_mode": "taosc",
            "specified_table_query": None,
            "super_table_query": None
        }

        self.table_query = {
            "query_interval": 1,
            "concurrent": 3,
            "sqls": None
        }

        self.stable_query = {
            "stblname": "stb",
            "query_interval": 1,
            "threads": 3,
            "sqls": None
        }

        self.sub_cfg = {
            "filetype": "subscribe",
            "cfgdir": "/etc/taos",
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "databases": "db",
            "confirm_parameter_prompt": "no",
            "specified_table_query": None,
            "super_table_query": None
        }

        self.table_sub = {
            "concurrent": 1,
            "mode": "sync",
            "interval": 10000,
            "restart": "yes",
            "keepProgress": "yes",
            "sqls": None
        }

        self.stable_sub = {
            "stblname": "stb",
            "threads": 1,
            "mode": "sync",
            "interval": 10000,
            "restart": "yes",
            "keepProgress": "yes",
            "sqls": None
        }

        self.stbs = []
        self.stb_template = {
            "name": "stb",
            "child_table_exists": "no",
            "childtable_count": 100,
            "childtable_prefix": "stb_",
            "auto_create_table": "no",
            "batch_create_tbl_num": 5,
            "data_source": "rand",
            "insert_mode": "taosc",
            "insert_rows": 100,
            "childtable_limit": 10,
            "childtable_offset": 0,
            "interlace_rows": 0,
            "insert_interval": 0,
            "max_sql_len": 32766,
            "disorder_ratio": 0,
            "disorder_range": 1000,
            "timestamp_step": 1,
            "start_timestamp": "2020-10-01 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "tags_file": "",
            "columns": [{"type": "INT", "count": 1}],
            "tags": [{"type": "BIGINT", "count": 1}]
        }

        self.tb_query_sql = []
        self.tb_query_sql_template = {
            "sql": "select last_row(*) from stb_0 ",
            "result": "temp/query_res0.txt"
        }

        self.stb_query_sql = []
        self.stb_query_sql_template = {
            "sql": "select last_row(ts) from xxxx",
            "result": "temp/query_res2.txt"
        }

        self.tb_sub_sql = []
        self.tb_sub_sql_template = {
            "sql": "select * from stb_0 ;",
            "result": "temp/subscribe_res0.txt"
        }

        self.stb_sub_sql = []
        self.stb_sub_sql_template = {
            "sql": "select * from xxxx where ts > '2021-02-25 11:35:00.000' ;",
            "result": "temp/subscribe_res1.txt"
        }

    # The following functions are import functions for different dicts and lists
    # except import_sql, all other import functions will a dict and overwrite the origional dict
    # dict_in: the dict used to overwrite the target
    def import_insert_cfg(self, dict_in):
        self.insert_cfg = dict_in

    def import_db(self, dict_in):
        self.db = dict_in

    def import_stbs(self, dict_in):
        self.stbs = dict_in

    def import_query_cfg(self, dict_in):
        self.query_cfg = dict_in

    def import_table_query(self, dict_in):
        self.table_query = dict_in

    def import_stable_query(self, dict_in):
        self.stable_query = dict_in

    def import_sub_cfg(self, dict_in):
        self.sub_cfg = dict_in

    def import_table_sub(self, dict_in):
        self.table_sub = dict_in

    def import_stable_sub(self, dict_in):
        self.stable_sub = dict_in

    def import_sql(self, Sql_in, mode):
        """used for importing the sql later used

        Args:
            Sql_in (dict): the imported sql dict
            mode (str): the sql storing location within TDTaosdemoCfg
                format: 'fileType_tableType'
                fileType: query, sub
                tableType: table, stable
        """
        if mode == 'query_table':
            self.tb_query_sql = Sql_in
        elif mode == 'query_stable':
            self.stb_query_sql = Sql_in
        elif mode == 'sub_table':
            self.tb_sub_sql = Sql_in
        elif mode == 'sub_stable':
            self.stb_sub_sql = Sql_in
    # import functions end

    # The following functions are alter functions for different dicts
    #   Args:
    #       key: the key that is going to be modified
    #       value: the value of the key that is going to be modified
    #           if key = 'databases' | "specified_table_query" | "super_table_query"|"sqls"
    #           value will not be used

    def alter_insert_cfg(self, key, value):

        if key == 'databases':
            self.insert_cfg[key] = [
                {
                    'dbinfo': self.db,
                    'super_tables': self.stbs
                }
            ]
        else:
            self.insert_cfg[key] = value

    def alter_db(self, key, value):
        self.db[key] = value

    def alter_query_tb(self, key, value):
        if key == "sqls":
            self.table_query[key] = self.tb_query_sql
        else:
            self.table_query[key] = value

    def alter_query_stb(self, key, value):
        if key == "sqls":
            self.stable_query[key] = self.stb_query_sql
        else:
            self.stable_query[key] = value

    def alter_query_cfg(self, key, value):
        if key == "specified_table_query":
            self.query_cfg["specified_table_query"] = self.table_query
        elif key == "super_table_query":
            self.query_cfg["super_table_query"] = self.stable_query
        else:
            self.query_cfg[key] = value

    def alter_sub_cfg(self, key, value):
        if key == "specified_table_query":
            self.sub_cfg["specified_table_query"] = self.table_sub
        elif key == "super_table_query":
            self.sub_cfg["super_table_query"] = self.stable_sub
        else:
            self.sub_cfg[key] = value

    def alter_sub_stb(self, key, value):
        if key == "sqls":
            self.stable_sub[key] = self.stb_sub_sql
        else:
            self.stable_sub[key] = value

    def alter_sub_tb(self, key, value):
        if key == "sqls":
            self.table_sub[key] = self.tb_sub_sql
        else:
            self.table_sub[key] = value
    # alter function ends

    # the following functions are for handling the sql lists
    def append_sql_stb(self, target, value):
        """for appending sql dict into specific sql list

        Args:
            target (str): the target append list
                format: 'fileType_tableType'
                fileType: query, sub
                tableType: table, stable
                unique: 'insert_stbs'
            value (dict): the sql dict going to be appended
        """
        if target == 'insert_stbs':
            self.stbs.append(value)
        elif target == 'query_table':
            self.tb_query_sql.append(value)
        elif target == 'query_stable':
            self.stb_query_sql.append(value)
        elif target == 'sub_table':
            self.tb_sub_sql.append(value)
        elif target == 'sub_stable':
            self.stb_sub_sql.append(value)

    def pop_sql_stb(self, target, index):
        """for poping a sql dict from specific sql list

        Args:
            target (str): the target append list
                format: 'fileType_tableType'
                fileType: query, sub
                tableType: table, stable
                unique: 'insert_stbs'
            index (int): the sql dict that is going to be popped
        """
        if target == 'insert_stbs':
            self.stbs.pop(index)
        elif target == 'query_table':
            self.tb_query_sql.pop(index)
        elif target == 'query_stable':
            self.stb_query_sql.pop(index)
        elif target == 'sub_table':
            self.tb_sub_sql.pop(index)
        elif target == 'sub_stable':
            self.stb_sub_sql.pop(index)
    # sql list modification function end

    # The following functions are get functions for different dicts
    def get_db(self):
        return self.db

    def get_stb(self):
        return self.stbs

    def get_insert_cfg(self):
        return self.insert_cfg

    def get_query_cfg(self):
        return self.query_cfg

    def get_tb_query(self):
        return self.table_query

    def get_stb_query(self):
        return self.stable_query

    def get_sub_cfg(self):
        return self.sub_cfg

    def get_tb_sub(self):
        return self.table_sub

    def get_stb_sub(self):
        return self.stable_sub

    def get_sql(self, target):
        """general get function for all sql lists

        Args:
            target (str): the sql list want to get
                format: 'fileType_tableType'
                fileType: query, sub
                tableType: table, stable
                unique: 'insert_stbs'
        """
        if target == 'query_table':
            return self.tb_query_sql
        elif target == 'query_stable':
            return self.stb_query_sql
        elif target == 'sub_table':
            return self.tb_sub_sql
        elif target == 'sub_stable':
            return self.stb_sub_sql

    def get_template(self, target):
        """general get function for the default sql template

        Args:
            target (str): the sql list want to get
                format: 'fileType_tableType'
                fileType: query, sub
                tableType: table, stable
                unique: 'insert_stbs'
        """
        if target == 'insert_stbs':
            return self.stb_template
        elif target == 'query_table':
            return self.tb_query_sql_template
        elif target == 'query_stable':
            return self.stb_query_sql_template
        elif target == 'sub_table':
            return self.tb_sub_sql_template
        elif target == 'sub_stable':
            return self.stb_sub_sql_template
        else:
            print(f'did not find {target}')

    # the folloing are the file generation functions
    """defalut document:
        generator functio for generating taosdemo json file
        will assemble the dicts and dump the final json

        Args:
            pathName (str): the directory wanting the json file to be
            fileName (str): the name suffix of the json file
        Returns:
            str: [pathName]/[filetype]_[filName].json
    """

    def generate_insert_cfg(self, pathName, fileName):
        cfgFileName = f'{pathName}/insert_{fileName}.json'
        self.alter_insert_cfg('databases', None)
        with open(cfgFileName, 'w') as file:
            json.dump(self.insert_cfg, file)
        return cfgFileName

    def generate_query_cfg(self, pathName, fileName):
        cfgFileName = f'{pathName}/query_{fileName}.json'
        self.alter_query_tb('sqls', None)
        self.alter_query_stb('sqls', None)
        self.alter_query_cfg('specified_table_query', None)
        self.alter_query_cfg('super_table_query', None)
        with open(cfgFileName, 'w') as file:
            json.dump(self.query_cfg, file)
        return cfgFileName

    def generate_subscribe_cfg(self, pathName, fileName):
        cfgFileName = f'{pathName}/subscribe_{fileName}.json'
        self.alter_sub_tb('sqls', None)
        self.alter_sub_stb('sqls', None)
        self.alter_sub_cfg('specified_table_query', None)
        self.alter_sub_cfg('super_table_query', None)
        with open(cfgFileName, 'w') as file:
            json.dump(self.sub_cfg, file)
        return cfgFileName
    # file generation functions ends

    def drop_cfg_file(self, fileName):
        os.remove(f'{fileName}')


taosdemoCfg = TDTaosdemoCfg()
