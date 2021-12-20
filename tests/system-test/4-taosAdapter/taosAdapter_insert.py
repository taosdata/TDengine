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
import subprocess
import random
import inspect
import taos
import requests
import json
import traceback
import simplejson.errors
import csv

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *



class RestMsgInfo:
    def __init__(self, base_url,
                 port=6041,
                 api_url="/rest/sql",
                 header={'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='}
                 ):
        self.base_url = base_url
        self.port = port
        self.api_url = api_url
        self.header = header
        self.full_url = f"http://{base_url}:{port}{api_url}"



class TDTestCase:
    def __init__(self):
        self.base_url   = "127.0.0.1"
        self.dbname     = "db"
        self.precision  = "ms"
        self.tbnum      = 0
        self.data_row   = 0
        self.basetime   = 0
        self.file       = ""

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def caseDescription(self):
        '''
        case1 <cpwu>: create/alter/drop database/normal_table/child_table/stable \n
        case2 <cpwu>: insert into table multiple records \n
        case3 <cpwu>: insert multiple records into a given column \n
        case4 <cpwu>: insert multiple records into multiple tables \n
        case5 <cpwu>: automatically create a table when inserting, and specify a given tags column \n
        case6 <cpwu>: insert with files \n
        case7 <cpwu>: api_url test \n
        case8 <cpwu>: base_url test \n
        case9 <cpwu>: header test
        '''
        return

    def rest_test_table(self, dbname: str, tbnum: int) -> None :

        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database  if not exists {dbname} keep 3650 precision '{self.precision}' ")
        tdSql.execute(f"use {dbname}")

        tdSql.execute(
            f'''
            create stable {dbname}.stb1 (
                ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool, 
                c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)
                ) 
            tags(
                tag1 int, tag2 float, tag3 timestamp, tag4 binary(16), tag5 double, tag6 bool, 
                tag7 bigint, tag8 smallint, tag9 tinyint, tag10 nchar(16)
                )
            '''
        )
        tdSql.execute(
            f"create stable {dbname}.stb2 (ts timestamp, c1 int) tags(ttag1 int)"
        )

        for i in range(tbnum):
            tdSql.execute(
                f'''
                create table {dbname}.t{i} using {dbname}.stb1 
                tags(
                {i}, {i}, {1639032680000+i*10}, 'binary_{i}',{i},{random.choice([0, 1])}, {i},{i%32767},{i%127},'nchar_{i}'
                )'''
            )
            tdSql.execute(f"create table {dbname}.tt{i} using {dbname}.stb2 tags({i})")

        tdSql.execute(
            f"create table {dbname}.nt1 (ts timestamp, c1 int, c2 float)"
        )
        tdSql.execute(
            f"create table {dbname}.nt2 (ts timestamp, c1 int, c2 float)"
        )
        pass

    def rest_test_data(self, tbnum:int, data_row:int, basetime:int) -> None :
        for i in range(tbnum):
            for j in range(data_row):
                tdSql.execute(
                    f"insert into t{i} values ("
                    f"{basetime + (j+1)*10}, {random.randint(-200, -1)}, {random.uniform(200, -1)}, {basetime + random.randint(-200, -1)}, "
                    f"'binary_{j}', {random.uniform(-200, -1)}, {random.choice([0,1])}, {random.randint(-200,-1)}, "
                    f"{random.randint(-200, -1)}, {random.randint(-127, -1)}, 'nchar_{j}' )"
                )
                tdSql.execute(
                    f"insert into tt{i} values ( {basetime-(j+1) * 10}, {random.randint(1, 200)} )"
                )

    def check_err_case(self,query_msg: RestMsgInfo, data):
        url, header = query_msg.full_url, query_msg.header
        try:
            conn = requests.post(url=url, data=data, headers=header)
            resp_code = conn.status_code
            resp = conn.json()
            if resp_code != 200:
                tdLog.success(f"expect error occured, usrl: {url}, sql: {data}, error code is :{resp_code}")
                return
            status = resp["status"]
            desc = resp["desc"]
            if resp_code == 200 and status == "error":
                tdLog.success(f"expect error occured, usrl: {url}, sql: {data}, error is :{desc}")
                return
            else:
                tdLog.exit(f"expect error not occured")
        except requests.exceptions.InvalidHeader as e:
            print(f"expect error occured, request header error, header: {header}, error: {e}")
        except requests.exceptions.InvalidURL as e:
            print(f"expect error occured, request url error, url: {url}, error: {e}")
        except requests.exceptions.ConnectionError as e:
            print(f"expect error occured, request connection error,url: {url}, error: {e}")
        except simplejson.errors.JSONDecodeError as e:
            print(f"expect error occured, request json error,url: {url}, header: {header}, error: {e}")
        except Exception as e:
            print(f"expect error occured, url: {url}, header: {header}, {traceback.print_exc()}")
        # finally:
            # conn.close()

        pass

    def check_err_sql_case(self,query_msg: RestMsgInfo, data):
        url, header = query_msg.full_url, query_msg.header
        conn = requests.post(url=url, data=data, headers=header)
        resp_code = conn.status_code
        resp = conn.json()
        try:
            status = resp["status"]
            desc = resp["desc"]
            if resp_code == 200 and status == "error":
                tdLog.success(f"expect error occured, url: {url}, error is :{desc}")
                return
            else:
                tdLog.exit(f"expect error not occured")
        except Exception as e:
            tdLog.debug(f"url: {url}, resp: {resp} ")
            traceback.print_exc()
            raise e

    def check_current_case(self,query_msg: RestMsgInfo, data):
        url, header = query_msg.full_url, query_msg.header
        conn = requests.post(url=url, data=data, headers=header)
        resp_code = conn.status_code
        resp = conn.json()
        try:
            status = resp["status"]
            if resp_code == 200 and status == "succ":
                tdLog.success(f"restfull run success! url:{url}")
            else:
                tdLog.exit(f"restful api test failed, url:{url}, sql: {data}, resp: {resp}")
        except:
            tdLog.debug(f"resp_code: {resp_code}, url: {url}, resp:{resp}")
            traceback.print_exc()
            raise
        pass

    def check_case_res_data(self, query_msg: RestMsgInfo, data):
        url, header, api = query_msg.full_url, query_msg.header, query_msg.api_url
        try:
            ts_col = []
            stb_list = [f"describe {self.dbname}.stb1", f"describe {self.dbname}.stb2"]
            for stb in stb_list:
                conn = requests.post(url=url, data=stb, headers=header)
                resp = conn.json()
                for col in resp["data"]:
                    if "TIMESTAMP" == col[1]:
                        ts_col.append(col[0])

            check_column = []
            conn = requests.post(url=url, data=data, headers=header)
            resp = conn.json()
            if len(resp["data"]) < 1:
                return
            for meta in resp["column_meta"]:
                if meta[0] in ts_col:
                    check_column.append(meta[0])
            if len(check_column) < 1:
                return

            if self.precision == "ms" and (api == "/rest/sql" or api == f"/rest/sql/{self.dbname}"):
                return
        except:
            raise

        pass

    def db_tb_case_current(self):
        # when version > 2.6, add the follow case:
        #   f"alter table {self.dbname}.tb1 add column c2 float",
        #   f"alter table {self.dbname}.tb1 drop column c2 ",
        #   f"alter table {self.dbname}.tb1 add column c2 float ; alter table {self.dbname}.tb1 drop column c2 ",

        case_list = [
            "create database if not exists db",
            "create database if not exists db",
            "create database if not exists db1",
            "alter database db1 comp 2",
            "alter database db1 keep 36500",
            "drop database if exists db1",
            "drop database if exists db1",
            "drop database if exists db",
            f"create database if not exists {self.dbname}",
            f"create table if not exists {self.dbname}.tb1 (ts timestamp , c1 int)",
            f"create table if not exists {self.dbname}.tb1 (ts timestamp , c1 float)",
            f"create table if not exists {self.dbname}.stb1 (ts timestamp , c1 int) tags(tag1 int )",
            f"create table if not exists {self.dbname}.stb1 (ts timestamp , c1 float) tags(tag2 int )",
            f"create table if not exists {self.dbname}.stb2 (ts timestamp , c1 int) tags(tag1 int )",
            f"create table if not exists {self.dbname}.stb3 (ts timestamp , c1 int) tags(tag1 int )",
            f"create table if not exists {self.dbname}.tb2 using {self.dbname}.stb2 tags(2)",
            f"create table if not exists {self.dbname}.tb3 using {self.dbname}.stb2 tags(2)",
            f"drop table if exists {self.dbname}.tb2",
            f"drop table if exists {self.dbname}.tb2",
            f"drop table if exists {self.dbname}.stb2",
            f"drop table if exists {self.dbname}.stb2",
            f"drop table if exists {self.dbname}.t3",
            f"drop table if exists {self.dbname}.stb3",
        ]
        return case_list

    def db_tb_case_err(self):
        case_list = [
            "create database if exists db",
            f"drop database if not exists db",
            f"drop database  db3",
            f"create table if exists {self.dbname}.t1 ",
            f"create table if exists {self.dbname}.stb1 ",
            f"drop table if not exists {self.dbname}.stb1 ",
            f"drop table {self.dbname}.stb4 ",
            f"create table if not exists {self.dbname}.stb2 (c1 int, c2 timestamp ) tags(tag1 int)",
            f"create table if  exists {self.dbname}.stb3 (ts timestamp ,c1 int) ",
            f"create table if  exists {self.dbname}.t2 (c1 int) "
        ]
        return case_list

    def data_case_current(self, tbnum:int, data_row:int, basetime: int, file:str):
        case_list = []
        body_list = []
        row_times = data_row // 100
        row_alone = data_row % 100
        for i in range(row_times):
            body = ""
            for j in range(100):
                 body += f"(\
                 {basetime + (j+1)*10+ i*1000}, {random.randint(-200, -1)}, {random.uniform(200, -1)}, {basetime + random.randint(-200, -1)},\
                 'binary_{j}', {random.uniform(-200, -1)}, {random.choice([0,1])}, {random.randint(-200,-1)}, \
                 {random.randint(-200, -1)}, {random.randint(-127, -1)}, 'nchar_{j}' \
                 )"
            body_list.append(body)

        if  row_alone != 0:
            body = ""
            for j in range(row_alone):
                body += f"( \
                {basetime + (j+1)*10+ row_times*1000}, {random.randint(-200, -1)}, {random.uniform(200, -1)}, \
                {basetime + random.randint(-200, -1)},'binary_{j}', {random.uniform(-200, -1)}, {random.choice([0,1])}, \
                {random.randint(-200,-1)},{random.randint(-200, -1)}, {random.randint(-127, -1)}, 'nchar_{j}'  \
                )"
            body_list.append(body)

        for i in range(tbnum):
            pre_insert = f"insert into {self.dbname}.t{i} values "
            for value_body in body_list:
                insert_sql = pre_insert + value_body
                case_list.append(insert_sql)

        case_list.append(f'insert into {self.dbname}.nt1 values (now, 1, 1.0)')
        case_list.append(f'insert into {self.dbname}.nt1 values ({basetime + 10}, 2, 2.0)')
        case_list.append(f'insert into {self.dbname}.nt1 values ({basetime + 20}, 3, 3.0) {self.dbname}.nt2 values (now, 1, 1.0)')
        case_list.append(f'insert into {self.dbname}.nt1 (ts, c2, c1) values ({basetime + 20}, 4.0, 4) ')
        # exchange column order
        case_list.append(f'insert into {self.dbname}.ct1 using {self.dbname}.stb1 (tag1) tags(1) (ts, c1) values (now, 1)')

        # insert with file
        if not os.path.isfile(file):
            with open(file=file, mode="w", encoding="utf-8", newline="") as f:
                for j in range(data_row):
                    writer = csv.writer(f)
                    data_line =  [
                        basetime - (j + 1) * 10, random.randint(-200, -1), random.uniform(200, -1),
                        basetime + random.randint(-200, -1), f'"binary_{j}"', random.uniform(-200, -1),
                        random.choice([0, 1]), random.randint(-200, -1), random.randint(-200, -1),
                        random.randint(-127, -1), f'"nchar_{j}"'
                    ]
                    writer.writerow(data_line)

        case_list.append(f"insert into {self.dbname}.ct1 file {file}")

        return case_list
        pass

    def data_case_err(self):
        case_list = []
        nowtime = int(round(time.time()*1000))
        bigger_insert_sql = f"insert into {self.dbname}.nt1 values"
        for i in range(40000):
            bigger_insert_sql += f"({nowtime-i*10}, {i}, {i*1.0})"
        case_list.append(bigger_insert_sql)

        nodata_sql = f"insert into {self.dbname}.nt1 values()"
        case_list.append(nodata_sql)

        less_data_sql = f"insert into {self.dbname}.nt1 values(now)"
        case_list.append(less_data_sql)

        errtype_data_sql = f"insert into {self.dbname}.nt1 values(now+2, 1.0, 'binary_2')"
        case_list.append(errtype_data_sql)

        # insert into super table directly
        insert_super_data_sql = f"insert into {self.dbname}.stb1 values(now+3, 1, 1.0)"
        case_list.append(insert_super_data_sql)

        return case_list

    def port_case_current(self):
        case_list = [6041]
        return case_list

    def port_case_err(self):
        case_list = [
            6030,
            6051,
            666666666,
            None,
            "abcd"
        ]
        return case_list

    def api_case_current(self):
        case_List = [
            "/rest/sql",
            f"/rest/sql/{self.dbname}",
            "/rest/sqlt",
            f"/rest/sqlt/{self.dbname}",
            "/rest/sqlutc",
            f"/rest/sqlutc/{self.dbname}"
        ]
        return case_List

    def api_case_err(self):
        case_list = [
            "",
            "/rest1/sql",
            "/rest/sqlsqltsqlutc",
            1,
            ["/rest", "/sql"],
            "/influxdb/v1/write",
            "/opentsdb/v1/put/json/db",
            "/opentsdb/v1/put/telnet/db",
            "/rest*",
            "*"
        ]
        return case_list

    def header_case_current(self):
        case_list = [
            {'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='},
            {'Authorization': 'Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04'}
        ]
        return case_list

    def header_case_err(self):
        case_list = [
            {'Authorization': 'Basic '},
            {'Authorization': 'Taosd /root/taosdata'},
            {'Authorization': True}
        ]
        return case_list

    def run_case_api_err(self):
        err_cases = self.api_case_err()
        count = 0
        data = "create database if not exists db"
        for case in err_cases:
            print(f"err api case{count}: ", end="")
            query_msg = RestMsgInfo(base_url=self.base_url, api_url=case)
            self.check_err_case(query_msg=query_msg, data=data)
            count += 1
        pass

    def run_case_port_err(self):
        err_cases = self.port_case_err()
        count = 0
        data = "create database if not exists db"
        for case in err_cases:
            print(f"err port case{count}: ", end="")
            query_msg = RestMsgInfo(base_url=self.base_url, port=case)
            self.check_err_case(query_msg=query_msg, data=data)
            count += 1
        pass

    def run_case_header_err(self):
        err_cases = self.header_case_err()
        count = 0
        data = "create database if not exists db"
        for case in err_cases:
            print(f"err header case{count}: ", end="")
            query_msg = RestMsgInfo(base_url=self.base_url, header=case)
            self.check_err_case(query_msg=query_msg, data=data)
            count += 1
        pass

    def run_case_db_tb_err(self):
        err_cases = self.db_tb_case_err()
        count = 0
        query_msg = RestMsgInfo(base_url=self.base_url)
        for case in err_cases:
            print(f"err create db/tb case{count}: ", end="")
            self.check_err_sql_case(query_msg=query_msg, data=case)
            count += 1
        pass

    def run_case_data_err(self):
        err_cases = self.data_case_err()
        count = 0
        tdSql.execute(f"drop database if exists {self.dbname}")
        tdSql.execute(f"create database  if not exists {self.dbname} keep 3650 precision '{self.precision}' ")
        tdSql.execute(f"use {self.dbname}")
        tdSql.execute(
            f'''
            create stable {self.dbname}.stb1 (
                ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool, 
                c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)
                ) 
            tags(
                tag1 int, tag2 float, tag3 timestamp, tag4 binary(16), tag5 double, tag6 bool, 
                tag7 bigint, tag8 smallint, tag9 tinyint, tag10 nchar(16)
                )
            '''
        )

        query_msg = RestMsgInfo(base_url=self.base_url)
        for case in err_cases:
            print(f"err insert data case{count}: ", end="")
            self.check_err_sql_case(query_msg=query_msg, data=case)
            count += 1

        tdSql.execute(f"drop database if exists {self.dbname}")
        pass

    def run_case_port_current(self):
        current_cases = self.port_case_current()
        count = 0
        data = "create database if not exists db"
        for case in current_cases:
            print(f"current port case{count}: ", end="")
            query_msg = RestMsgInfo(base_url=self.base_url, port=case)
            self.check_current_case(query_msg=query_msg, data=data)
            count += 1
        pass

    def run_case_api_current(self):
        current_cases = self.api_case_current()
        count = 0
        data = "create database if not exists db"
        for case in current_cases:
            print(f"current api case{count}: ", end="")
            query_msg = RestMsgInfo(base_url=self.base_url, api_url=case)
            self.check_current_case(query_msg=query_msg, data=data)
            count += 1
        pass

    def run_case_header_current(self):
        current_cases = self.header_case_current()
        count = 0
        data = "create database if not exists db"
        for case in current_cases:
            print(f"current header case{count}: ", end="")
            query_msg = RestMsgInfo(base_url=self.base_url, header=case)
            self.check_current_case(query_msg=query_msg, data=data)
            count += 1
        pass

    def run_case_db_tb_current(self):
        current_cases = self.db_tb_case_current()
        count = 0
        for case in current_cases:
            print(f"current insert db/tb case{count}: ", end="")
            for api in ["/rest/sql", "/rest/sqlt", "/rest/sqlutc"]:
                query_msg = RestMsgInfo(base_url=self.base_url, api_url=api)
                self.check_current_case(query_msg=query_msg, data=case)
            count += 1
        pass

    def run_case_data_current(self):
        self.rest_test_table(dbname=self.dbname, tbnum=self.tbnum)
        current_cases = self.data_case_current(tbnum=self.tbnum, data_row=self.data_row, basetime=self.basetime, file=self.file)
        count = 0
        print(current_cases[12])
        api_cases = self.api_case_current()
        for case in current_cases:
            print(f"current insert data case{count}: ", end="")
            for api in api_cases:
                query_msg = RestMsgInfo(base_url=self.base_url, api_url=api)
                self.check_current_case(query_msg=query_msg, data=case)
            count += 1
        pass

    def run_case_err(self):
        self.run_case_api_err()
        self.run_case_port_err()
        self.run_case_header_err()
        self.run_case_db_tb_err()
        self.run_case_data_err()
        pass

    def run_case_current(self):
        self.run_case_api_current()
        self.run_case_port_current()
        self.run_case_header_current()
        self.run_case_db_tb_current()
        self.run_case_data_current()
        pass

    def run_all_case(self):
        self.run_case_err()
        self.run_case_current()
        pass

    def set_default_args(self):
        nowtime = int(round(time.time() * 1000))
        url = "127.0.0.1"
        per_table_rows = 100
        tbnum = 10
        database_name = "db"
        precision ="ms"
        clear_data = True
        insert_case_filename = "data_insert.csv"
        config_default = {
            "base_url"      : url,
            "precision"     : precision,
            "clear_data"    : clear_data,
            "database_name" : database_name,
            "tbnum"         : tbnum,
            "data_row"      : per_table_rows,
            "case_file"     : insert_case_filename,
            "basetime"      : nowtime,
            "all_case"      : False,
            "all_err"       : False,
            "all_current"   : True,
            "err_case"      : {
                "port_err"          : True,
                "api_err"           : True,
                "header_err"        : True,
                "db_tb_err"         : True,
                "data_err"          : True,
            },
            "current_case"  : {
                "port_current"      : True,
                "api_current"       : True,
                "header_current"    : True,
                "db_tb_current"     : True,
                "data_current"      : True,
            }
        }

        config_file_name = f"{os.path.dirname(os.path.abspath(__file__))}/rest_insert_config.json"
        with open(config_file_name, "w") as f:
            json.dump(config_default, f)
        return config_file_name

    def run(self):
        config_file = f"{os.path.dirname(os.path.abspath(__file__))}/rest_insert_config.json"
        if not os.path.isfile(config_file):
            config_file = self.set_default_args()

        with open(config_file, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        self.tbnum      = cfg["tbnum"]
        self.data_row   = cfg["data_row"]
        self.basetime   = cfg["basetime"]
        self.dbname     = cfg["database_name"]
        self.base_url   = cfg["base_url"]
        self.precision  = cfg["precision"]
        self.file       = cfg["case_file"]
        clear_data      = True      if cfg["clear_data"]    else False

        if clear_data:
            self.rest_test_table(dbname=self.dbname, tbnum=self.tbnum)

        run_all_case            = True  if cfg["all_case"]                          else False
        run_all_err_case        = True  if cfg["all_err"]                           else False
        run_all_current_case    = True  if cfg["all_current"]                       else False
        run_port_err_case       = True  if cfg["err_case"]["port_err"]              else False
        run_api_err_case        = True  if cfg["err_case"]["api_err"]               else False
        run_header_err_case     = True  if cfg["err_case"]["header_err"]            else False
        run_db_tb_err_case      = True  if cfg["err_case"]["db_tb_err"]             else False
        run_data_err_case       = True  if cfg["err_case"]["data_err"]              else False
        run_port_current_case   = True  if cfg["current_case"]["port_current"]      else False
        run_api_current_case    = True  if cfg["current_case"]["api_current"]       else False
        run_header_current_case = True  if cfg["current_case"]["header_current"]    else False
        run_db_tb_current_case  = True  if cfg["current_case"]["db_tb_current"]     else False
        run_data_current_case   = True  if cfg["current_case"]["data_current"]      else False

        print("run_all_case:" ,run_all_case)
        print("run_all_err_case:" ,run_all_err_case)
        print("run_all_current_case:" ,run_all_current_case)
        print("run_port_err_case:" ,run_port_err_case)
        print("run_api_err_case:" ,run_api_err_case)
        print("run_header_err_case:" ,run_header_err_case)
        print("run_db_tb_err_case:" ,run_db_tb_err_case)
        print("run_data_err_case:" ,run_data_err_case)
        print("run_port_current_case:" ,run_port_current_case)
        print("run_api_current_case:" ,run_api_current_case)
        print("run_header_current_case:" ,run_header_current_case)
        print("run_db_tb_current_case:" ,run_db_tb_current_case)
        print("run_data_current_case:" ,run_data_current_case)


        if  not (run_all_err_case | run_all_current_case | run_port_err_case | run_api_err_case | run_header_err_case |
                 run_db_tb_err_case | run_data_err_case | run_port_current_case | run_api_current_case |
                 run_header_current_case | run_db_tb_current_case | run_data_current_case ):
            run_all_case = True
        if run_all_err_case & run_all_current_case:
            run_all_case = True

        if run_all_case:
            self.run_all_case()
            return
        if run_all_err_case :
            self.run_case_err()
            return
        if run_all_current_case:
            self.run_case_current()
            return
        if run_port_err_case:
            self.run_case_port_err()
        if run_api_err_case:
            self.run_case_api_err()
        if run_header_err_case:
            self.run_case_header_err()
        if run_db_tb_err_case:
            self.run_case_db_tb_err()
        if run_data_err_case:
            self.run_case_data_err()
        if run_port_current_case:
            self.run_case_port_current()
        if run_api_current_case:
            self.run_case_api_current()
        if run_header_current_case:
            self.run_case_header_current()
        if run_db_tb_current_case:
            self.run_case_db_tb_current()
        if run_data_current_case:
            self.run_case_data_current()
        pass

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
