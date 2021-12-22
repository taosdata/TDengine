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
import math

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
from collections import defaultdict



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
        self.base_url  = "127.0.0.1"
        self.dbname    = "db"
        self.precision = "ms"

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def caseDescription(self):
        '''
        case1 <cpwu>: specified SQL
        case2 <cpwu>: select sql,include stable 、child table and normal table, include correct SQL and invalid SQL \n
        case3 <cpwu>: port test \n
        case4 <cpwu>: api_url test \n
        case5 <cpwu>: base_url test \n
        case6 <cpwu>: header test \n
        case7 <cpwu>: big data test
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
                tags({i}, {i}, {1639032680000+i*10}, 'binary_{i}',{i},{random.choice([0, 1])}, {i},{i%32767},{i%127},'nchar_{i}')
                '''
            )
            tdSql.execute(f"create table {dbname}.tt{i} using {dbname}.stb2 tags({i})")
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
                print(f"expect error occured, url: {url}, sql: {data}, error code is :{resp_code}")
                return
            status = resp["status"]
            desc = resp["desc"]
            if resp_code == 200 and status == "error":
                print(f"expect error occured, url: {url}, sql: {data}, error is :{desc}")
                return
            else:
                tdLog.exit(f"expect error not occured")
        except requests.exceptions.InvalidHeader as e:
            tdLog.success(f"expect error occured, request header error, header: {header}, error: {e}")
        except requests.exceptions.InvalidURL as e:
            tdLog.success(f"expect error occured, request url error, url: {url}, error: {e}")
        except requests.exceptions.ConnectionError as e:
            tdLog.success(f"expect error occured, request connection error,url: {url}, error: {e}")
        except simplejson.errors.JSONDecodeError as e:
            tdLog.success(f"expect error occured, request json error,url: {url}, header: {header}, error: {e}")
        except Exception as e:
            tdLog.success(f"expect error occured, url: {url}, header: {header}, {traceback.print_exc()}")
        # finally:
            # conn.close()

        pass

    def check_err_sql_case(self,query_msg: RestMsgInfo, data):
        url, header = query_msg.full_url, query_msg.header
        try:
            conn = requests.post(url=url, data=data, headers=header)
            resp_code = conn.status_code
            resp = conn.json()
            status = resp["status"]
            desc = resp["desc"]
            if resp_code == 200 and status == "error":
                tdLog.success(f"expect error occured, url: {url}, sql: {data}, error is :{desc}")
                return
            else:
                tdLog.exit(f"expect error not occured")
        except Exception as e:
            traceback.print_exc()
            raise e

    def check_current_case(self,query_msg: RestMsgInfo, data):
        url, header = query_msg.full_url, query_msg.header
        conn = requests.post(url=url, data=data, headers=header)
        try:
            resp_code = conn.status_code
            resp = conn.json()
            status = resp["status"]
            if resp_code == 200 and status == "succ":
                tdLog.printNoPrefix(f"restfull run success! url:{url}, sql: {data}")
            else:
                tdLog.exit(f"restful api test failed, url:{url}, sql: {data}")
        except:
            tdLog.debug(f"resp_code: {resp_code}, url: {url}")
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

            index_dict = defaultdict(int)
            conn = requests.post(url=url, data=data, headers=header)
            resp = conn.json()
            if resp["data"] is None:
                return
            for index, meta in enumerate(resp["column_meta"]):
                if meta[0] in ts_col:
                    index_dict[meta[0]] = index
            if len(index_dict) < 1:
                return

            if self.precision == "ms" and (api == "/rest/sql" or api == f"/rest/sql/{self.dbname}"):
                for col_name, col_index in index_dict.items():
                    for res_data in resp["data"]:
                        if len(res_data[col_index]) !=23:
                            print(res_data)
                            tdLog.exit(f"restful timestamp column err, url:{url}, sql: {data}，result is: {res_data[col_index]}")
                return
            if self.precision == "ms" and (api == "/rest/sqlt" or api == f"/rest/sqlt/{self.dbname}"):
                for col_name, col_index in index_dict.items():
                    for res_data in resp["data"]:
                        if not isinstance(res_data[col_index], int) or round(math.log10(res_data[col_index])) != 12:
                            print(res_data)
                            tdLog.exit(
                                f"restful timestamp column err, url:{url}, sql: {data}，result is: {res_data[col_index]}"
                            )
                return
            if self.precision == "ms" and (api == "/rest/sqlutc" or api == f"/rest/sqlutc/{self.dbname}"):
                for col_name, col_index in index_dict.items():
                    for res_data in resp["data"]:
                        if len(res_data[col_index]) != 29 and len(res_data[col_index]) != 28 and len(res_data[col_index]) != 27 and len(res_data[col_index]) != 25:
                            print(res_data)
                            tdLog.exit(
                                f"restful timestamp column err, url:{url}, sql: {data}，result is: {res_data[col_index]}, length is: {len(res_data[col_index])}"
                            )
                return
            if self.precision == "us" and (api == "/rest/sql" or api == f"/rest/sql/{self.dbname}"):
                for col_name, col_index in index_dict.items():
                    for res_data in resp["data"]:
                        if len(res_data[col_index]) !=26:
                            print(res_data)
                            tdLog.exit(f"restful timestamp column err, url:{url}, sql: {data}，result is: {res_data[col_index]}")
                return

            if self.precision == "us" and (api == "/rest/sqlt" or api == f"/rest/sqlt/{self.dbname}"):
                for col_name, col_index in index_dict.items():
                    for res_data in resp["data"]:
                        if not isinstance(res_data[col_index], int) or round(math.log10(res_data[col_index])) != 15:
                            print(res_data)
                            tdLog.exit(
                                f"restful timestamp column err, url:{url}, sql: {data}，result is: {res_data[col_index]}"
                            )
                return

            if self.precision == "us" and (api == "/rest/sqlutc" or api == f"/rest/sqlutc/{self.dbname}"):
                for col_name, col_index in index_dict.items():
                    for res_data in resp["data"]:
                        if len(res_data[col_index]) != 32 and len(res_data[col_index]) != 31 and len(res_data[col_index]) != 30 and len(res_data[col_index]) != 28:
                            print(res_data)
                            tdLog.exit(
                                f"restful timestamp column err, url:{url}, sql: {data}，result is: {res_data[col_index]}"
                            )
                return
            if self.precision == "ns" and (api == "/rest/sql" or api == f"/rest/sql/{self.dbname}"):
                for col_name, col_index in index_dict.items():
                    for res_data in resp["data"]:
                        if len(res_data[col_index]) !=29:
                            print(res_data)
                            tdLog.exit(f"restful timestamp column err, url:{url}, sql: {data}，result is: {res_data[col_index]}")
                return

            if self.precision == "ns" and (api == "/rest/sqlt" or api == f"/rest/sqlt/{self.dbname}"):
                for col_name, col_index in index_dict.items():
                    for res_data in resp["data"]:
                        if not isinstance(res_data[col_index], int) or round(math.log10(res_data[col_index])) != 18:
                            print(res_data)
                            tdLog.exit(
                                f"restful timestamp column err, url:{url}, sql: {data}，result is: {res_data[col_index]}"
                            )
                return

            if self.precision == "ns" and (api == "/rest/sqlutc" or api == f"/rest/sqlutc/{self.dbname}"):
                for col_name, col_index in index_dict.items():
                    for res_data in resp["data"]:
                        if len(res_data[col_index]) != 35 and len(res_data[col_index]) != 34 and len(res_data[col_index]) != 33 and len(res_data[col_index]) != 31:
                            print(res_data)
                            tdLog.exit(
                                f"restful timestamp column err, url:{url}, sql: {data}，result is: {res_data[col_index]}"
                            )
                return

        except:
            traceback.print_exc()
            raise

        pass

    def sql_case_current(self):
        case_list = [
            "show databases",
            f"show {self.dbname}.stables",
            f"show {self.dbname}.tables",
            "select server_status()",
            "select client_version()",
            "select server_version()",
            "select database()",
            f"show create database {self.dbname}",
            f"show create stable {self.dbname}.stb1",
            f"select * from {self.dbname}.stb1",
            f"select ts from {self.dbname}.stb1",
            f"select _c0 from {self.dbname}.stb1",
            f"select c1 from {self.dbname}.stb1",
            f"select c2 from {self.dbname}.stb1",
            f"select c3 from {self.dbname}.stb1",
            f"select c4 from {self.dbname}.stb1",
            f"select c5 from {self.dbname}.stb1",
            f"select c6 from {self.dbname}.stb1",
            f"select c7 from {self.dbname}.stb1",
            f"select c8 from {self.dbname}.stb1",
            f"select c9 from {self.dbname}.stb1",
            f"select c10 from {self.dbname}.stb1",
            f"select tbname from {self.dbname}.stb1",
            f"select tag1 from {self.dbname}.stb1",
            f"select tag2 from {self.dbname}.stb1",
            f"select tag3 from {self.dbname}.stb1",
            f"select tag4 from {self.dbname}.stb1",
            f"select tag5 from {self.dbname}.stb1",
            f"select tag6 from {self.dbname}.stb1",
            f"select tag7 from {self.dbname}.stb1",
            f"select tag8 from {self.dbname}.stb1",
            f"select tag9 from {self.dbname}.stb1",
            f"select tag10 from {self.dbname}.stb1",
            f"select count(*) from {self.dbname}.stb1",
            f"select count(c1) from {self.dbname}.stb1",
            f"select avg(c1) from {self.dbname}.stb1",
            f"select twa(c1) from {self.dbname}.stb1 group by tbname",
            f"select sum(c1) from {self.dbname}.stb1",
            f"select stddev(c1) from {self.dbname}.stb1",
            f"select min(c1) from {self.dbname}.stb1",
            f"select max(c1) from {self.dbname}.stb1",
            f"select first(c1) from {self.dbname}.stb1",
            f"select first(*) from {self.dbname}.stb1",
            f"select last(c1) from {self.dbname}.stb1",
            f"select last(*) from {self.dbname}.stb1",
            f"select top(c1, 3) from {self.dbname}.stb1",
            f"select bottom(c1, 3) from {self.dbname}.stb1",
            f"select apercentile(c1, 50, 't-digest') from {self.dbname}.stb1",
            f"select last_row(c1) from {self.dbname}.stb1",
            f"select last_row(*) from {self.dbname}.stb1",
            f"select interp(c1) from {self.dbname}.stb1 where ts=0  group by tbname",
            f"select interp(c1) from {self.dbname}.stb1 where ts=0 fill(next)  group by tbname",
            f"select interp(c1) from {self.dbname}.stb1 where ts>0 and ts <100000000 every(5s)  group by tbname",
            f"select diff(c1) from {self.dbname}.stb1 group by tbname",
            f"select derivative(c1, 10m, 0) from {self.dbname}.stb1 group by tbname",
            f"select derivative(c1, 10m, 1) from {self.dbname}.stb1 group by tbname",
            f"select spread(c1) from {self.dbname}.stb1",
            f"select ceil(c1) from {self.dbname}.stb1",
            f"select floor(c1) from {self.dbname}.stb1",
            f"select round(c1) from {self.dbname}.stb1",
            f"select c1*2+2%c2-c2/2 from {self.dbname}.stb1",
            f"select max(c1) from {self.dbname}.stb1 where ts>'2021-12-05 18:25:41.136' and ts<'2021-12-05 18:25:44.13' interval(1s) sliding(500a) fill(NULL) group by tbname",
            f"select max(c1) from {self.dbname}.stb1 where (c1 >=0 and c1 <> 0 and c2 is not null or c1 < -1 or (c2 between 1 and 10) ) and tbname like 't_' ",
            f"select max(c1) from {self.dbname}.stb1 group by tbname order by ts desc slimit 2 soffset 2 limit 1 offset 0",
            f"select max(c1) from {self.dbname}.stb1 group by c6 order by ts desc slimit 1 soffset 1 limit 1 offset 0 ",
            f"select * from {self.dbname}.t1",
            f"select ts from {self.dbname}.t1",
            f"select _c0 from {self.dbname}.t1",
            f"select c1 from {self.dbname}.t1",
            f"select c2 from {self.dbname}.t1",
            f"select c3 from {self.dbname}.t1",
            f"select c4 from {self.dbname}.t1",
            f"select c5 from {self.dbname}.t1",
            f"select c6 from {self.dbname}.t1",
            f"select c7 from {self.dbname}.t1",
            f"select c8 from {self.dbname}.t1",
            f"select c9 from {self.dbname}.t1",
            f"select c10 from {self.dbname}.t1",
            f"select tbname from {self.dbname}.t1",
            f"select tag1 from {self.dbname}.t1",
            f"select tag2 from {self.dbname}.t1",
            f"select tag3 from {self.dbname}.t1",
            f"select tag4 from {self.dbname}.t1",
            f"select tag5 from {self.dbname}.t1",
            f"select tag6 from {self.dbname}.t1",
            f"select tag7 from {self.dbname}.t1",
            f"select tag8 from {self.dbname}.t1",
            f"select tag9 from {self.dbname}.t1",
            f"select tag10 from {self.dbname}.t1",
            f"select count(*) from {self.dbname}.t1",
            f"select count(c1) from {self.dbname}.t1",
            f"select avg(c1) from {self.dbname}.t1",
            f"select twa(c1) from {self.dbname}.t1",
            f"select sum(c1) from {self.dbname}.t1",
            f"select stddev(c1) from {self.dbname}.t1",
            f"select leastsquares(c1, 1, 1) from {self.dbname}.t1",
            f"select min(c1) from {self.dbname}.t1",
            f"select max(c1) from {self.dbname}.t1",
            f"select first(c1) from {self.dbname}.t1",
            f"select first(*) from {self.dbname}.t1",
            f"select last(c1) from {self.dbname}.t1",
            f"select last(*) from {self.dbname}.t1",
            f"select top(c1, 3) from {self.dbname}.t1",
            f"select bottom(c1, 3) from {self.dbname}.t1",
            f"select percentile(c1, 50) from {self.dbname}.t1",
            f"select apercentile(c1, 50, 't-digest') from {self.dbname}.t1",
            f"select last_row(c1) from {self.dbname}.t1",
            f"select last_row(*) from {self.dbname}.t1",
            f"select interp(c1) from {self.dbname}.t1 where ts=0 ",
            f"select interp(c1) from {self.dbname}.t1 where ts=0 fill(next)",
            f"select interp(c1) from {self.dbname}.t1 where ts>0 and ts <100000000 every(5s)",
            f"select diff(c1) from {self.dbname}.t1",
            f"select derivative(c1, 10m, 0) from {self.dbname}.t1",
            f"select derivative(c1, 10m, 1) from {self.dbname}.t1",
            f"select spread(c1) from {self.dbname}.t1",
            f"select ceil(c1) from {self.dbname}.t1",
            f"select floor(c1) from {self.dbname}.t1",
            f"select round(c1) from {self.dbname}.t1",
            f"select c1*2+2%c2-c2/2 from {self.dbname}.t1",
            f"select max(c1) from {self.dbname}.t1 where ts>'2021-12-05 18:25:41.136' and ts<'2021-12-05 18:25:44.13' interval(1s) sliding(500a) fill(NULL)",
            f"select max(c1) from {self.dbname}.t1 where (c1 >=0 and c1 <> 0 and c2 is not null or c1 < -1 or (c2 between 1 and 10) ) and c10 like 'nchar___1' ",
            f"select max(c1) from {self.dbname}.t1 group by c6 order by ts desc ",
            f"select stb1.c1, stb2.c1 from {self.dbname}.stb1 stb1, {self.dbname}.stb2 stb2 where stb1.ts=stb2.ts and stb1.tag1=stb2.ttag1",
            f"select t1.c1, t2.c1 from {self.dbname}.t1 t1, {self.dbname}.t2 t2 where t1.ts=t2.ts",
            f"select c1 from (select c2 c1 from {self.dbname}.stb1) ",
            f"select c1 from {self.dbname}.t1 union all select c1 from {self.dbname}.t2"
        ]
        return case_list

    def sql_case_err(self):
        case_list = [
            "show database",
            f"select percentile(c1, 50) from {self.dbname}.stb1 group by tbname",
            f"select leastsquares(c1, 1, 1) from {self.dbname}.stb1",
        ]
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
        data = "show databases"
        for case in err_cases:
            print(f"err api case{count}: ", end="")
            query_msg = RestMsgInfo(base_url=self.base_url, api_url=case)
            self.check_err_case(query_msg=query_msg, data=data)
            count += 1
        pass

    def run_case_port_err(self):
        err_cases = self.port_case_err()
        count = 0
        data = "show databases"
        for case in err_cases:
            print(f"err port case{count}: ", end="")
            query_msg = RestMsgInfo(base_url=self.base_url, port=case)
            self.check_err_case(query_msg=query_msg, data=data)
            count += 1
        pass

    def run_case_header_err(self):
        err_cases = self.header_case_err()
        count = 0
        data = "show databases"
        for case in err_cases:
            print(f"err header case{count}: ", end="")
            query_msg = RestMsgInfo(base_url=self.base_url, header=case)
            self.check_err_case(query_msg=query_msg, data=data)
            count += 1
        pass

    def run_case_sql_err(self):
        err_cases = self.sql_case_err()
        count = 0
        for case in err_cases:
            print(f"err sql case{count}: ", end="")
            query_msg = RestMsgInfo(base_url=self.base_url)
            self.check_err_sql_case(query_msg=query_msg, data=case)
            count += 1
        pass

    def run_case_port_current(self):
        current_cases = self.port_case_current()
        count = 0
        data = "show databases"
        for case in current_cases:
            print(f"current port case{count}: ", end="")
            query_msg = RestMsgInfo(base_url=self.base_url, port=case)
            self.check_current_case(query_msg=query_msg, data=data)
            count += 1
        pass

    def run_case_api_current(self):
        current_cases = self.api_case_current()
        count = 0
        data = "show databases"
        for case in current_cases:
            print(f"current api case{count}: ", end="")
            query_msg = RestMsgInfo(base_url=self.base_url, api_url=case)
            self.check_current_case(query_msg=query_msg, data=data)
            count += 1
        pass

    def run_case_header_current(self):
        current_cases = self.header_case_current()
        count = 0
        data = "show databases"
        for case in current_cases:
            print(f"current header case{count}: ", end="")
            query_msg = RestMsgInfo(base_url=self.base_url, header=case)
            self.check_current_case(query_msg=query_msg, data=data)
            count += 1
        pass

    def run_case_sql_current(self):
        current_cases = self.sql_case_current()
        count = 0
        api_cases = self.api_case_current()
        for case in current_cases:
            print(f"current sql case{count}: ", end="")
            for api in api_cases:
                query_msg = RestMsgInfo(base_url=self.base_url, api_url=api)
                self.check_current_case(query_msg=query_msg, data=case)
                self.check_case_res_data(query_msg=query_msg, data=case)
            count += 1
        pass

    def run_case_err(self):
        self.run_case_api_err()
        self.run_case_port_err()
        self.run_case_header_err()
        self.run_case_sql_err()
        pass

    def run_case_current(self):
        self.run_case_api_current()
        self.run_case_port_current()
        self.run_case_header_current()
        self.run_case_sql_current()
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
        config_default = {
            "base_url"     : url,
            "precision"    : precision,
            "clear_data"   : clear_data,
            "database_name": database_name,
            "tbnum"        : tbnum,
            "data_row"     : per_table_rows,
            "basetime"     : nowtime,
            "all_case"     : False,
            "all_err"      : False,
            "all_current"  : True,
            "err_case"     : {
                "port_err"       : True,
                "api_err"        : True,
                "header_err"     : True,
                "sql_err"        : True,
            },
            "current_case" : {
                "port_current"   : True,
                "api_current"    : True,
                "header_current" : True,
                "sql_current"    : True,
            }
        }

        config_file_name = f"{os.path.dirname(os.path.abspath(__file__))}/rest_query_config.json"
        with open(config_file_name, "w") as f:
            json.dump(config_default, f)
        return config_file_name

    def run(self):
        config_file = f"{os.path.dirname(os.path.abspath(__file__))}/rest_query_config.json"
        if not os.path.isfile(config_file):
            config_file = self.set_default_args()

        with open(config_file, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        tbnum           = cfg["tbnum"]
        data_row        = cfg["data_row"]
        basetime        = cfg["basetime"]
        self.dbname     = cfg["database_name"]
        self.base_url   = cfg["base_url"]
        self.precision  = cfg["precision"]
        clear_data      = True      if cfg["clear_data"]    else False

        if clear_data:
            self.rest_test_table(dbname=self.dbname, tbnum=tbnum)
            self.rest_test_data(tbnum=tbnum, data_row=data_row, basetime=basetime)

        run_all_case            = True  if cfg["all_case"]                          else False
        run_all_err_case        = True  if cfg["all_err"]                           else False
        run_all_current_case    = True  if cfg["all_current"]                       else False
        run_port_err_case       = True  if cfg["err_case"]["port_err"]              else False
        run_api_err_case        = True  if cfg["err_case"]["api_err"]               else False
        run_header_err_case     = True  if cfg["err_case"]["header_err"]            else False
        run_sql_err_case        = True  if cfg["err_case"]["sql_err"]               else False
        run_port_current_case   = True  if cfg["current_case"]["port_current"]      else False
        run_api_current_case    = True  if cfg["current_case"]["api_current"]       else False
        run_header_current_case = True  if cfg["current_case"]["header_current"]    else False
        run_sql_current_case    = True  if cfg["current_case"]["sql_current"]       else False

        if  not (run_all_err_case | run_all_current_case | run_port_err_case | run_api_err_case |
            run_header_err_case | run_sql_err_case | run_port_current_case | run_api_current_case
            | run_header_current_case | run_sql_current_case):
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
        if run_sql_err_case:
            self.run_case_sql_err()
        if run_port_current_case:
            self.run_case_port_current()
        if run_api_current_case:
            self.run_case_api_current()
        if run_header_current_case:
            self.run_case_header_current()
        if run_sql_current_case:
            self.run_case_sql_current()
        pass

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
