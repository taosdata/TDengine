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

import taos
import os
import json
import argparse
import subprocess
import datetime
import re

from multiprocessing import cpu_count
# from util.log import *
# from util.sql import *
# from util.cases import *
# from util.dnodes import *

class JoinPerf:

    def __init__(self, clearCache, dbName, keep):
        self.clearCache = clearCache
        self.dbname = dbName
        self.drop = "yes"
        self.keep = keep
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taosperf"
        self.conn = taos.connect(
            self.host,
            self.user,
            self.password,
            self.config)

    # def init(self, conn, logSql):
    #     tdLog.debug(f"start to excute {__file__}")
    #     tdSql.init(conn.cursor())

    def getBuildPath(self) -> str:
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/debug/build/bin")]
                    break
        return buildPath

    def getCfgDir(self) -> str:
        return self.getBuildPath() + "/sim/dnode1/cfg"

    # def initConnection(self):
    #     return self.getCfgDir()

    # def connectDB(self):
    #     self.conn = taos.connect(
    #         self.host,
    #         self.user,
    #         self.password,
    #         self.getCfgDir())
    #     return self.conn.cursor()

    def dbinfocfg(self) -> dict:
        return {
            "name": self.dbname,
            "drop": self.drop,
            "replica": 1,
            "duration": 10,
            "cache": 16,
            "blocks": 8,
            "precision": "ms",
            "keep": self.keep,
            "minRows": 100,
            "maxRows": 4096,
            "comp": 2,
            "walLevel": 1,
            "cachelast": 0,
            "quorum": 1,
            "wal_fsync_period": 3000,
            "update": 0
        }

    # def column_tag_count(self, **column_tag) -> list :
    #     return [{"type": type, "count": count} for type, count in column_tag.items()]

    def type_check(func):
        def wrapper(self, **kwargs):
            num_types = ["int", "float", "bigint", "tinyint", "smallint", "double"]
            str_types = ["binary", "nchar"]
            for k ,v in kwargs.items():
                if k.lower() not in num_types and k.lower() not in str_types:
                    return f"args {k} type error, not allowed"
                elif not isinstance(v, (int, list, tuple)):
                    return f"value {v} type error, not allowed"
                elif k.lower() in num_types and not isinstance(v, int):
                    return f"arg {v} takes 1 positional argument must be type int "
                elif isinstance(v, (list,tuple)) and len(v) > 2:
                    return f"arg {v} takes from 1 to 2 positional arguments but more than 2 were given "
                elif isinstance(v,(list,tuple))  and [ False for _ in v if not isinstance(_, int) ]:
                    return f"arg {v} takes from 1 to 2 positional arguments must be type int "
                else:
                    pass
            return func(self, **kwargs)
        return wrapper

    @type_check
    def column_tag_count(self, **column_tag) -> list :
        init_column_tag = []
        for k, v in column_tag.items():
            if re.search(k, "int, float, bigint, tinyint, smallint, double", re.IGNORECASE):
                init_column_tag.append({"type": k, "count": v})
            elif re.search(k, "binary, nchar", re.IGNORECASE):
                if isinstance(v, int):
                    init_column_tag.append({"type": k, "count": v, "len":8})
                elif len(v) == 1:
                    init_column_tag.append({"type": k, "count": v[0], "len": 8})
                else:
                    init_column_tag.append({"type": k, "count": v[0], "len": v[1]})
        return init_column_tag

    def stbcfg(self, stb: str, child_tab_count: int, prechildtab: str, columns: dict, tags: dict) -> dict:
        return {
            "name": stb,
            "child_table_exists": "no",
            "childtable_count": child_tab_count,
            "childtable_prefix": prechildtab,
            "auto_create_table": "no",
            "data_source": "rand",
            "insert_mode": "taosc",
            "insert_rows": 50,
            "multi_thread_write_one_tbl": "no",
            "number_of_tbl_in_one_sql": 0,
            "rows_per_tbl": 1,
            "max_sql_len": 65480,
            "disorder_ratio": 0,
            "disorder_range": 1000,
            "timestamp_step": 20000,
            "start_timestamp": "1969-12-31 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "tags_file": "",
            "columns": self.column_tag_count(**columns),
            "tags": self.column_tag_count(**tags)
        }

    def createcfg(self,db: dict, stbs: list) -> dict:
        return {
            "filetype": "insert",
            "cfgdir": self.config,
            "host": self.host,
            "port": 6030,
            "user": self.user,
            "password": self.password,
            "thread_count": cpu_count(),
            "create_table_thread_count": cpu_count(),
            "result_file": "/tmp/insert_res.txt",
            "confirm_parameter_prompt": "no",
            "insert_interval": 0,
            "num_of_records_per_req": 100,
            "max_sql_len": 1024000,
            "databases": [{
                "dbinfo": db,
                "super_tables": stbs
            }]
        }

    def createinsertfile(self,db: dict, stbs: list) -> str:
        date = datetime.datetime.now().strftime("%Y%m%d%H%M")
        file_create_table = f"/tmp/insert_{date}.json"

        with open(file_create_table, 'w') as f:
            json.dump(self.createcfg(db, stbs), f)

        return file_create_table

    def querysqls(self, sql: str) -> list:
        return [{"sql":sql,"result":""}]

    def querycfg(self, sql: str) -> dict:
        return {
            "filetype": "query",
            "cfgdir": self.config,
            "host": self.host,
            "port": 6030,
            "user": self.user,
            "password": self.password,
            "confirm_parameter_prompt": "yes",
            "databases": "db",
            "specified_table_query": {
                "query_interval": 0,
                "concurrent": cpu_count(),
                "sqls": self.querysqls(sql)
            }
        }

    def createqueryfile(self, sql: str):
        date = datetime.datetime.now().strftime("%Y%m%d%H%M")
        file_query_table = f"/tmp/query_{date}.json"

        with open(file_query_table,"w") as f:
            json.dump(self.querycfg(sql), f)

        return file_query_table

    def taosdemotable(self, filepath, resultfile="/dev/null"):
        taosdemopath = self.getBuildPath() + "/debug/build/bin"
        with open(filepath,"r") as f:
            filetype = json.load(f)["filetype"]
        if filetype == "insert":
            taosdemo_table_cmd = f"{taosdemopath}/taosdemo -f {filepath}  > {resultfile} 2>&1"
        else:
            taosdemo_table_cmd = f"yes | {taosdemopath}/taosdemo -f {filepath}  > {resultfile} 2>&1"
        _ = subprocess.check_output(taosdemo_table_cmd, shell=True).decode("utf-8")

    def droptmpfile(self):
        drop_file_cmd = "rm -f /tmp/query_* "
        _ = subprocess.check_output(drop_file_cmd, shell=True).decode("utf-8")
        drop_file_cmd = "rm -f querySystemInfo-*"
        _ = subprocess.check_output(drop_file_cmd, shell=True).decode("utf-8")
        drop_file_cmd = "rm -f /tmp/insert_* "
        _ = subprocess.check_output(drop_file_cmd, shell=True).decode("utf-8")

    def run(self):
        print("========== join on table schema performance ==========")
        if self.clearCache == True:
            # must be root permission
            subprocess.check_output("echo 3 > /proc/sys/vm/drop_caches")

        # cfg = {
        #     'enableRecordSql': '1'
        # }
        # tdDnodes.deploy(1, cfg)

        # tdLog.printNoPrefix("==========step1: create 1024 columns on different data type==========")
        # db = self.dbinfocfg()
        # stblist = []
        # # the supertable config for each type in column_tag_types
        # column_tag_types = ["INT","FLOAT","BIGINT","TINYINT","SMALLINT","DOUBLE","BINARY","NCHAR"]
        # for i in range(len(column_tag_types)):
        #     tagtype =  {
        #         "INT": 0,
        #         "FLOAT": 0,
        #         "BIGINT": 0,
        #         "TINYINT": 0,
        #         "SMALLINT": 0,
        #         "DOUBLE": 0,
        #         "BINARY": 0,
        #         "NCHAR": 0
        #     }
        #     columntype = tagtype.copy()
        #     tagtype["INT"] = 2
        #     if column_tag_types[i] == "BINARY":
        #         columntype[column_tag_types[i]] = [509, 10]
        #     elif column_tag_types[i] == "NCHAR":
        #         columntype[column_tag_types[i]] = [350, 10]
        #     else:
        #         columntype[column_tag_types[i]] = 1021
        #     supertable = self.stbcfg(
        #         stb=f"stb{i}",
        #         child_tab_count=2,
        #         prechildtab=f"t{i}",
        #         columns=columntype,
        #         tags=tagtype
        #     )
        #     stblist.append(supertable)
        # self.taosdemotable(self.createinsertfile(db=db, stbs=stblist))

        # tdLog.printNoPrefix("==========step2: execute query operation==========")
        # tdLog.printNoPrefix("==========execute query operation==========")
        sqls = {
            "nchar":"select * from t00,t01 where t00.ts=t01.ts"
        }
        for type, sql in sqls.items():
            result_file = f"/tmp/queryResult_{type}.log"
            self.taosdemotable(self.createqueryfile(sql), resultfile=result_file)
            if result_file:
                # tdLog.printNoPrefix(f"execute type {type} sql, the sql is: {sql}")
                print(f"execute type {type} sql, the sql is: {sql}")
                max_sql_time_cmd = f'''
                grep Spent {result_file} | awk 'NR==1{{max=$7;next}}{{max=max>$7?max:$7}}END{{print "Max=",max,"s"}}'
                '''
                max_sql_time = subprocess.check_output(max_sql_time_cmd, shell=True).decode("UTF-8")
                # tdLog.printNoPrefix(f"type: {type} sql time : {max_sql_time}")
                print(f"type: {type} sql time : {max_sql_time}")

                min_sql_time_cmd = f'''
                grep Spent {result_file} | awk 'NR==1{{min=$7;next}}{{min=min<$7?min:$7}}END{{print "Min=",min,"s"}}'
                '''
                min_sql_time = subprocess.check_output(min_sql_time_cmd, shell=True).decode("UTF-8")
                # tdLog.printNoPrefix(f"type: {type} sql time : {min_sql_time}")
                print(f"type: {type} sql time : {min_sql_time}")

                avg_sql_time_cmd = f'''
                grep Spent {result_file} |awk '{{sum+=$7}}END{{print "Average=",sum/NR,"s"}}'
                '''
                avg_sql_time = subprocess.check_output(avg_sql_time_cmd, shell=True).decode("UTF-8")
                # tdLog.printNoPrefix(f"type: {type}  sql time : {avg_sql_time}")
                print(f"type: {type}  sql time : {avg_sql_time}")

                # tdLog.printNoPrefix(f"==========type {type} sql is over==========")

        # tdLog.printNoPrefix("==========query operation is over==========")

        self.droptmpfile()
        # tdLog.printNoPrefix("==========tmp file has been deleted==========")

    # def stop(self):
    #     tdSql.close()
    #     tdLog.success(f"{__file__} successfully executed")

# tdCases.addLinux(__file__, TDTestCase())
# tdCases.addWindows(__file__, TDTestCase())

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-r',
        '--remove-cache',
        action='store_true',
        default=False,
        help='clear cache before query (default: False)')
    parser.add_argument(
        '-d',
        '--database-name',
        action='store',
        default='db',
        type=str,
        help='Database name to be created (default: db)')
    parser.add_argument(
        '-k',
        '--keep-time',
        action='store',
        default=36500,
        type=int,
        help='Database keep parameters (default: 36500)')

    args = parser.parse_args()
    jointest = JoinPerf(args.remove_cache, args.database_name, args.keep_time)
    jointest.run()