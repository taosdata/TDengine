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
import sys
import os
import json
import argparse
import subprocess
import datetime
import re


from multiprocessing import cpu_count
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.dnodes import TDDnode

class Taosdemo:
    def __init__(self, clearCache, dbName, keep):
        self.clearCache = clearCache
        self.dbname = dbName
        self.drop = "yes"
        self.keep = keep
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        # self.config = "/etc/taosperf"
        # self.conn = taos.connect(
        #     self.host,
        #     self.user,
        #     self.password,
        #     self.config)

    # env config
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

    def getExeToolsDir(self) -> str:
        self.debugdir = self.getBuildPath() + "/debug/build/bin"
        return self.debugdir

    def getCfgDir(self) -> str:
        self.config = self.getBuildPath() + "/sim/dnode1/cfg"
        return self.config

    # taodemo insert file config
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

    def stbcfg(self, stb: str, child_tab_count: int, rows: int, prechildtab: str, columns: dict, tags: dict) -> dict:
        return {
            "name": stb,
            "child_table_exists": "no",
            "childtable_count": child_tab_count,
            "childtable_prefix": prechildtab,
            "auto_create_table": "no",
            "batch_create_tbl_num": 10,
            "data_source": "rand",
            "insert_mode": "taosc",
            "insert_rows": rows,
            "childtable_limit": 0,
            "childtable_offset": 0,
            "rows_per_tbl": 1,
            "max_sql_len": 65480,
            "disorder_ratio": 0,
            "disorder_range": 1000,
            "timestamp_step": 10,
            "start_timestamp": f"{datetime.datetime.now():%F %X}",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "tags_file": "",
            "columns": self.column_tag_count(**columns),
            "tags": self.column_tag_count(**tags)
        }

    def schemecfg(self,intcount=1,floatcount=0,bcount=0,tcount=0,scount=0,doublecount=0,binarycount=0,ncharcount=0):
        return  {
            "INT": intcount,
            "FLOAT": floatcount,
            "BIGINT": bcount,
            "TINYINT": tcount,
            "SMALLINT": scount,
            "DOUBLE": doublecount,
            "BINARY": binarycount,
            "NCHAR": ncharcount
        }

    def insertcfg(self,db: dict, stbs: list) -> dict:
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
        date = datetime.datetime.now()
        file_create_table = f"/tmp/insert_{date:%F-%H%M}.json"

        with open(file_create_table, 'w') as f:
            json.dump(self.insertcfg(db, stbs), f)

        return file_create_table

    # taosdemo query file config
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
            "query_times": 10,
            "query_mode": "taosc",
            "databases": self.dbname,
            "specified_table_query": {
                "query_interval": 0,
                "concurrent": cpu_count(),
                "sqls": self.querysqls(sql)
            }
        }

    def createqueryfile(self, sql: str):
        date = datetime.datetime.now()
        file_query_table = f"/tmp/query_{date:%F-%H%M}.json"

        with open(file_query_table,"w") as f:
            json.dump(self.querycfg(sql), f)

        return file_query_table

    # Execute taosdemo, and delete temporary files when finished
    def taosdemotable(self, filepath: str, resultfile="/dev/null"):
        taosdemopath = self.getBuildPath() + "/debug/build/bin"
        with open(filepath,"r") as f:
            filetype = json.load(f)["filetype"]
            if filetype == "insert":
                taosdemo_table_cmd = f"{taosdemopath}/taosdemo -f {filepath}  > {resultfile} 2>&1"
            else:
                taosdemo_table_cmd = f"yes | {taosdemopath}/taosdemo -f {filepath}  > {resultfile} 2>&1"
        try:
            _ = subprocess.check_output(taosdemo_table_cmd, shell=True).decode("utf-8")
        except subprocess.CalledProcessError as e:
            _ = e.output

    def droptmpfile(self, filepath: str):
        drop_file_cmd = f"[ -f {filepath} ] && rm -f  {filepath}"
        try:
            _ = subprocess.check_output(drop_file_cmd, shell=True).decode("utf-8")
        except subprocess.CalledProcessError as e:
            _ = e.output

    # TODO:需要完成TD-4153的数据插入和客户端请求的性能查询。
    def td4153insert(self):

        tdLog.printNoPrefix("========== start to create table and insert data ==========")
        self.dbname = "td4153"
        db = self.dbinfocfg()
        stblist = []

        columntype = self.schemecfg(intcount=1, ncharcount=100)
        tagtype = self.schemecfg(intcount=1)
        stbname = "stb1"
        prechild = "t1"
        stable = self.stbcfg(
            stb=stbname,
            prechildtab=prechild,
            child_tab_count=2,
            rows=10000,
            columns=columntype,
            tags=tagtype
        )
        stblist.append(stable)
        insertfile = self.createinsertfile(db=db, stbs=stblist)

        nmon_file = f"/tmp/insert_{datetime.datetime.now():%F-%H%M}.nmon"
        cmd = f"nmon -s5 -F {nmon_file} -m /tmp/"
        try:
            _ = subprocess.check_output(cmd, shell=True).decode("utf-8")
        except subprocess.CalledProcessError as e:
            _ = e.output

        self.taosdemotable(insertfile)
        self.droptmpfile(insertfile)
        self.droptmpfile("/tmp/insert_res.txt")

        # In order to prevent too many performance files from being generated, the nmon file is deleted.
        # and the delete statement can be cancelled during the actual test.
        self.droptmpfile(nmon_file)

        cmd = f"ps -ef|grep -w nmon| grep -v grep | awk '{{print $2}}'"
        try:
            time.sleep(10)
            _ = subprocess.check_output(cmd,shell=True).decode("utf-8")
        except BaseException as e:
            raise e

    def td4153query(self):
        tdLog.printNoPrefix("========== start to query operation ==========")

        sqls = {
            "select_all": "select * from stb1",
            "select_join": "select * from t10, t11 where t10.ts=t11.ts"
        }
        for type, sql in sqls.items():
            result_file = f"/tmp/queryResult_{type}.log"
            query_file = self.createqueryfile(sql)
            try:
                self.taosdemotable(query_file, resultfile=result_file)
            except subprocess.CalledProcessError as e:
                out_put = e.output
            if result_file:
                print(f"execute rows {type.split('_')[1]} sql, the sql is: {sql}")
                max_sql_time_cmd = f'''
                grep -o Spent.*s {result_file} |awk 'NR==1{{max=$2;next}}{{max=max>$2?max:$2}}END{{print "Max=",max,"s"}}'
                 '''
                max_sql_time = subprocess.check_output(max_sql_time_cmd, shell=True).decode("UTF-8")
                print(f"{type.split('_')[1]} rows sql time : {max_sql_time}")

                min_sql_time_cmd = f'''
                grep -o Spent.*s {result_file} |awk 'NR==1{{min=$2;next}}{{min=min<$2?min:$2}}END{{print "Min=",min,"s"}}'
                '''
                min_sql_time = subprocess.check_output(min_sql_time_cmd, shell=True).decode("UTF-8")
                print(f"{type.split('_')[1]} rows sql time : {min_sql_time}")

                avg_sql_time_cmd = f'''
                grep -o Spent.*s {result_file} |awk '{{sum+=$2}}END{{print "Average=",sum/NR,"s"}}'
                '''
                avg_sql_time = subprocess.check_output(avg_sql_time_cmd, shell=True).decode("UTF-8")
                print(f"{type.split('_')[1]} rows sql time : {avg_sql_time}")

            self.droptmpfile(query_file)
            self.droptmpfile(result_file)

        drop_query_tmt_file_cmd = " find ./ -name 'querySystemInfo-*'  -type f -exec rm {} \; "
        try:
            _ = subprocess.check_output(drop_query_tmt_file_cmd, shell=True).decode("utf-8")
        except subprocess.CalledProcessError as e:
            _ = e.output
        pass

    def td4153(self):
        self.td4153insert()
        self.td4153query()


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
        default=3650,
        type=int,
        help='Database keep parameters (default: 3650)')

    args = parser.parse_args()
    taosdemo = Taosdemo(args.remove_cache, args.database_name, args.keep_time)
    # taosdemo.conn = taos.connect(
    #     taosdemo.host,
    #     taosdemo.user,
    #     taosdemo.password,
    #     taosdemo.config
    # )

    debugdir = taosdemo.getExeToolsDir()
    cfgdir = taosdemo.getCfgDir()
    cmd = f"{debugdir}/taosd -c {cfgdir} >/dev/null 2>&1 &"
    try:
        _ = subprocess.check_output(cmd, shell=True).decode("utf-8")
    except subprocess.CalledProcessError as e:
        _ = e.output

    if taosdemo.clearCache:
        # must be root permission
        subprocess.check_output("echo 3 > /proc/sys/vm/drop_caches", shell=True).decode("utf-8")

    taosdemo.td4153()
