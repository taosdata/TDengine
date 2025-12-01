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

from new_test_framework.utils import tdLog, tdSql, etool, eutil, eos
from new_test_framework.utils.autogen import AutoGen
import os
import shutil
import json
import datetime
import csv
import gzip
import platform


class TestBenchmarkWithCsv:
    updatecfgDict = {
        'slowLogScope' : "others"
    }

    def setup_class(cls):
        tdLog.info(f"start to init {__file__}")


    #
    # ------------------- test_create_table_from_csv.py ----------------
    #
    def do_create_table_from_csv(self):
        tdLog.info(f"start to excute {__file__}")
        binPath = etool.benchMarkFile()
        if binPath == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found in %s" % binPath)

        # insert: create one  or multiple tables per sql and insert multiple rows per sql
        os.system("%s -f %s/json/create_table_tags.json -y " % (binPath, os.path.dirname(__file__)))
        tdSql.query("SELECT COUNT(*) FROM (SELECT DISTINCT tbname FROM test.meters);")
        tdSql.checkData(0, 0, 4)

        print("do create table from csv .............. [passed]")

    #
    # ------------------- test_create_table_keywords.py ----------------
    #
    def do_create_tbl_keywords(self):
        tdSql.prepare()
        tdSql.execute("DROP DATABASE IF EXISTS test;")
        autoGen = AutoGen()
        autoGen.create_db("test", 1, 1)
        tdSql.execute(f"use test")
        tdLog.info(f"start to excute {__file__}")
        tdSql.execute('''CREATE TABLE IF NOT EXISTS test.meters (time TIMESTAMP,`value` double, qulity bigint,flags bigint) TAGS (id nchar(32),station nchar(32),type nchar(8))''')
        
        binPath = etool.benchMarkFile()
        if binPath == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found in %s" % binPath)

        # insert: create one  or multiple tables per sql and insert multiple rows per sql
        templateFilePath = f"{os.path.dirname(os.path.realpath(__file__))}/json/create_table_keywords.json"
        os.system(f"{binPath} -f {templateFilePath} -y ")
        tdSql.query("SELECT COUNT(*) FROM test.meters;")
        tdSql.checkData(0, 0, 9)

        print("do create table keywords .............. [passed]")

    #
    # ------------------- test_csv_export.py ----------------
    #
    def clear_directory(self, target_dir: str = 'csv'):
        try:
            if not os.path.exists(target_dir): 
                return 
            for entry in os.listdir(target_dir): 
                entry_path = os.path.join(target_dir,  entry)
                if os.path.isfile(entry_path)  or os.path.islink(entry_path): 
                    os.unlink(entry_path)
                else:
                    shutil.rmtree(entry_path)
 
            tdLog.debug("clear succ, dir: %s " % (target_dir))
        except OSError as e:
            tdLog.exit("clear fail, dir: %s " % (target_dir))


    def convert_timestamp(self, ts, ts_format):
        dt_object = datetime.datetime.fromtimestamp(ts / 1000)
        formatted_time = dt_object.strftime(ts_format)
        return formatted_time


    def calc_time_slice_partitions(self, total_start_ts, total_end_ts, ts_step, ts_format, ts_interval):
        interval_days   = int(ts_interval[:-1])
        n_days_millis   = interval_days * 24 * 60 * 60 * 1000

        dt_start        = datetime.datetime.fromtimestamp(total_start_ts / 1000.0)
        formatted_str   = dt_start.strftime(ts_format) 
        s0_dt           = datetime.datetime.strptime(formatted_str, ts_format)
        s0              = int(s0_dt.timestamp() * 1000)

        partitions      = []
        current_s       = s0

        while current_s <= total_end_ts:
            current_end     = current_s + n_days_millis 
            start_actual    = max(current_s, total_start_ts)
            end_actual      = min(current_end, total_end_ts)
            
            if start_actual >= end_actual:
                count = 0 
            else:
                delta       = end_actual - start_actual
                delta 
                delta_start = start_actual - total_start_ts
                delta_end   = end_actual - total_start_ts
                if delta % ts_step:
                    count = delta // ts_step + 1
                else:
                    count = delta // ts_step

            partitions.append({ 
                "start_ts": current_s,
                "end_ts": current_end,
                "start_time": self.convert_timestamp(current_s, ts_format),
                "end_time": self.convert_timestamp(current_end, ts_format),
                "count": count 
            })
            
            current_s += n_days_millis 
        
        # partitions = [p for p in partitions if p['count'] > 0]
        return partitions


    def check_stb_csv_correct(self, csv_file_name, all_rows, interlace_rows):
        # open as csv
        tbname_idx  = 14
        count       = 0
        batch       = 0
        name        = ""
        header      = True
        with open(csv_file_name) as file:
            rows = csv.reader(file)
            for row in rows:
                if header:
                    header = False
                    continue

                # interlace_rows
                if name == "":
                    name  = row[tbname_idx]
                    batch = 1
                else:
                    if name == row[tbname_idx]:
                        batch += 1
                    else:
                        # switch to another child table
                        if batch != interlace_rows:
                            tdLog.exit(f"interlace rows is not as expected. tbname={name}, actual: {batch}, expected: {interlace_rows}, count: {count}, csv_file_name: {csv_file_name}")
                        batch = 1
                        name  = row[tbname_idx]
                # count ++
                count += 1
        # batch
        if batch != interlace_rows:
            tdLog.exit(f"interlace rows is not as expected. tbname={name}, actual: {batch}, expected: {interlace_rows}, count: {count}, csv_file_name: {csv_file_name}")


        # check all rows
        if count != all_rows:
            tdLog.exit(f"total rows is not as expected. actual: {count}, expected: {all_rows}, csv_file_name: {csv_file_name}")

        tdLog.info(f"check generate csv file successfully. csv_file_name: {csv_file_name}, count: {count}, interlace_rows: {interlace_rows}")


    # check correct
    def check_stb_correct(self, data, db, stb):
        filepath        = data["output_dir"]
        stbName         = stb["name"]
        child_count     = stb["childtable_to"] - stb["childtable_from"]
        insert_rows     = stb["insert_rows"]
        interlace_rows  = stb["interlace_rows"]
        csv_file_prefix = stb["csv_file_prefix"]
        csv_ts_format   = stb.get("csv_ts_format", None)
        csv_ts_interval = stb.get("csv_ts_interval", None)

        ts_step          = stb["timestamp_step"]
        total_start_ts   = stb["start_timestamp"]
        total_end_ts     = total_start_ts + ts_step * insert_rows


        all_rows = child_count * insert_rows
        if interlace_rows > 0:
            # interlace

            if not csv_ts_format:
                # normal
                csv_file_name = f"{filepath}{csv_file_prefix}.csv"
                self.check_stb_csv_correct(csv_file_name, all_rows, interlace_rows)
            else:
                # time slice
                partitions = self.calc_time_slice_partitions(total_start_ts, total_end_ts, ts_step, csv_ts_format, csv_ts_interval)
                for part in partitions:
                    csv_file_name = f"{filepath}{csv_file_prefix}_{part['start_time']}_{part['end_time']}.csv"
                    self.check_stb_csv_correct(csv_file_name, part['count'] * child_count, interlace_rows)
        else:
            # batch
            thread_count    = stb["thread_count"]
            interlace_rows  = insert_rows
            if not csv_ts_format:
                # normal
                for i in range(thread_count):
                    csv_file_name = f"{filepath}{csv_file_prefix}_{i + 1}.csv"
                    if i < child_count % thread_count:
                        self.check_stb_csv_correct(csv_file_name, insert_rows * (child_count // thread_count + 1), interlace_rows)
                    else:
                        self.check_stb_csv_correct(csv_file_name, insert_rows * (child_count // thread_count), interlace_rows)
            else:
                # time slice
                for i in range(thread_count):
                    partitions = self.calc_time_slice_partitions(total_start_ts, total_end_ts, ts_step, csv_ts_format, csv_ts_interval)
                    for part in partitions:
                        csv_file_name = f"{filepath}{csv_file_prefix}_{i + 1}_{part['start_time']}_{part['end_time']}.csv"
                        if i < child_count % thread_count:
                            slice_rows = part['count'] * (child_count // thread_count + 1)
                        else:
                            slice_rows = part['count'] * (child_count // thread_count)

                        self.check_stb_csv_correct(csv_file_name, slice_rows, part['count'])


    # check result
    def check_result(self, json_file):
         # csv
        with open(json_file) as file:
             data = json.load(file)

        # read json
        database = data["databases"][0]
        stables  = database["super_tables"]

        for stable in stables:
            # check csv context correct
            self.check_stb_correct(data, database, stable)


    def exec_benchmark(self, benchmark, json_file, options=""):
        cmd = f"{benchmark} {options} -f {json_file}"
        eos.exe(cmd)


    def check_export_csv_main(self, benchmark, json_file, options=""):
        # clear
        self.clear_directory()

        # exec
        self.exec_benchmark(benchmark, json_file, options)

        # check result
        self.check_result(json_file)
 

    def check_export_csv_others(self, benchmark, json_file, options=""):
        # clear
        self.clear_directory()

        # file ts interval second
        new_json_file = self.genNewJson(json_file, self.func_csv_ts_interval_second)
        self.exec_benchmark(benchmark, new_json_file, options)
        self.check_file_line_count("./csv/data_20231115061320_20231115061321.csv", 10001)
        self.deleteFile(new_json_file)

        # file ts interval minute
        new_json_file = self.genNewJson(json_file, self.func_csv_ts_interval_minute)
        self.exec_benchmark(benchmark, new_json_file, options)
        self.check_file_line_count("./csv/data_202311150613_202311150614.csv", 10001)
        self.deleteFile(new_json_file)

        # file ts interval hour
        new_json_file = self.genNewJson(json_file, self.func_csv_ts_interval_hour)
        self.exec_benchmark(benchmark, new_json_file, options)
        self.check_file_line_count("./csv/data_2023111506_2023111507.csv", 10001)
        self.deleteFile(new_json_file)

        # db precision us
        new_json_file = self.genNewJson(json_file, self.func_db_precision_us)
        self.exec_benchmark(benchmark, new_json_file, options)
        self.check_file_line_count("./csv/data_20231115_20231116.csv", 10001)
        self.deleteFile(new_json_file)

        # db precision ns
        new_json_file = self.genNewJson(json_file, self.func_db_precision_ns)
        self.exec_benchmark(benchmark, new_json_file, options)
        self.check_file_line_count("./csv/data_20231115_20231116.csv", 10001)
        self.deleteFile(new_json_file)

        # thread num
        new_json_file = self.genNewJson(json_file, self.func_thread_num)
        self.exec_benchmark(benchmark, new_json_file, options)
        self.check_file_line_count("./csv/data_10.csv", 1001)
        self.deleteFile(new_json_file)

        # create sql
        new_json_file = self.genNewJson(json_file, self.func_create_sql)
        self.exec_benchmark(benchmark, new_json_file, options)
        self.check_file_line_count("./csv/create_stmt.txt", 2)
        self.deleteFile(new_json_file)

        # gzip
        new_json_file = self.genNewJson(json_file, self.func_gzip)
        self.exec_benchmark(benchmark, new_json_file, options)
        if platform.system().lower() == 'windows':
            with gzip.open(r'./csv/data.csv.gz', 'rt', encoding='utf-8') as fin, open(r'./csv/data.csv', 'w', encoding='utf-8') as fout:
                for line in fin:
                    fout.write(line)
        else:
            eos.exe("gzip -f ./csv/data.csv")
        eos.exe("gunzip ./csv/data.csv.gz")
        self.check_file_line_count("./csv/data.csv", 10001)
        self.deleteFile(new_json_file)


    def func_csv_ts_interval_second(self, data):
        db  = data['databases'][0]
        stb = db["super_tables"][0]
        stb['timestamp_step'] = '10'
        stb['csv_ts_format'] = '%Y%m%d%H%M%S'
        stb['csv_ts_interval'] = '1s'


    def func_csv_ts_interval_minute(self, data):
        db  = data['databases'][0]
        stb = db["super_tables"][0]
        stb['timestamp_step'] = '600'
        stb['csv_ts_format'] = '%Y%m%d%H%M'
        stb['csv_ts_interval'] = '1m'


    def func_csv_ts_interval_hour(self, data):
        db  = data['databases'][0]
        stb = db["super_tables"][0]
        stb['timestamp_step'] = '36000'
        stb['csv_ts_format'] = '%Y%m%d%H'
        stb['csv_ts_interval'] = '1h'


    def func_db_precision_us(self, data):
        db  = data['databases'][0]
        db['dbinfo']['precision'] = 'us'
        stb = db["super_tables"][0]
        stb['start_timestamp'] = 1700000000000000


    def func_db_precision_ns(self, data):
        db  = data['databases'][0]
        db['dbinfo']['precision'] = 'ns'
        stb = db["super_tables"][0]
        stb['start_timestamp'] = 1700000000000000000


    def func_thread_num(self, data):
        data['thread_count'] = 12
        db  = data['databases'][0]
        stb = db["super_tables"][0]
        stb.pop('interlace_rows', None)
        stb.pop('csv_ts_format', None)
        stb.pop('csv_ts_interval', None)


    def func_create_sql(self, data):
        db  = data['databases'][0]
        dbinfo = db['dbinfo']
        dbinfo['buffer']    = 256
        dbinfo['cachemode'] = 'none'
        stb = db["super_tables"][0]
        stb['primary_key']  = 1
        stb['columns'][0]   = { "type": "bool", "name": "bc", "encode": 'simple8b', 'compress': 'lz4', 'level': 'medium'}
        stb['comment']      = "csv export sample"
        stb['delay']        = 10
        stb['file_factor']  = 20
        stb['rollup']       = 'min'
        stb['max_delay']    = '300s'
        stb['watermark']    = '10m'
        stb['columns'][1]   = { "type": "float", "name": "fc", "min": 1, "sma": "yes"}
        stb['columns'][2]   = { "type": "double", "name": "dc", "min":10, "max":10, "sma": "yes"}


    def func_gzip(self, data):
        db  = data['databases'][0]
        stb = db["super_tables"][0]
        stb.pop('csv_ts_format', None)
        stb.pop('csv_ts_interval', None)
        stb['csv_compress_level'] = "fast"


    def check_file_line_count(self, filename, expected_lines):
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                actual_lines = sum(1 for line in file)

            if expected_lines >= 0:
                is_correct = actual_lines == expected_lines
                if not is_correct:
                    tdLog.exit(f"check csv data failed, actual: {actual_lines}, expected: {expected_lines}, filename: {filename}")
        
        except FileNotFoundError:
            tdLog.exit(f"check csv data failed, file not exists. filename: {filename}")


    def do_csv_export(self):
        # path
        benchmark = etool.benchMarkFile()

        # check normal
        json_file = f"{os.path.dirname(__file__)}/json/csv-export.json"
        self.check_export_csv_main(benchmark, json_file)

        # check others
        json_file = f"{os.path.dirname(__file__)}/json/csv-export-template.json"
        self.check_export_csv_others(benchmark, json_file)

        print("do csv export ......................... [passed]")

    #
    # ------------------- test_insert_json_csv.py ----------------
    #
    def do_insert_json_csv(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        if binPath == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found in %s" % binPath)

        # insert: create one  or multiple tables per sql and insert multiple rows per sql
        # test case for https://jira.taosdata.com:18080/browse/TD-4985
        os.system("%s -f %s/json/insert-json-csv.json -y " % (binPath, os.path.dirname(__file__)))

        tdSql.execute("use db")
        if major_ver == "3":
            tdSql.query("select count(*) from (select distinct(tbname) from stb0)")
        else:
            tdSql.query("select count(tbname) from stb0")
        tdSql.checkData(0, 0, 1)

        tdSql.query("select * from stb0 where  tbname like 'stb00_0'  limit 10")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)

        print("do insert csv  ........................ [passed]")

    #
    # ------------------- test_stmt_sample_csv_json_doesnt_use_ts.py ----------------
    #
    def do_stmt_sample_csv_json_doesnt_use_ts(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/stmt_sample_doesnt_use_ts.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(8)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 80)
        tdSql.query("select * from db.stb_0")
        tdSql.checkRows(10)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, None)
        tdSql.query("select distinct(t0) from db.stb")
        tdSql.checkRows(2)

        
        dbresult = tdSql.queryResult
        if dbresult[0][0] not in (17, None):
            tdLog.exit("result[0][0]: {}".format(dbresult[0][0]))
        else:
            tdLog.info("result[0][0]: {}".format(dbresult[0][0]))

        print("do stmt sample csv no use ts .......... [passed]")
    
    #
    # ------------------- test_stmt_sample_csv_json_subtable.py ----------------
    #
    def do_stmt_sample_csv_json_subtable(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/stmt_sample_use_ts-subtable.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(8)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 32)
        tdSql.query("select * from db.stb0")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, None)

        tdSql.query("select * from db.stb3")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 300)
        tdSql.checkData(1, 1, 600)
        tdSql.checkData(2, 1, 900)
        tdSql.checkData(3, 1, None)

        tdSql.query("select * from db.stb5")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 500)
        tdSql.checkData(1, 1, 1000)
        tdSql.checkData(2, 1, 1500)
        tdSql.checkData(3, 1, None)

        tdSql.query("select distinct(t0) from db.stb")
        tdSql.checkRows(2)

        dbresult = tdSql.queryResult
        if dbresult[0][0] not in (17, None):
            tdLog.exit("result[0][0]: {}".format(dbresult[0][0]))
        else:
            tdLog.info("result[0][0]: {}".format(dbresult[0][0]))

        print("do stmt sample subtable ............... [passed]")
            
    #
    # ------------------- test_stmt_sample_csv_json.py ----------------
    #       
    def do_stmt_sample_csv_json(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/stmt_sample_use_ts.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(8)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 32)
        tdSql.query("select * from db.stb_0")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, None)
        tdSql.query("select distinct(t0) from db.stb")
        tdSql.checkRows(2)

        dbresult = tdSql.queryResult
        if dbresult[0][0] not in (17, None):
            tdLog.exit("result[0][0]: {}".format(dbresult[0][0]))
        else:
            tdLog.info("result[0][0]: {}".format(dbresult[0][0]))

        print("do stmt sample csv .................... [passed]")
    
    
    #
    # ------------------- test_taosc_sample_csv_json_subtable.py ----------------
    #
    def do_taosc_sample_csv_json_subtable(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/taosc_sample_use_ts-subtable.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(8)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 32)
        tdSql.query("select * from db.stb0")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, None)

        tdSql.query("select * from db.stb3")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 300)
        tdSql.checkData(1, 1, 600)
        tdSql.checkData(2, 1, 900)
        tdSql.checkData(3, 1, None)

        tdSql.query("select * from db.stb5")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 500)
        tdSql.checkData(1, 1, 1000)
        tdSql.checkData(2, 1, 1500)
        tdSql.checkData(3, 1, None)

        tdSql.query("select distinct(t0) from db.stb")
        tdSql.checkRows(2)

        dbresult = tdSql.queryResult
        if dbresult[0][0] not in (17, None):
            tdLog.exit("result[0][0]: {}".format(dbresult[0][0]))
        else:
            tdLog.info("result[0][0]: {}".format(dbresult[0][0]))

        print("do taosc csv subtable ................. [passed]") 

    #
    # ------------------- test_taosc_sample_csv_json.py ----------------
    #
    def do_taosc_sample_csv_json(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/taosc_sample_use_ts.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(8)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 32)
        tdSql.query("select * from db.stb_0")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, None)
        tdSql.query("select distinct(t0) from db.stb")
        tdSql.checkRows(2)

        dbresult = tdSql.queryResult
        if dbresult[0][0] not in (17, None):
            tdLog.exit("result[0][0]: {}".format(dbresult[0][0]))
        else:
            tdLog.info("result[0][0]: {}".format(dbresult[0][0]))

        print("do taosc csv .......................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_benchmark_with_csv(self):
        """taosBenchmark json with csv

        1. Create table tags from csv file
        2. Create table with keywords
        3. Export data to csv files
        4. Insert data from json and csv files
        5. Stmt sample with csv and json
        6. Stmt sample with csv and json doesnt use ts
        7. Stmt sample with csv and json subtable
        8. Taosc sample with csv and json
        9. Taosc sample with csv and json subtable

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_create_table_from_csv.py
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_create_table_keywords.py
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_csv_export.py
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_insert_json_csv.py
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_stmt_sample_csv_json.py 
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_stmt_sample_csv_json_doesnt_use_ts.py
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_stmt_sample_csv_json_subtable.py
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosc_sample_csv_json_subtable.py
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosc_sample_csv_json.py 

        """
        self.do_create_table_from_csv()
        self.do_create_tbl_keywords()
        self.do_csv_export()
        self.do_insert_json_csv()
        self.do_stmt_sample_csv_json()
        self.do_stmt_sample_csv_json_doesnt_use_ts()
        self.do_stmt_sample_csv_json_subtable()
        self.do_taosc_sample_csv_json_subtable()
        self.do_taosc_sample_csv_json()
