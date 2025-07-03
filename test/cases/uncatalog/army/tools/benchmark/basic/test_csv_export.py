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
from new_test_framework.utils import tdLog, tdSql, etool, eos
import os
import shutil
import json
import datetime
import csv


class TestCsvExport:
    def caseDescription(self):
        """
        [TS-5089] taosBenchmark support exporting csv
        """


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


    def test_csv_export(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
        # path
        benchmark = etool.benchMarkFile()

        # check normal
        json_file = "tools/benchmark/basic/json/csv-export.json"
        self.check_export_csv_main(benchmark, json_file)

        # check others
        json_file = "tools/benchmark/basic/json/csv-export-template.json"
        self.check_export_csv_others(benchmark, json_file)


        tdLog.success("%s successfully executed" % __file__)


