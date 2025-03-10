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
import os
import json
import csv
import datetime

import frame
import frame.eos
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
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
                            tdLog.exit(f"interlace_rows invalid. tbName={name} actual={batch} expected={interlace_rows} i={count} csv_file_name={csv_file_name}")
                        batch = 1
                        name  = row[tbname_idx]
                # count ++
                count += 1
        # batch
        if batch != interlace_rows:
            tdLog.exit(f"interlace_rows invalid. tbName={name} actual={batch} expected={interlace_rows} i={count} csv_file_name={csv_file_name}")

        # check all rows
        if count != all_rows:
            tdLog.exit(f"all_rows invalid. actual={count} expected={all_rows} csv_file_name={csv_file_name}")

        tdLog.info(f"Check generate csv file successfully. csv_file_name={csv_file_name} count={count} interlace_rows={batch}")


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
    def check_result(self, jsonFile):
         # csv
        with open(jsonFile) as file:
             data = json.load(file)

        # read json
        database = data["databases"][0]
        stables  = database["super_tables"]

        for stable in stables:
            # check csv context correct
            self.check_stb_correct(data, database, stable)


    def check_export_csv(self, benchmark, jsonFile, options=""):
        # clear
        self.clear_directory()

        # exec
        cmd = f"{benchmark} {options} -f {jsonFile}"
        eos.exe(cmd)

        # check result
        self.check_result(jsonFile)
 

    def run(self):
        # path
        benchmark = etool.benchMarkFile()

        # do check interlace normal
        json = "tools/benchmark/basic/json/csv-export.json"
        self.check_export_csv(benchmark, json)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
