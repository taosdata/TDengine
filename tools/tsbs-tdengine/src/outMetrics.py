#
# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

import os
import sys
import time

class OutMetrics:
    def __init__(self):
        self.time_start       = None
        self.time_start_write = {}
        self.time_start_test  = {}
        self.time_end_write   = {}
        self.time_end_test    = {}
        self.time_end         = None
        
        self.scenarioId  = {}
        self.classification  = {}
        self.data_rows   = {}
        self.output_rows = {}
        self.status      = {}
    
    def init_metrics(self, metrics_file):
        print("Initializing metrics output")
        self.metrics_file = metrics_file
        
    def output_metrics(self):
        print(f"Outputting metrics to {self.metrics_file}")
        print("| Scenario ID | Classification | Out Records   | In Records | Start Time   | End Time     | Duration(ms) | Throughput(rec/s) | Status |")
        print("|-------------|----------------|---------------|------------|--------------|--------------|--------------|-------------------|--------|")        
        for scenario in self.scenarioId:
            start_time = self.time_start_write.get(scenario, 0)
            end_time   = self.time_end_test.get(scenario, 0)
            duration   = (end_time - start_time) * 1000  # in milliseconds
            out_rows   = self.output_rows.get(scenario, 0)
            in_rows    = self.data_rows.get(scenario, 0)
            throughput = (out_rows / (end_time - start_time)) if (end_time - start_time) > 0 else 0
            status     = self.status.get(scenario, "UNKNOWN")
            
            print(f"| {self.scenarioId[scenario]} | {self.classification[scenario]} | {out_rows:<13} | {in_rows:<10} | {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))} | {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))} | {duration:<12.2f} | {throughput:<17.2f} | {status} |")
        
    
    def start(self):
        self.time_start = time.time()    

    def start_write(self, name):
        self.time_start_write[name] = time.time()

    def start_test(self, name):
        self.time_start_test[name] = time.time()
        
    def end_write(self, name):
        self.time_end_write[name] = time.time()
        cost = self.time_end_write[name] - self.time_start_write[name]
        print(f"Write step '{name}' took {cost:.3f} seconds")

    def end_test(self, name):
        self.time_end_test[name] = time.time()
        cost = self.time_end_test[name] - self.time_start_test[name]
        print(f"Test step '{name}' took {cost:.3f} seconds")
    
    def end(self):
        self.time_end = time.time()
        total_cost = self.time_end - self.time_start
        print(f"Total execution time: {total_cost:.3f} seconds")
        
    def add_data_rows(self, name, rows):
        print(f"Total data rows written for '{name}': {rows}")
        if self.data_rows[name] is None:
            self.data_rows[name] = rows
        else:
            self.data_rows[name] += rows
        
metrics = OutMetrics()