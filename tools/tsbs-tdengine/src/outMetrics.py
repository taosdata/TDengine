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
    
    def init_metrics(self, metrics_file):
        print("Initializing metrics output")
        self.metrics_file = metrics_file
        
    def output_metrics(self):
        print(f"Outputting metrics to {self.metrics_file}")    
    
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
        
        
metrics = OutMetrics()