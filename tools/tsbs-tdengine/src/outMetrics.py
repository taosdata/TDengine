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


class OutMetrics:
    def __init__(self):
        pass
    
    def init_metrics(self, metrics_file):
        print("Initializing metrics output")
        self.metrics_file = metrics_file
        
    def output_metrics(self):
        print(f"Outputting metrics to {self.metrics_file}")    

        
metrics = OutMetrics()