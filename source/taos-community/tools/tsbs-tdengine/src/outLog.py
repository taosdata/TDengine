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


class OutLog:
    def __init__(self):
        self.log_file = "tsbs_tdengine.log"

    def init_log(self, log_file):
        self.log_file = log_file
        
    def outSuccess(self, message): 
        self.out("=" * 30)
        self.out(f"✓ {message}")
        self.out("=" * 30)        

    def out(self, message):
        print(message)
        # write to log file
        with open(self.log_file, 'a') as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} {message}\n")  

    def close(self):
        print("Log file is : %s \n\n" % self.log_file)
        
        
log = OutLog()