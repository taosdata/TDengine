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
import util

from baseStep import BaseStep
from outMetrics import metrics

class WriteData(BaseStep):
    def __init__(self, scene):
        self.scene = scene
        self.des_url = "taos://root:taosdata@localhost:6030/test"

    def taosx_import_csv(sef, desc, source, parser_file, batch_size):
        command = f"taosx run -t '{desc}' -f '{source}?batch_size={batch_size}' --parser '@{parser_file}'"
        print(f"Executing command: {command}")
        output, error, code = util.exe_cmd(command)
        print(f"Output: {output}")
        print(f"Error: {error}")
        print(f"Return code: {code}")
    
        return output, error, code

    def run(self):
        print("WriteData step executed")
        metrics.start_write(self.scene.name)
        for csv_file in self.scene.csv_files:
            print(f"Writing data from CSV file: {csv_file}")
            output, error, code = self.taosx_import_csv (
                desc = self.des_url,
                source = f"csv:" + csv_file,
                parser_file = csv_file .replace(".csv", ".taosx.json"),
                batch_size = 50000
            )
            
        metrics.end_write(self.scene.name)