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


class Scene:
    def __init__(self, scenario, sql, config_path, data_path):
        self.scenario = scenario
        self.sql = sql
        tables = self.parser_tables(sql)
        self.generate_filename(tables, config_path, data_path)
        
    def parser_tables(self, sql):
        pass    
    
    # Generate file names for CSV, SQL, and YAML files based on table names
    def generate_filename(self, tables, config_path, data_path):
        self.csv_files = []
        self.sql_files = []
        self.yarml_files = []
        for table in tables:
            filename = os.path.join(data_path, f"{table}.csv")
            self.csv_files.append(filename)
            filename = os.path.join(config_path, f"{table}.sql")
            self.sql_files.append(filename)
            filename = os.path.join(config_path, f"{table}.yaml")
            self.yarml_files.append(filename)
    