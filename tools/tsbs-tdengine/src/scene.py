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
    def __init__(self, name, sql, classification, config_path, data_path):
        self.name = name
        self.sql  = sql
        self.classification = classification
        self.db_name     = {}
        self.config_path = config_path
        self.data_path   = data_path

        self.tables = self.find_tables_from_sql(sql)
        self.generate_filename(self.tables, config_path, data_path)
        
    # find table from sql
    def find_tables_from_sql(self, sql):
        # find keyword 'from' in sql, get the table name after it
        tables = []
        sql_lower = sql.lower()
        tokens = sql_lower.split()
        for i in range(len(tokens)):
            if tokens[i] == 'from' and i + 1 < len(tokens):
                table = tokens[i + 1]
                # remove any trailing semicolon or comma
                table = table.rstrip(';,')
                names = table.split(".")
                db    = names[0]
                table = names[-1]
                # skip
                if table[0] == "(" or table[0] == "'" or table[0] == '"' or len(table) < 2:
                    continue
                if table[0] == "%":  # skip special table like %%tbname
                    continue
                
                # put paired db and table
                self.db_name[table] = db
                
                # append
                if table not in tables:
                    tables.append(table)

        print(f"Found tables : {tables} scene:{self.name}")
        return tables 
    
    # Generate file names for CSV, SQL, and YAML files based on table names
    def generate_filename(self, tables, config_path, data_path):
        self.csv_files = []
        self.sql_files = []
        self.yaml_files = []
        for table in tables:
            filename = os.path.join(data_path, f"{table}.csv")
            self.csv_files.append(filename)
            filename = os.path.join(config_path, f"{table}.sql")
            self.sql_files.append(filename)
            filename = os.path.join(config_path, f"{table}.yaml")
            self.yaml_files.append(filename)

    def get_csv_file(self, table):
            return os.path.join(self.data_path, f"{table}.csv")

    def get_sql_file(self, table):
            return os.path.join(self.config_path, f"{table}.sql")

    def get_yaml_file(self, table):
            return os.path.join(self.config_path, f"{table}.yaml")