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
import yaml
import util

from tdengine import *
from baseStep import BaseStep
from outMetrics import metrics
from cmdLine import cmd
from outLog import log

class WriteData(BaseStep):
    def __init__(self, scene):
        self.scene = scene
        self.des_url = f"taos://{cmd.get_user()}:{cmd.get_password()}@{cmd.get_host()}:{cmd.get_port()}/"

    def taosx_import_csv(sef, desc, source, parser_file, batch_size):
        command = f"taosx run -t '{desc}' -f '{source}?batch_size={batch_size}' --parser '@{parser_file}'"
        log.out(f"Executing command: {command}")
        output, error, code = util.exe_cmd(command)
        log.out(f"Output: {output}")
        log.out(f"Error: {error}")
        log.out(f"Return code: {code}")
    
        return output, error, code
    
    def benchmark_insert(sef, json_file):
        command = f"taosBenchmark -f {json_file}"
        log.out(f"Executing command: {command}")
        output, error, code = util.exe_cmd(command)
        log.out(f"Output: {output}")
        log.out(f"Error: {error}")
        log.out(f"Return code: {code}")
    
        return output, error, code
    
    def read_data_info(self, scenario_id, yaml_file):
        try:
            with open(yaml_file, 'r') as f:
                data = yaml.safe_load(f)
                cases = data.get("testCases", [])
                
                # Search for matching test case by scenarioId
                for case in cases:
                    if case.get('scenarioId') == scenario_id:
                        query_sql = case.get('querySql')
                        data_rows = case.get('dataRows')
                        log.out(f"Found data info for scenario '{scenario_id}':")
                        log.out(f"  Query SQL: {query_sql}")
                        log.out(f"  data Rows: {data_rows}")
                        return query_sql, data_rows
                
                # If no matching scenario found
                log.out(f"No verify info found for scenario '{scenario_id}' in {yaml_file}")
                return None, None
                
        except FileNotFoundError:
            log.out(f"YAML file not found: {yaml_file}")
            return None, None
        except yaml.YAMLError as e:
            log.out(f"Error parsing YAML file: {e}")
            return None, None
        except Exception as e:
            log.out(f"Error reading verify info: {e}")
            return None, None 
   
    def insert_data(self, table):
        if cmd.get_check_delay():
            # benchmark
            json_file = self.scene.get_json_file(table)
            output, error, code = self.benchmark_insert(json_file)
        else:
            # taosX     
            csv_file = self.scene.get_csv_file(table)
            output, error, code = self.taosx_import_csv (
                desc = self.des_url + self.scene.db_name[table],
                source = f"csv:" + csv_file,
                parser_file = csv_file .replace(".csv", ".taosx.json"),
                batch_size = 50000
            )                

    def run(self):
        log.out("WriteData step executed")
        metrics.start_write(self.scene.name)
        
        # loop tables
        for table in self.scene.tables:
            self.insert_data(table)
            
        metrics.end_write(self.scene.name)
        
        # verify expect rows
        for table in self.scene.tables:
            yaml_file = self.scene.get_yaml_file(table)
            querySql, dataRows = self.read_data_info(self.scene.name, yaml_file)
            if querySql == None or dataRows == None:
                log.out(f"data sql is none, skipping  {yaml_file}")
                metrics.status[self.scene.name] = "Failed"                
                continue
            realRows = db_first_value(querySql)
            # set data rows metric
            metrics.add_data_rows(self.scene.name, realRows)
            if realRows == dataRows:
                log.out(f"data write completed. real rows: {realRows}, expect rows: {dataRows}")
            else:
                log.out(f"data write error. real rows: {realRows}, expect rows: {dataRows}")
                metrics.set_status(self.scene.name, "Failed")
                return False
        
        return True
            
            
            