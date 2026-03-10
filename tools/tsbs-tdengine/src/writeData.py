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
from concurrent.futures import ThreadPoolExecutor, as_completed

from tdengine import *
from baseStep import BaseStep
from outMetrics import metrics
from cmdLine import cmd
from outLog import log

class WriteData(BaseStep):
    def __init__(self, scene):
        self.scene = scene
        self.des_url = f"taos://{cmd.get_user()}:{cmd.get_password()}@{cmd.get_host()}:{cmd.get_port()}/"

    def taosx_import_csv(self, desc, source, parser_file, batch_size):
        command = f"taosx run -t '{desc}' -f '{source}?batch_size={batch_size}' --parser '@{parser_file}'"
        log.out(f"Executing command: {command}")
        output, error, code = util.exe_cmd(command)
        log.out(f"Output: {output}")
        log.out(f"Error: {error}")
        log.out(f"Return code: {code}")
    
        return output, error, code
    
    def benchmark_insert(self, json_file):
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
                        log.out(f"Found data info:")
                        log.out(f"  Query SQL: {query_sql}")
                        log.out(f"  data Rows: {data_rows}")
                        return query_sql, data_rows
                
                # If no matching scenario found
                log.out(f"No verify info found in {yaml_file}")
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
        """Insert data for a single table"""
        try:
            log.out(f"Starting data insertion for table: {table}")
            
            if cmd.get_check_delay():
                # benchmark
                json_file = self.scene.get_json_file(table)
                output, error, code = self.benchmark_insert(json_file)
            else:
                # taosX     
                csv_file = self.scene.get_csv_file(table)
                output, error, code = self.taosx_import_csv(
                    desc = self.des_url + self.scene.db_name[table],
                    source = f"csv:" + csv_file,
                    parser_file = csv_file.replace(".csv", ".taosx.json"),
                    batch_size = 50000
                )
            
            log.out(f"Completed data insertion for table: {table}, return code: {code}")
            return table, code, None
            
        except Exception as e:
            log.out(f"Error inserting data for table {table}: {e}")
            return table, -1, str(e)

    def insert_data_serial(self, tables):
        """
        Insert data for tables serially (one by one)
        Args:
            tables: List of table names
        Returns:
            Dictionary of results {table: (return_code, error)}
        """
        log.out(f"Starting serial data insertion for {len(tables)} table(s)")
        results = {}
        
        for i, table in enumerate(tables, 1):
            table_name, return_code, error = self.insert_data(table)
            results[table_name] = (return_code, error)
            log.out(f"Progress: {i}/{len(tables)} tables completed")
        
        log.out(f"All {len(tables)} table(s) insertion completed (serial)")
        return results

    def insert_data_concurrent(self, tables, max_workers=None):
        """
        Insert data for multiple tables concurrently
        Args:
            tables: List of table names
            max_workers: Maximum number of concurrent workers (default: number of tables or CPU count)
        Returns:
            Dictionary of results {table: (return_code, error)}
        """
        if not tables:
            log.out("No tables to insert")
            return {}
        
        # Determine number of workers
        if max_workers is None:
            max_workers = min(len(tables), os.cpu_count() or 4)
        
        log.out(f"Starting concurrent data insertion for {len(tables)} tables with {max_workers} workers")
        
        results = {}
        
        # Use ThreadPoolExecutor for concurrent execution
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_table = {
                executor.submit(self.insert_data, table): table 
                for table in tables
            }
            
            # Wait for all tasks to complete and collect results
            completed = 0
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    table_name, return_code, error = future.result()
                    results[table_name] = (return_code, error)
                    completed += 1
                    log.out(f"Progress: {completed}/{len(tables)} tables completed")
                except Exception as e:
                    log.out(f"Exception occurred for table {table}: {e}")
                    results[table] = (-1, str(e))
                    completed += 1
        
        log.out(f"All {len(tables)} tables insertion completed (concurrent)")
        return results

    def run(self):
        log.out("WriteData step executed")
        metrics.start_write(self.scene.name)
        
        # Check number of tables and choose insertion method
        num_tables = len(self.scene.tables)
        
        if num_tables == 0:
            log.out("No tables to insert")
            metrics.end_write(self.scene.name)
            return True
        elif num_tables == 1:
            # Single table - use serial insertion
            log.out(f"Single table detected, using serial insertion")
            results = self.insert_data_serial(self.scene.tables)
        else:
            # Multiple tables - use concurrent insertion
            log.out(f"Multiple tables ({num_tables}) detected, using concurrent insertion")
            results = self.insert_data_concurrent(self.scene.tables, max_workers = num_tables)
        
        # Check if any insertions failed
        failed_tables = [table for table, (code, error) in results.items() if code != 0]
        if failed_tables:
            log.out(f"Warning: {len(failed_tables)} table(s) failed to insert: {failed_tables}")
            for table in failed_tables:
                code, error = results[table]
                log.out(f"  Table '{table}': code={code}, error={error}")
            
        metrics.end_write(self.scene.name)
        
        success_count = len(self.scene.tables) - len(failed_tables)
        log.outSuccess(f"Write data finished! Success: {success_count}/{len(self.scene.tables)}")
        
        # Verify expect rows
        for table in self.scene.tables:
            yaml_file = self.scene.get_yaml_file(table)
            querySql, dataRows = self.read_data_info(self.scene.name, yaml_file)
            if querySql == None or dataRows == None:
                log.out(f"data sql is none, skipping {yaml_file}")
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


