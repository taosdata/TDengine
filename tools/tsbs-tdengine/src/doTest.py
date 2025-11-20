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
import taos
import time
import yaml

from tdengine import *
from cmdLine import cmd
from scene import Scene
from baseStep import BaseStep
from outMetrics import metrics

class DoTest(BaseStep):
    def __init__(self, scene):
        self.scene = scene

    def wait_stream_end(self, verifySql, expectRows, timeout):
        print("Waiting for stream processing to complete...")
        print(f"Verify SQL: {verifySql}, Expect Rows: {expectRows}, Timeout: {timeout} seconds")
        conn = taos_connect()
        cursor = conn.cursor()
        
        last_rows = 0
        cnt = 0
        i = 0
        while cnt < timeout:
            try:        
                cursor.execute(verifySql)
                results = cursor.fetchall()
                rows = results[0][0]
                if rows != last_rows:
                    # have new rows, reset cnt
                    last_rows = rows
                    cnt = 0
                    metrics.output_rows[self.scene.name] = rows                    
                
                if rows == expectRows:
                    print(f"{i} real rows: {rows}, expect rows: {expectRows} ==> Passed")
                    conn.close()
                    metrics.set_status(self.scene.name, "Passed")
                    return
                print(f"{i} real rows: {rows}, expect rows: {expectRows}")
                
                # add step
                i += 1
                cnt += 1
            except Exception as e:
                print(f"i={i} query stream result table except: {e}")
            # sleep 1 second
            time.sleep(1)

        info = f"stream processing not completed in {timeout} seconds"
        print(info)
        conn.close()
        metrics.set_status(self.scene.name, "Timeout")
        #raise Exception(info)
    
    def read_verify_info(self, scenario_id, yaml_file):
        try:
            with open(yaml_file, 'r') as f:
                data = yaml.safe_load(f)
                cases = data.get("testCases", [])
                
                # Search for matching test case by scenarioId
                for case in cases:
                    if case.get('scenarioId') == scenario_id:
                        verify_sql = case.get('verifySql')
                        expect_rows = case.get('expectRows')
                        print(f"Found verify info for scenario '{scenario_id}':")
                        print(f"  Verify SQL: {verify_sql}")
                        print(f"  Expect Rows: {expect_rows}")
                        return verify_sql, expect_rows
                
                # If no matching scenario found
                print(f"No verify info found for scenario '{scenario_id}' in {yaml_file}")
                return None, None
                
        except FileNotFoundError:
            print(f"YAML file not found: {yaml_file}")
            return None, None
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file: {e}")
            return None, None
        except Exception as e:
            print(f"Error reading verify info: {e}")
            return None, None

    def run(self):
        print("DoTest step executed")
        metrics.start_test(self.scene.name)
        # read table.yaml files to read verify sql and expect rows
        for table in self.scene.tables:
            yaml_file = self.scene.get_yaml_file(table)
            print(f"Processing YAML file: {yaml_file}")
            verifySql, expectRows = self.read_verify_info(self.scene.name, yaml_file)
            if verifySql != None and expectRows != None:
                self.wait_stream_end(verifySql, expectRows, timeout = cmd.timeout)
            else:
                print(f"verify sql is none, skipping  {yaml_file}")
        
        # mark test end time
        metrics.end_test(self.scene.name)