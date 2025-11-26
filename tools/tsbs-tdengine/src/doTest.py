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
from outLog import log

class DoTest(BaseStep):
    def __init__(self, scene):
        self.scene = scene

    def wait_stream_end(self, verifySql, expectRows, compare, timeout, max_test_time):
        log.out("Waiting for stream processing to complete...")
        log.out(f"Verify SQL: {verifySql}, Expect Rows: {expectRows}, Max Test Time: {max_test_time} seconds, Timeout: {timeout} seconds")
        conn = taos_connect()
        cursor = conn.cursor()
        
        last_rows = 0
        cnt = 0
        i = 0
        while cnt < timeout and i < max_test_time:
            try:        
                cursor.execute(verifySql)
                results = cursor.fetchall()
                rows = results[0][0]
                if rows != last_rows:
                    # have new rows, reset cnt
                    last_rows = rows
                    cnt = 0
                    metrics.output_rows[self.scene.name] = rows                    
                
                # check passed
                passed = False
                if compare == ">=":
                    if rows >= expectRows:
                        passed = True
                elif compare == ">":
                    if rows > expectRows:
                        passed = True
                else:
                    compare = "="
                    if rows == expectRows:
                        passed = True
                                        
                # set status passed
                if passed:
                    log.out(f"{i} rows real: {rows} {compare} expect: {expectRows} ==> Passed")
                    conn.close()
                    metrics.set_status(self.scene.name, "Passed")
                    return

                log.out(f"{i} rows real: {rows}, expect: {expectRows}")
                
                # add step
                i += 1
                cnt += 1
            except Exception as e:
                log.out(f"i={i} query stream result table except: {e}")
                # add step
                i += 1
                cnt += 1
            
            if cmd.user_canceled:
                log.out("User canceled the operation during wait_stream_end.")
                conn.close()
                metrics.set_status(self.scene.name, "Canceled")
                return    

            # sleep 1 second
            time.sleep(1)            
    
        log.out(f"{i} real rows: {last_rows}, expect rows: {expectRows} ==> Not Passed")
        conn.close()
        if last_rows == 0:
            status = "No Data"
        else:
            status = "Timeout"
        metrics.set_status(self.scene.name, status)
    
    def read_verify_info(self, scenario_id, yaml_file):
        try:
            with open(yaml_file, 'r') as f:
                data = yaml.safe_load(f)
                cases = data.get("testCases", [])
                
                # Search for matching test case by scenarioId
                for case in cases:
                    if case.get('scenarioId') == scenario_id:
                        verify_sql  = case.get('verifySql')
                        expect_rows = case.get('expectRows')
                        compare     = case.get('compare')
                        log.out(f"Found verify info for scenario '{scenario_id}':")
                        log.out(f"  Verify SQL: {verify_sql}")
                        log.out(f"  Expect Rows: {expect_rows}")
                        return verify_sql, expect_rows, compare
                
                # If no matching scenario found
                log.out(f"No verify info found for scenario '{scenario_id}' in {yaml_file}")
                return None, None
                
        except FileNotFoundError:
            log.out(f"YAML file not found: {yaml_file}")
            return None, None, None
        except yaml.YAMLError as e:
            log.out(f"Error parsing YAML file: {e}")
            return None, None, None
        except Exception as e:
            log.out(f"Error reading verify info: {e}")
            return None, None, None

    def run(self):
        log.out("DoTest step executed")
        metrics.start_test(self.scene.name)
        # read table.yaml files to read verify sql and expect rows
        for table in self.scene.tables:
            yaml_file = self.scene.get_yaml_file(table)
            log.out(f"Processing YAML file: {yaml_file}")
            verifySql, expectRows, compare = self.read_verify_info(self.scene.name, yaml_file)
            if verifySql != None and expectRows != None:
                self.wait_stream_end(verifySql, expectRows, compare, timeout = cmd.timeout, max_test_time = cmd.max_test_time)
            else:
                log.out(f"verify sql is none, skipping  {yaml_file}")
        
        # mark test end time
        metrics.end_test(self.scene.name)