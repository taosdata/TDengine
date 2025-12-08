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
from outMetrics import metrics, Delay
from outLog import log

class DoTest(BaseStep):
    def __init__(self, scene):
        self.scene = scene
        
    def total_delay(self, cursor, delaySql, i):
        try:
            cursor.execute(delaySql)
            results = cursor.fetchall()
            row = results[0]
            delay = Delay()
            delay.cnt = row[0]
            delay.avg = round(row[1]/1000, 2)
            delay.min = round(row[2]/1000, 2)
            delay.p10 = round(row[3]/1000, 2)
            delay.p50 = round(row[4]/1000, 2)
            delay.p90 = round(row[5]/1000, 2)
            delay.p95 = round(row[6]/1000, 2)
            delay.p99 = round(row[7]/1000, 2)
            delay.max = round(row[8]/1000, 2)
            metrics.update_delay(self.scene.name, i, delay)
        except Exception as e:
            log.out(f"  {i} total_delay except: {e}")

    def wait_stream_end(self, verifySql, expectRows, compare, delaySql, equalSql, diffRows, timeout, max_test_time):
        log.out("Waiting for stream processing to complete...")
        conn = taos_connect()
        cursor = conn.cursor()
        
        if expectRows is None and equalSql is not None:
            start = time.time()
            cursor.execute(equalSql)
            results = cursor.fetchall()
            expectRows = results[0][0]
            cost = time.time() - start
            metrics.write_metrics(f"  Equal SQL took: {cost:.3f} seconds")
            log.out(f"  Equal SQL: {equalSql}")
            log.out(f"  Get Rows: {expectRows} diffRows: {diffRows}")
            
            if expectRows == 0:
                log.out(f"Expect Rows is 0, skip wait stream.")
                conn.close()
                metrics.set_status(self.scene.name, "Failed")
                return

            if diffRows is not None:
                expectRows += diffRows
        
        log.out(f"Verify SQL: {verifySql}, Expect Rows: {expectRows}, Max Test Time: {max_test_time} seconds, Timeout: {timeout} seconds")
        
        last_rows = 0
        cnt = 0
        i = 0
        while cnt < timeout and i < max_test_time:
            try:        
                cursor.execute(verifySql)
                results = cursor.fetchall()
                nrows = results[0][0]
                if nrows != last_rows:
                    # have new rows, reset cnt
                    last_rows = nrows
                    cnt = 0
                    metrics.output_rows[self.scene.name] = nrows
                
                # check passed
                passed = False
                if compare == ">=":
                    if nrows >= expectRows:
                        passed = True
                elif compare == ">":
                    if nrows > expectRows:
                        passed = True
                else:
                    compare = "="
                    if nrows == expectRows:
                        passed = True
                                        
                # set status passed
                if passed:
                    log.out(f"  {i} rows real: {nrows} {compare} expect: {expectRows} ==> Passed")
                    conn.close()
                    metrics.set_status(self.scene.name, "Passed")
                    return

                # total delay
                if cmd.get_check_delay():
                    self.total_delay(cursor, delaySql, i)
                else:
                    log.out(f"  {i} rows real: {nrows}, expect: {expectRows}")
                
                # add step
                i += 1
                cnt += 1
            except Exception as e:
                log.out(f"  {i} query stream result table except: {e}")
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
                        delay_sql   = case.get('delaySql')
                        equal_sql   = case.get('equalSql')
                        diffRows    = case.get('diffRows')
                        
                        log.out(f"Found verify info for scenario '{scenario_id}':")
                        log.out(f"  Verify SQL: {verify_sql}")
                        log.out(f"  Expect Rows: {expect_rows}")
                        log.out(f"  Compare: {compare}")
                        log.out(f"  Equal SQL: {equal_sql}")
                        log.out(f"  Diff Rows: {diffRows}")
                        return verify_sql, expect_rows, compare, delay_sql, equal_sql, diffRows
                
                # If no matching scenario found
                log.out(f"No verify info found for scenario '{scenario_id}' in {yaml_file}")
                return None, None, None, None
                
        except FileNotFoundError:
            log.out(f"YAML file not found: {yaml_file}")
            return None, None, None, None
        except yaml.YAMLError as e:
            log.out(f"Error parsing YAML file: {e}")
            return None, None, None, None
        except Exception as e:
            log.out(f"Error reading verify info: {e}")
            return None, None, None, None

    def run(self):
        log.out("DoTest step executed")
        metrics.start_test(self.scene.name)
        # read table.yaml files to read verify sql and expect rows
        for table in self.scene.tables:
            yaml_file = self.scene.get_yaml_file(table)
            log.out(f"Processing YAML file: {yaml_file}")
            verifySql, expectRows, compare, delaySql, equalSql, diffRows = self.read_verify_info(self.scene.name, yaml_file)
            if verifySql != None and ( expectRows is not None or equalSql is not None):
                self.wait_stream_end(verifySql, 
                                     expectRows,
                                     compare, 
                                     delaySql,
                                     equalSql,
                                     diffRows,
                                     timeout = cmd.timeout, 
                                     max_test_time = cmd.max_test_time)
            else:
                log.out(f"verify sql is none, skipping  {yaml_file}")
        
        # mark test end time
        metrics.end_test(self.scene.name)
        log.outSuccess("doTest finished!")