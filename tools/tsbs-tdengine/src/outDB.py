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
import json
import taos
import datetime
from datetime import datetime
from outLog import log
from tdengine import taos_connect


class OutDB:
    def __init__(self):
        pass
    
    def create_db_and_stable(self, cursor):
        cursor.execute("CREATE DATABASE IF NOT EXISTS stream_bench")
        cursor.execute("USE stream_bench")
        
        create_stb_sql = """
        CREATE STABLE IF NOT EXISTS stb_tp(
            start_time TIMESTAMP, 
            scenario VARCHAR(32), 
            status VARCHAR(32), 
            classif VARCHAR(32), 
            out_rec INT, 
            in_rec INT, 
            end_time TIMESTAMP, 
            dur FLOAT, 
            tp FLOAT, 
            delay_avg INT, 
            delay_min FLOAT, 
            delay_p10 FLOAT, 
            delay_p50 FLOAT, 
            delay_p90 FLOAT, 
            delay_p95 FLOAT, 
            delay_p99 FLOAT, 
            delay_max FLOAT, 
            write_took FLOAT, 
            test_took FLOAT
        ) 
        TAGS(
            run_start TIMESTAMP, 
            run_took FLOAT, 
            test_note VARCHAR(128)
        )
        """
        cursor.execute(create_stb_sql)
        log.out("Super table stb_tp created or already exists")
    
    def write_metrics_to_db(self, metrics_data, out_db):
        """
        Write metrics to TDengine database
        Args:
            metrics_data: JSON metrics data
            out_db: Connection string format "host,port,user,password"
        """
        if not out_db:
            log.out("No output database configured, skipping database write")
            return
        
        try:
            #
            # connect db
            #
            
            # Parse connection string
            parts = out_db.split(',')
            if len(parts) < 1:
                log.out(f"Invalid out_db format. Expected: host, [port, user, password]")
                return
            
            host = parts[0].strip()
            port = int(parts[1].strip()) if len(parts) >= 2 else None
            user = parts[2].strip() if len(parts) >= 3 else None
            pwd  = parts[3].strip() if len(parts) >= 4 else None

            # Build connection parameters
            conn_params = {
                'host': host
            }

            # Add optional parameters only if they are not None
            if len(parts) >= 2:
                conn_params['port'] = int(parts[1].strip()) 
            if len(parts) >= 3:
                conn_params['user'] = parts[2].strip()
            if len(parts) >= 4:
                conn_params['password'] = parts[3].strip()

            # Connect with dynamic parameters
            log.out(f"Connecting to TDengine at {host} with parameters: {conn_params}")
            conn = taos.connect(**conn_params)
            cursor = conn.cursor()
            
            # Create database and super table if not exist
            self.create_db_and_stable(cursor)
            
            # Generate subtable name from run_start timestamp
            run_start_str = metrics_data.get('run_start', '')
            if not run_start_str:
                log.out("Missing run_start in metrics_data")
                cursor.close()
                conn.close()
                return
            
            #
            # create child table
            #
            
            # Convert run_start to timestamp format for subtable name
            # Format: tp_20250115_103045_123
            try:
                dt = datetime.strptime(run_start_str, '%Y-%m-%d %H:%M:%S.%f')
                subtable_name = f"tp_{dt.strftime('%Y%m%d_%H%M%S')}"
            except:
                # Fallback: use current timestamp
                subtable_name = f"tp_{int(time.time())}"

            # Prepare tag values
            run_took = metrics_data.get('run_took', 0)
            test_note = metrics_data.get('note', '')
            
            log.out(f"Using subtable: {subtable_name}")
            sql = f"create table {subtable_name} using stb_tp tags('{run_start_str}', {run_took}, '{test_note}')"
            cursor.execute(sql)
            
            # Insert data for each scenario
            scenarios = metrics_data.get('scenarios', [])
            inserted_count = 0
            
            for scenario in scenarios:
                try:
                    # Extract values from scenario data
                    start_time = scenario.get('start_time', '')
                    scenario_name = scenario.get('scenario', '')
                    status = scenario.get('status', '')
                    classif = scenario.get('classif', '')
                    out_rec = scenario.get('out_rec', 0)
                    in_rec = scenario.get('in_rec', 0)
                    end_time = scenario.get('end_time', '')
                    dur = scenario.get('dur', 0)
                    tp = scenario.get('tp', 0)
                    
                    if status != "Passed":
                        log.out(f"Skipping save db scenario '{scenario_name}' with status '{status}'")
                        continue
                    
                    # Delay metrics (optional)
                    delay_avg = scenario.get('delay_avg', 0)
                    delay_min = scenario.get('delay_min', 0)
                    delay_p10 = scenario.get('delay_p10', 0)
                    delay_p50 = scenario.get('delay_p50', 0)
                    delay_p90 = scenario.get('delay_p90', 0)
                    delay_p95 = scenario.get('delay_p95', 0)
                    delay_p99 = scenario.get('delay_p99', 0)
                    delay_max = scenario.get('delay_max', 0)
                    
                    write_took = scenario.get('write_took', 0)
                    test_took = scenario.get('test_took', 0)
                    
                    # Convert time strings to TDengine timestamp format
                    if start_time == '-' or not start_time:
                        start_time = 'NULL'
                    else:
                        start_time = f"'{start_time}'"
                    
                    if end_time == '-' or not end_time:
                        end_time = 'NULL'
                    else:
                        end_time = f"'{end_time}'"
                    
                    # Build INSERT SQL using automatic subtable creation
                    insert_sql = f"""
                        INSERT INTO {subtable_name} 
                        VALUES(
                            {start_time},
                            '{scenario_name}',
                            '{status}',
                            '{classif}',
                            {out_rec},
                            {in_rec},
                            {end_time},
                            {dur},
                            {tp},
                            {delay_avg},
                            {delay_min},
                            {delay_p10},
                            {delay_p50},
                            {delay_p90},
                            {delay_p95},
                            {delay_p99},
                            {delay_max},
                            {write_took},
                            {test_took}
                        )
                    """
                    
                    cursor.execute(insert_sql)
                    inserted_count += 1
                    
                except Exception as e:
                    log.out(f"Failed to insert scenario '{scenario_name}': {e}")
                    continue
            
            log.out(f"Successfully inserted {inserted_count}/{len(scenarios)} scenarios to database")
            
            # Close connection
            cursor.close()
            conn.close()
            
        except Exception as e:
            log.out(f"Error writing metrics to database: {e}")
            import traceback
            traceback.print_exc()