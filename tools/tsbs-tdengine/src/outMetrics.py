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
from datetime import datetime
from outLog import log
from cmdLine import cmd
from outDB import OutDB

class Delay :
    def __init__(self, cnt=0 ,min=0, avg=0, p10=0, p50=0, p90=0, p95=0, p99=0, max=0):
        self.cnt = cnt
        self.min = min
        self.avg = avg
        self.p10 = p10
        self.p50 = p50
        self.p90 = p90
        self.p95 = p95
        self.p99 = p99
        self.max = max
class OutMetrics:
    def __init__(self):
        self.version          = "3.0"
        self.run_type         = "DAILY"
        self.time_start       = None
        self.time_start_write = {}
        self.time_start_test  = {}
        self.time_end_write   = {}
        self.time_end_test    = {}
        self.time_end         = None
        
        self.scenarioId  = {}
        self.classification  = {}
        self.data_rows   = {}
        self.output_rows = {}
        self.status      = {}
        self.delay       = {}     
    
    def init_metrics(self, metrics_file):
        log.out("Initializing metrics output")
        self.metrics_file = metrics_file
    
    def set_status(self,name, status):
        self.status[name] = status    
            
    def start(self):
        self.time_start = time.time()
        self.write_metrics(f"Test start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.time_start))}")
        self.write_metrics("Note: " + cmd.get_note())

    def start_write(self, name):
        self.time_start_write[name] = time.time()

    def start_test(self, name):
        self.time_start_test[name] = time.time()
        
    def end_write(self, name):
        self.time_end_write[name] = time.time()
        cost = self.time_end_write[name] - self.time_start_write[name]
        self.write_metrics(f"WRITE STEP '{name}' took {cost:.3f} seconds")

    def end_test(self, name):
        self.time_end_test[name] = time.time()
        cost = self.time_end_test[name] - self.time_start_test[name]
        self.write_metrics(f"TEST STEP '{name}' took {cost:.3f} seconds")
    
    def end(self):
        self.time_end = time.time()
        total_cost = self.time_end - self.time_start
        self.write_metrics(f"Total execution time: {total_cost:.3f} seconds")
        
    def add_data_rows(self, name, rows):
        log.out(f"Total data rows written for '{name}': {rows}")
        if self.data_rows[name] is None:
            self.data_rows[name] = rows
        else:
            self.data_rows[name] += rows
            
    def update_delay(self, name, i, delay):
        log.out(f"  i={i:03d} delay cnt {delay.cnt}, "
                                  f"avg {delay.avg:.2f}s, "
                                  f"min {delay.min:.2f}s, "
                                  f"p10 {delay.p10:.2f}s, "
                                  f"p50 {delay.p50:.2f}s, "
                                  f"p90 {delay.p90:.2f}s, "
                                  f"p95 {delay.p95:.2f}s, "
                                  f"p99 {delay.p99:.2f}s, "
                                  f"max {delay.max:.2f}s")
        self.delay[name] = delay
    
    def write_metrics(self, msg):
        log.out(msg)
        with open(self.metrics_file, 'a') as f:
            f.write(msg + '\n') 

    def output_metrics(self):
        log.out(f"Outputting metrics to {self.metrics_file}")
        succ = 0
        
        # Column widths
        col_widths = {
            'scenario': 8,
            'status': 8,
            'classif': 8,
            'out_rec': 10,
            'in_rec': 10,
            'start_time': 19,
            'end_time': 19,
            'dur': 10,
            'tp': 10,
            "delay_avg": 6,
            "delay_min": 6,
            "delay_p10": 6,
            "delay_p50": 6,
            "delay_p90": 6,
            "delay_p95": 6,
            "delay_p99": 6,
            "delay_max": 6
        }
        
        header_delay = ""
        if cmd.get_check_delay():
            header_delay = (
                f"| {'Avg(s)':>{col_widths['delay_avg']}} "
                f"| {'Min(s)':>{col_widths['delay_min']}} "
                f"| {'P10(s)':>{col_widths['delay_p10']}} "
                f"| {'P50(s)':>{col_widths['delay_p50']}} "
                f"| {'P90(s)':>{col_widths['delay_p90']}} "
                f"| {'P95(s)':>{col_widths['delay_p95']}} "
                f"| {'P99(s)':>{col_widths['delay_p99']}} "
                f"| {'Max(s)':>{col_widths['delay_max']}} "
            )
        
        # Header
        header = (
            f"| {'Scenario':<{col_widths['scenario']}} "
            f"| {'Status':<{col_widths['status']}} "
            f"| {'Classif':<{col_widths['classif']}} "
            f"| {'Out Rec':>{col_widths['out_rec']}} "
            f"| {'In Rec':>{col_widths['in_rec']}} "
            f"| {'Start Time':>{col_widths['start_time']}} "
            f"| {'End Time':>{col_widths['end_time']}} "
            f"| {'Dur(s)':>{col_widths['dur']}} "
            f"| {'TP(rec/s)':>{col_widths['tp']}} "
            f"{header_delay}"
            f"|"
        )
        
        separator_delay = ""
        if cmd.get_check_delay():
            separator_delay = (
                f"|{'-' * (col_widths['delay_avg'] + 2)}"
                f"|{'-' * (col_widths['delay_min'] + 2)}"
                f"|{'-' * (col_widths['delay_p10'] + 2)}"
                f"|{'-' * (col_widths['delay_p50'] + 2)}"
                f"|{'-' * (col_widths['delay_p90'] + 2)}"
                f"|{'-' * (col_widths['delay_p95'] + 2)}"
                f"|{'-' * (col_widths['delay_p99'] + 2)}"
                f"|{'-' * (col_widths['delay_max'] + 2)}"
            )
        
        # Separator line
        separator = (
            f"|{'-' * (col_widths['scenario'] + 2)}"
            f"|{'-' * (col_widths['status'] + 2)}"
            f"|{'-' * (col_widths['classif'] + 2)}"
            f"|{'-' * (col_widths['out_rec'] + 2)}"
            f"|{'-' * (col_widths['in_rec'] + 2)}"
            f"|{'-' * (col_widths['start_time'] + 2)}"
            f"|{'-' * (col_widths['end_time'] + 2)}"
            f"|{'-' * (col_widths['dur'] + 2)}"
            f"|{'-' * (col_widths['tp'] + 2)}"
            f"{separator_delay}"
            f"|"
        )
        
        self.write_metrics(header)
        self.write_metrics(separator)
        
        #
        # Json summary
        #
        metrics_data = {
            'version': self.version,
            'run_type': self.run_type,
            'run_start': datetime.fromtimestamp(self.time_start).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            'run_took': (self.time_end - self.time_start) if (self.time_end and self.time_start) else 0,
            'note': cmd.get_note(),
            'scenarios': []
        }
        
        # Data rows
        for scenario in self.scenarioId:
            start_time = self.time_start_write.get(scenario, 0)
            end_time   = self.time_end_test.get(scenario, 0)
            duration   = round(end_time - start_time, 2)  # s
            out_rows   = self.output_rows.get(scenario, 0)
            in_rows    = self.data_rows.get(scenario, 0)
            throughput = round((in_rows / duration) if duration > 0 else 0, 2)
            status     = self.status.get(scenario, "UNKNOWN")
            write_took = round(self.time_end_write.get(scenario, 0) - self.time_start_write.get(scenario, 0), 2)
            test_took = round(self.time_end_test.get(scenario, 0) - self.time_start_test.get(scenario, 0), 2)
            
            # count success
            if status == "Passed":
                succ += 1
            
            # Format time strings
            if start_time == 0 or start_time is None:
                start_time_str = "-"    
                duration = 0
            else:
                start_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
    
            if end_time == 0 or end_time is None:
                end_time_str = "-"
                duration = 0
            else:
                end_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))
            
            row_delay = ""
            if cmd.get_check_delay():
                delay = self.delay.get(scenario, Delay())
                row_delay = (
                    f"| {delay.avg:>{col_widths['delay_avg']}.2f} "
                    f"| {delay.min:>{col_widths['delay_min']}.2f} "
                    f"| {delay.p10:>{col_widths['delay_p10']}.2f} "
                    f"| {delay.p50:>{col_widths['delay_p50']}.2f} "
                    f"| {delay.p90:>{col_widths['delay_p90']}.2f} "
                    f"| {delay.p95:>{col_widths['delay_p95']}.2f} "
                    f"| {delay.p99:>{col_widths['delay_p99']}.2f} "
                    f"| {delay.max:>{col_widths['delay_max']}.2f} "
                )
            
            # Print row with fixed width
            row = (
                f"| {self.scenarioId[scenario]:<{col_widths['scenario']}} "
                f"| {status:<{col_widths['status']}} "
                f"| {self.classification[scenario]:<{col_widths['classif']}} "
                f"| {out_rows:>{col_widths['out_rec']}} "
                f"| {in_rows:>{col_widths['in_rec']}} "
                f"| {start_time_str:<{col_widths['start_time']}} "
                f"| {end_time_str:<{col_widths['end_time']}} "
                f"| {duration:>{col_widths['dur']}.2f} "
                f"| {throughput:>{col_widths['tp']}.0f} "
                f"{row_delay}"
                f"|"
            )
            self.write_metrics(row)
            
            #
            # json each scenario
            #
            json_data = {
                'scenario': self.scenarioId[scenario],
                'status': status,
                'classif': self.classification[scenario],
                'out_rec': out_rows,
                'in_rec': in_rows,
                'start_time': start_time_str,
                'end_time': end_time_str,
                'dur': duration,
                'tp': throughput,
                'write_took': write_took,
                'test_took': test_took
            }
            if cmd.get_check_delay():
                delay = self.delay.get(scenario, Delay())
                json_delay = {
                    'delay_avg': delay.avg,
                    'delay_min': delay.min,
                    'delay_p10': delay.p10,
                    'delay_p50': delay.p50,
                    'delay_p90': delay.p90,
                    'delay_p95': delay.p95,
                    'delay_p99': delay.p99,
                    'delay_max': delay.max
                }                
                json_data.update(json_delay)
            
            # append
            metrics_data['scenarios'].append(json_data)
        
        # End
        log.out("\nMetrics appand: %s" % self.metrics_file)
        
        # Save to single metrics json file to metrics/ subfolder
        
        # Create metrics directory
        metrics_dir = os.path.join(os.path.dirname(self.metrics_file), 'metrics')
        os.makedirs(metrics_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime())
        json_filename = f'tsbs_{timestamp}.json'
        json_filepath = os.path.join(metrics_dir, json_filename)
        
        # Write JSON to file
        try:
            with open(json_filepath, 'w', encoding='utf-8') as f:
                json.dump(metrics_data, f, indent=2, ensure_ascii=False)
            log.out(f"Metrics json saved: {json_filepath}")
        except Exception as e:
            log.out(f"Failed to save metrics JSON: {e}")
            
        # Write JSON to database if configured
        try:        
            out_db = cmd.get_out_db()
            if out_db and succ > 0:
                    db = OutDB()
                    db.write_metrics_to_db(metrics_data, out_db)
        except Exception as e:
            log.out(f"Failed to write metrics to database: {e}")     
        
metrics = OutMetrics()