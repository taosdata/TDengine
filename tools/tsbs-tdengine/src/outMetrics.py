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
from outLog import log

class OutMetrics:
    def __init__(self):
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
    
    def init_metrics(self, metrics_file):
        log.out("Initializing metrics output")
        self.metrics_file = metrics_file
    
    def set_status(self,name, status):
        self.status[name] = status    
            
    def start(self):
        self.time_start = time.time()
        self.write_log(f"Test start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.time_start))}")

    def start_write(self, name):
        self.time_start_write[name] = time.time()

    def start_test(self, name):
        self.time_start_test[name] = time.time()
        
    def end_write(self, name):
        self.time_end_write[name] = time.time()
        cost = self.time_end_write[name] - self.time_start_write[name]
        self.write_log(f"Write step '{name}' took {cost:.3f} seconds")

    def end_test(self, name):
        self.time_end_test[name] = time.time()
        cost = self.time_end_test[name] - self.time_start_test[name]
        self.write_log(f"Test step '{name}' took {cost:.3f} seconds")
    
    def end(self):
        self.time_end = time.time()
        total_cost = self.time_end - self.time_start
        self.write_log(f"Total execution time: {total_cost:.3f} seconds")
        
    def add_data_rows(self, name, rows):
        log.out(f"Total data rows written for '{name}': {rows}")
        if self.data_rows[name] is None:
            self.data_rows[name] = rows
        else:
            self.data_rows[name] += rows
    
    def write_log(self, msg):
        log.out(msg)
        with open(self.metrics_file, 'a') as f:
            f.write(msg + '\n')        

    def output_metrics(self):
        log.out(f"Outputting metrics to {self.metrics_file}")
        
        # Column widths
        col_widths = {
            'scenario_id': 15,
            'classification': 16,
            'out_records': 13,
            'in_records': 12,
            'start_time': 19,
            'end_time': 19,
            'duration': 14,
            'throughput': 19,
            'status': 8
        }
        
        # Header
        header = (
            f"| {'Scenario ID':<{col_widths['scenario_id']}} "
            f"| {'Classification':<{col_widths['classification']}} "
            f"| {'Out Records':>{col_widths['out_records']}} "
            f"| {'In Records':>{col_widths['in_records']}} "
            f"| {'Start Time':<{col_widths['start_time']}} "
            f"| {'End Time':<{col_widths['end_time']}} "
            f"| {'Duration(ms)':>{col_widths['duration']}} "
            f"| {'Throughput(rec/s)':>{col_widths['throughput']}} "
            f"| {'Status':<{col_widths['status']}} |"
        )
        
        # Separator line
        separator = (
            f"|{'-' * (col_widths['scenario_id'] + 2)}"
            f"|{'-' * (col_widths['classification'] + 2)}"
            f"|{'-' * (col_widths['out_records'] + 2)}"
            f"|{'-' * (col_widths['in_records'] + 2)}"
            f"|{'-' * (col_widths['start_time'] + 2)}"
            f"|{'-' * (col_widths['end_time'] + 2)}"
            f"|{'-' * (col_widths['duration'] + 2)}"
            f"|{'-' * (col_widths['throughput'] + 2)}"
            f"|{'-' * (col_widths['status'] + 2)}|"
        )
        
        self.write_log(header)
        self.write_log(separator)
        
        # Collect metrics data for JSON output
        metrics_data = {
            'test_start_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.time_start)) if self.time_start else None,
            'test_end_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.time_end)) if self.time_end else None,
            'total_duration_seconds': (self.time_end - self.time_start) if (self.time_end and self.time_start) else 0,
            'scenarios': []
        }
        
        # Data rows
        for scenario in self.scenarioId:
            start_time = self.time_start_write.get(scenario, 0)
            end_time   = self.time_end_test.get(scenario, 0)
            duration   = (end_time - start_time) * 1000  # ms
            out_rows   = self.output_rows.get(scenario, 0)
            in_rows    = self.data_rows.get(scenario, 0)
            throughput = (in_rows / duration * 1000) if duration > 0 else 0
            status     = self.status.get(scenario, "UNKNOWN")
            
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
            
            # Print row with fixed width
            row = (
                f"| {self.scenarioId[scenario]:<{col_widths['scenario_id']}} "
                f"| {self.classification[scenario]:<{col_widths['classification']}} "
                f"| {out_rows:>{col_widths['out_records']}} "
                f"| {in_rows:>{col_widths['in_records']}} "
                f"| {start_time_str:<{col_widths['start_time']}} "
                f"| {end_time_str:<{col_widths['end_time']}} "
                f"| {duration:>{col_widths['duration']}.0f} "
                f"| {throughput:>{col_widths['throughput']}.2f} "
                f"| {status:<{col_widths['status']}} |"
            )
            self.write_log(row)
            
            # Add to JSON data
            scenario_data = {
                'scenario_id': self.scenarioId[scenario],
                'classification': self.classification[scenario],
                'out_records': out_rows,
                'in_records': in_rows,
                'start_time': start_time_str,
                'end_time': end_time_str,
                'duration_ms': round(duration, 0),
                'throughput_rec_per_sec': round(throughput, 2),
                'status': status
            }
            metrics_data['scenarios'].append(scenario_data)
        
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
        
        # Write JSON file
        try:
            with open(json_filepath, 'w', encoding='utf-8') as f:
                json.dump(metrics_data, f, indent=2, ensure_ascii=False)
            log.out(f"Metrics json saved: {json_filepath}")
        except Exception as e:
            log.out(f"Failed to save metrics JSON: {e}")
        
        
        
metrics = OutMetrics()