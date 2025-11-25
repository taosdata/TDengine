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

import argparse
import sys
import os
import yaml

from scene import Scene
from util import *
from outLog import log

VERSION = "1.0.0"

class CmdLine:
    """
    Command line argument handler for TSBS TDengine Tool.
    This class encapsulates all command-line parameter processing.
    """
    
    def __init__(self):
        # default
        self.config_path = relative_path('../resource/config/')
        self.data_path   = relative_path('../resource/data/')
        self.host        = "localhost"
        self.port        = 6030
        self.user        = "root"
        self.password    = "taosdata"
        self.timeout     = 120 # seconds
        self.max_test_time = 1800 # seconds
        self.use_previous = False
        self.user_canceled = False
        
        # args
        self.parser = None
        self.args = None
        self._setup_parser()
    
    def _setup_parser(self):
        """Setup argument parser with all supported parameters"""
        self.parser = argparse.ArgumentParser(
            prog='tsbs-tdengine',
            description='TSBS TDengine Testing Tool - Performance testing tool for TDengine using TSBS dataset',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog='Example usage:\n  tsbs-tdengine -c ./config/cases.yaml -d ./data/readings.csv -s cpu-memory -p 8'
        )
        
        # Config file
        self.parser.add_argument(
            '-c', '--config',
            type=str,
            default=None,
            metavar='PATH',
            help='Test case configuration file path (default: built-in config)'
        )
        
        # Data file
        self.parser.add_argument(
            '-d', '--data',
            type=str,
            default=None,
            metavar='PATH',
            help='Readings data file path (default: built-in data)'
        )

        # Timeout
        self.parser.add_argument(
            '-t', '--timeout',
            type=int,
            default=None,
            metavar='SECONDS',
            help='Timeout in seconds for each case (default: 120)'
        )        

        # Max Test Time
        self.parser.add_argument(
            '-m', '--max-test-time',
            type=int,
            default=None,
            metavar='SECONDS',
            help='Maximum test time in seconds for each case (default: 1800)'
        )  
        
        # Log output
        self.parser.add_argument(
            '-l', '--log-output',
            type=str,
            default='./tsbs-tdengine-log.txt',
            metavar='PATH',
            help='Log file output path (default: ./tsbs-tdengine-log.txt)'
        )
        
        # JSON output
        self.parser.add_argument(
            '-j', '--json-output',
            type=str,
            default='./tsbs-tdengine-result.json',
            metavar='PATH',
            help='JSON result file output path (default: ./tsbs-tdengine-result.json)'
        )
        
        # Scenario
        self.parser.add_argument(
            '-s', '--scenario',
            type=str,
            default=None,
            metavar='SCENARIO',
            help='Execute specific test scenario (default: all scenarios)'
        )
        
        # use-previous
        self.parser.add_argument(
            '-P', '--use-previous',
            type=bool,
            default=None,
            metavar='NUM',
            help='No create new cluster and use previous cluster (default: False)'
        )
        
        # Version
        self.parser.add_argument(
            '-v', '--version',
            action='version',
            version=f'TSBS TDengine Tool v{VERSION}'
        )
    
    # Getter methods for all parameters
    
    def get_config(self):
        return self.args.config
    
    def get_data(self):
        return self.args.data
    
    def get_host(self):
        return self.host

    def get_port(self):
        return self.port
    
    def get_user(self):
        return self.user
    
    def get_password(self):
        return self.password
    
    def get_log_output(self):
        """Get log output file path"""
        return self.args.log_output
    
    def get_json_output(self):
        """Get JSON output file path"""
        return self.args.json_output
    
    def get_scenario(self):
        """Get scenario name, returns None if all scenarios should run"""
        return self.args.scenario
    
    def show_config(self):
        """Print current configuration"""
        log.out("\n=== Current Configuration ===")
        log.out(f"Config File:    {self.args.config or 'Built-in default'}")
        log.out(f"Data File:      {self.args.data or 'Built-in default'}")
        log.out(f"Log Output:     {self.args.log_output}")
        log.out(f"JSON Output:    {self.args.json_output}")
        log.out(f"Scenario:       {self.args.scenario or 'All scenarios'}")
        log.out(f"Use Previous Cluster:    {self.args.use_previous}")
        log.out("============================\n")


    def case_to_scene_obj(self, case):
        name = case["scenarioId"]
        sql  = case["sql"]
        classification = case["classfication"]
        
        return Scene(name, sql, classification, self.config_path, self.data_path)

    def load_cases_yaml(self, yaml_file):
        try:
            log.out(f"Loading YAML file: {yaml_file}")
            with open(yaml_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            # Print formatted test cases
            '''
            log.out("\n=== Test Cases ===")
            if 'testCases' in data:
                for i, case in enumerate(data['testCases'], 1):
                    log.out(f"\n--- Test Case {i} ---")
                    log.out(f"Scenario ID:     {case.get('scenarioId', 'N/A')}")
                    log.out(f"Classification:  {case.get('classfication', 'N/A')}")
                    log.out(f"Description:     {case.get('description', 'N/A')}")
                    log.out(f"SQL:\n{case.get('sql', 'N/A')}")
                    log.out("-" * 50)
            '''
            return data
            
        except FileNotFoundError:
            log.out(f"Error: File not found: {yaml_file}")
            return None
        except yaml.YAMLError as e:
            log.out(f"Error parsing YAML: {e}")
            return None
            
            
            

    #
    # ---------------------------- public methods ----------------------------
    #
 
    #
    # init    
    #
    def init(self):
        # parse args
        self.args = self.parser.parse_args()
        
        # config
        args_config = self.args.config
        if args_config is not None:
            self.config_path = args_config

        # data
        args_data = self.args.data
        if args_data is not None:
            self.data_path = args_data
            
        # timeout
        args_timeout = self.args.timeout
        if args_timeout is not None:
            self.timeout = args_timeout
            log.out(f"Set timeout to {self.timeout} seconds")    

        # max test time
        args_max_test_time = self.args.max_test_time
        if args_max_test_time is not None:
            self.max_test_time = args_max_test_time
            log.out(f"Set max test time to {self.max_test_time} seconds")    

        # max test time
        args_use_previous = self.args.use_previous
        if args_use_previous is not None:
            self.use_previous = args_use_previous
            log.out(f"Use previous cluster: {self.use_previous}")


        # init file name
        self.cases_yaml = os.path.join(self.config_path, 'cases.yaml')        
        self.scenes = self.load_cases_yaml(self.cases_yaml)
        
    #
    #  scenes
    #
    def get_scenes(self):
        scenario = self.get_scenario()
        if scenario is None:
            # all scenes
            scene_list = []
            for case in self.scenes['testCases']:
                scene_obj = self.case_to_scene_obj(case)
                scene_list.append(scene_obj)
            return scene_list
        else:
            # specific scene
            for case in self.scenes['testCases']:
                if case.get('scenarioId', '') == scenario:
                    scene_obj = self.case_to_scene_obj(case)
                    return [scene_obj]
            log.out(f"Error: Scenario '{scenario}' not found in configuration.")
            sys.exit(1)

# Global instance
cmd = CmdLine()