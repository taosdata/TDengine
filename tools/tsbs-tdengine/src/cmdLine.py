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
            default=120,
            metavar='SECONDS',
            help='Timeout in seconds for stream processing (default: 120)'
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
        
        # Parallelism
        self.parser.add_argument(
            '-p', '--parallelism',
            type=int,
            default=4,
            metavar='NUM',
            help='Processing parallelism level (default: 4)'
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
    
    
    def get_parallelism(self):
        """Get parallelism level"""
        return self.args.parallelism
    
    def show_config(self):
        """Print current configuration"""
        print("\n=== Current Configuration ===")
        print(f"Config File:    {self.args.config or 'Built-in default'}")
        print(f"Data File:      {self.args.data or 'Built-in default'}")
        print(f"Log Output:     {self.args.log_output}")
        print(f"JSON Output:    {self.args.json_output}")
        print(f"Scenario:       {self.args.scenario or 'All scenarios'}")
        print(f"Parallelism:    {self.args.parallelism}")
        print("============================\n")


    def case_to_scene_obj(self, case):
        name = case["scenarioId"]
        sql  = case["sql"]
        classification = case["classfication"]
        
        return Scene(name, sql, classification, self.config_path, self.data_path)

    def load_cases_yaml(self, yaml_file):
        try:
            print(f"Loading YAML file: {yaml_file}")
            with open(yaml_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            scenes = []
            # Print formatted test cases
            print("\n=== Test Cases ===")
            if 'testCases' in data:
                for i, case in enumerate(data['testCases'], 1):
                    print(f"\n--- Test Case {i} ---")
                    print(f"Scenario ID:     {case.get('scenarioId', 'N/A')}")
                    print(f"Classification:  {case.get('classfication', 'N/A')}")
                    print(f"Description:     {case.get('description', 'N/A')}")
                    print(f"SQL:\n{case.get('sql', 'N/A')}")
                    print("-" * 50)
            
            return data
            
        except FileNotFoundError:
            print(f"Error: File not found: {yaml_file}")
            return None
        except yaml.YAMLError as e:
            print(f"Error parsing YAML: {e}")
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
        print(f"Set timeout to {self.timeout} seconds")    

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
            print(f"Error: Scenario '{scenario}' not found in configuration.")
            sys.exit(1)

# Global instance
cmd = CmdLine()