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
from scene import Scene

class CmdLine:
    def __init__(self):
        self.scenes = []
        
    def init(self):
        print("Parsing command line arguments")
        # parse cmdline arguments here
        
        # load yarm config
        self.scenes_yarm = ""
    
    def load_cases_yarm(self, yarm_file):
        print(f"Loading yarm config from {yarm_file}")
        self.scenes_yarm = yarm_file    
        
    def arguments(self, name):
        if name == "scenario":
            return None
    
    def get_scene_yarm(self):
        return 
   
    def read_all_scenes(self):        
        return self.scenes
    
    def read_scene(self, scene_name):
        pass
    
    def get_scene(self, scene_name):
        for scene in self.scenes:
            if scene.name == scene_name:
                return [scene]
        return None
    
    def get_scenes(self):
        scenario = self.arguments("scenario")
        if scenario is None:
            return self.scenes
        else:
            return self.get_scene(scenario)
