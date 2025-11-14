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
import taos
import time


# --------------------- util -----------------------

def relative_path(file_name):
    current_dir = os.path.dirname(__file__)
    return os.path.join(current_dir, file_name)


def read_file_context(filename):
    file = open(filename)
    context = file.read()
    file.close()
    return context

# run return output and error
def exe_cmd(command):
    id = time.time_ns() % 100000
    out = f"out_{id}.txt"
    err = f"err_{id}.txt"
    command += f" 1>{out} 2>{err}"
    
    code = os.system(command)

    # read from file
    output = read_file_context(out)
    error  = read_file_context(err)

    # del
    if os.path.exists(out):
        os.remove(out)
    if os.path.exists(err):
        os.remove(err)

    return output, error, code