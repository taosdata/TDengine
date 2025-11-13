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
def exe(command, show = False):
    code = os.system(command)
    if show:
        print(f"eos.exe retcode={code} command:{command}")
    return code

def readFileContext(filename):
    file = open(filename)
    context = file.read()
    file.close()
    return context

# run return output and error
def run(command):
    id = time.time_ns() % 100000
    out = f"out_{id}.txt"
    err = f"err_{id}.txt"
    
    code = exe(command + f" 1>{out} 2>{err}")

    # read from file
    output = readFileContext(out)
    error  = readFileContext(err)

    # del
    if os.path.exists(out):
        os.remove(out)
    if os.path.exists(err):
        os.remove(err)

    return output, error, code


def waitStreamReady(stream_name="", timeout=120):
    sql = "select * from information_schema.ins_stream_tasks where type = 'Trigger' and status != 'Running'"
    if len(stream_name) > 0:
        sql += f" and name = '{stream_name}'"
    
    conn = taos.connect()
    cursor = conn.cursor()   
    print(f"Wait stream ready...")
    time.sleep(5)
    for i in range(timeout):
        cursor.execute(sql)
        results = cursor.fetchall()
        if len(results) == 0:
            conn.close()
            return
        time.sleep(1)
        

    info = f"stream task status not ready in {timeout} seconds"
    print(info)
    conn.close()
    raise Exception(info)