# Copyright (c) 2023 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
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

# -*- coding: utf-8 -*-

#
# about tools funciton extension
#

import sys
import os
import time
import datetime
import psutil
import glob
from .srvCtl import *
from .eos    import *
from .log import tdLog
from .sql import tdSql

# cpu frequent as random
def cpuRand(max):
    decimal = int(str(psutil.cpu_freq().current).split(".")[1])
    return decimal % max

# remove single and doulbe quotation
def removeQuota(origin):
    value = ""
    for c in origin:
        if c != '\'' and c != '"':
            value += c

    return value

# find string in taosd log
def findTaosdLog(key, dnodeIdx=1, retry=60):
    logPath = sc.dnodeLogPath(dnodeIdx)
    logFile = logPath + "/taosdlog.0"
    return findLog(logFile, key, retry)


# find string in log
def findLog(logFile, key, retry = 60):
    for i in range(retry):
        cmd = f"grep '{key}' {logFile} | wc -l"
        output, error = run(cmd)
        try:
            cnt = int(output)
            if cnt > 0:
                return cnt
        except ValueError:
            print(f"cmd ={cmd} out={output} err={error}")
        print(f"not found string: {key} in {logFile} , retry again {i} ...")    
        time.sleep(1)
    return 0

# find thread in taosd, return thread count
def findTaosdThread(threadName):
    """
    Find the first process named 'taosd' and count the number of threads 
    that contain the specified thread name.
    
    Args:
        threadName (str): Thread name to search for (supports partial matching)
        
    Returns:
        int: Number of matching threads in the first taosd process found, 
             returns 0 if no taosd process is found or no threads match.
    """
    # Step 1: Find all processes named "taosd"
    taosd_pids = []

    # Iterate through all process directories in /proc
    for pid_dir in glob.glob('/proc/[0-9]*'):
        try:
            # Extract PID from directory path
            pid = os.path.basename(pid_dir)

            # Read process command name
            with open(os.path.join(pid_dir, 'comm'), 'r') as f:
                comm = f.read().strip()

            # Check if process is taosd
            if comm == 'taosd':
                taosd_pids.append(pid)
        except:
            # Skip inaccessible process directories
            continue

    # Return 0 if no taosd process is found
    if not taosd_pids:
        return 0

    # Step 2: Get thread information for the first taosd process
    taosd_pid = taosd_pids[0]
    task_dir = os.path.join('/proc', taosd_pid, 'task')

    # Count threads matching the specified name
    thread_count = 0

    # Iterate through all threads of the process
    for tid in os.listdir(task_dir):
        try:
            # Read thread command name
            comm_file = os.path.join(task_dir, tid, 'comm')
            if os.path.exists(comm_file):
                with open(comm_file, 'r') as f:
                    thread_comm = f.read().strip()

                # Check if thread name contains the search string
                if threadName in thread_comm:
                    thread_count += 1
        except:
            # Skip inaccessible thread directories
            continue

    return thread_count


def check_db_cachemodel(dbname, cachemodel, retry=10):
    """
    Check whether the database's cachemodel matches the expected value.

    Args:
        dbname: Database name.
        cachemodel: Expected cachemodel value.
        retry: Max retry count, default 10.

    Returns:
        bool: True if matched, exit otherwise.
    """
    sql = (
        f"select `cachemodel` from information_schema.ins_databases "
        f"where name = '{dbname}'"
    )
    actual = None
    for _ in range(retry):
        tdSql.query(sql)
        if tdSql.queryRows > 0:
            actual = tdSql.queryResult[0][0]
            if str(actual) == str(cachemodel):
                tdLog.info(f"Database {dbname} cachemodel is {cachemodel}")
                return True
        time.sleep(1)
    tdLog.exit(f"Database {dbname} cachemodel is {actual}, expected {cachemodel}")


def check_explain_last_row_scan(sql, retry=10):
    """
    Check whether the explain plan contains "Last row scan" and
    does not contain "table scan".

    Args:
        sql: SELECT statement to explain (e.g. "select LAST_ROW(c1) from db.tb").
        retry: Max retry count, default 10.

    Returns:
        bool: True if plan contains "Last row scan" and does not contain
              "table scan"; exit after retry attempts otherwise.
    """
    explain_sql = (
        f"explain {sql.rstrip(';')}"
        if not sql.strip().lower().startswith("explain")
        else sql
    )
    for _ in range(retry):
        tdSql.query(explain_sql)
        plan_parts = []
        for row in (tdSql.queryResult or []):
            for cell in row:
                if cell is not None:
                    plan_parts.append(str(cell))
        plan_text = " ".join(plan_parts)
        plan_lower = plan_text.lower()
        has_last_row_scan = "last row scan" in plan_lower
        has_table_scan = "table scan" in plan_lower
        if has_last_row_scan and not has_table_scan:
            tdLog.info("Explain plan contains 'Last row scan' and does not contain 'table scan'")
            return True
        time.sleep(1)
    tdLog.exit("Explain plan does not contain 'Last row scan' or contains 'table scan'")
