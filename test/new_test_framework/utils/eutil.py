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
import re
import shutil
import subprocess
import signal
from pathlib import Path
from .srvCtl import *
from .eos    import *
from .log import tdLog
from .sql import tdSql

def start_test_process(cmd, **kwargs):
    if os.name == "nt":
        # Launch the executable directly in a private console for Ctrl-Break.
        startup = subprocess.STARTUPINFO()
        startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startup.wShowWindow = subprocess.SW_HIDE
        return subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE,
                                startupinfo=startup, **kwargs)
    return subprocess.Popen(cmd, shell=True, start_new_session=True, **kwargs)


def signal_test_process(proc, interrupt=False):
    if proc.poll() is not None:
        return
    if os.name == "nt":
        if interrupt:
            # taosdump handles CTRL_BREAK_EVENT with its interruption handler.
            from new_test_framework.utils.server.win_process import _send_ctrl_break_event
            _send_ctrl_break_event(proc.pid)
        else:
            proc.kill()
    else:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT if interrupt else signal.SIGKILL)


def build_windows_test_program(source, name, build_dir):
    source = Path(source).resolve()
    community_dir = next((parent for parent in source.parents
                          if (parent / 'include' / 'client' / 'taos.h').is_file()), None)
    if community_dir is None:
        raise RuntimeError(f'Cannot locate the community SDK headers for {source}')
    bin_dir = Path(build_dir) / 'build' / 'bin'
    destination = bin_dir / (name + '.exe')
    compiler = shutil.which('gcc')
    if compiler is None:
        raise RuntimeError('Windows test helper requires gcc from the CI MSYS2 toolchain')
    command = [compiler, '-DWINDOWS', '-o', str(destination), str(source),
               '-I' + str(community_dir / 'include' / 'client'),
               '-I' + str(community_dir / 'include' / 'util'),
               str(bin_dir / 'taos.dll')]
    result = subprocess.run(command, capture_output=True, text=True, errors='replace', timeout=120)
    if result.returncode:
        raise RuntimeError(f'Failed to compile {name}: {result.stdout}\n{result.stderr}')
    return str(destination)

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
        if isWin():
            try:
                pattern = re.compile(key)
                with open(logFile, 'rb') as log:
                    # Bound each scan: a busy server may append faster than
                    # Python can search, so reading to EOF can never finish.
                    remaining = os.fstat(log.fileno()).st_size
                    cnt = 0
                    while remaining > 0:
                        line = log.readline(remaining)
                        if not line:
                            break
                        remaining -= len(line)
                        cnt += pattern.search(line.decode('utf-8', errors='replace')) is not None
            except FileNotFoundError:
                cnt = 0
            if cnt > 0:
                return cnt
            time.sleep(1)
            continue
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
    if isWin():
        from .server.win_process import _find_taosd_pid
        config_dir = sc.dnodeCfgPath(1)
        if os.path.basename(config_dir).lower() == "taos.cfg":
            config_dir = os.path.dirname(config_dir)
        pid = _find_taosd_pid(config_dir=config_dir)
        if pid is None:
            return 0
        process = psutil.Process(pid)
        started = datetime.datetime.fromtimestamp(process.create_time())
        # Windows does not expose the pthread names through /proc or
        # GetThreadDescription. Correlate worker startup records with live TIDs.
        workers = {}
        pattern = re.compile(r'worker:([^:\s]+):\d+ is running, thread:(\d+)')
        for log_file in Path(sc.dnodeLogPath(1)).glob('taosdlog.*'):
            if not log_file.is_file() or log_file.suffix == '.gz':
                continue
            with log_file.open(encoding='utf-8', errors='replace') as log:
                for line in log:
                    match = pattern.search(line)
                    if match is None:
                        continue
                    logged_at = datetime.datetime.strptime(
                        line[:21], '%m/%d %H:%M:%S.%f').replace(year=started.year)
                    if logged_at < started:
                        continue
                    name, tid = match.group(1), int(match.group(2))
                    if tid not in workers or logged_at > workers[tid][0]:
                        workers[tid] = (logged_at, name)
        live_tids = {thread.id for thread in process.threads()}
        return sum(tid in live_tids and threadName in name
                   for tid, (_, name) in workers.items())

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
