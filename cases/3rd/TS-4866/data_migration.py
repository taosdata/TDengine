# -----!/usr/bin/python3
###################################################################
#           Copyright (c) 2016-2099 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################
# -*- coding: utf-8 -*-

import textwrap
import argparse
import sys
from dataclasses import dataclass
import taos
import os
import subprocess
import json
import time
import psutil
import time

@dataclass
class CmdOption:
    """
    Represents the command line options for data migration.
    """
    host: str = None
    port: int = None
    config_dir: str = None
    thread_count: str = None
    num_of_records_per_req: int = None
    source_dbname: str = None
    source_stbname: str = None
    target_dbname: str = None
    target_stbname: str = None
    vgroups: int = None
    tables: int = None
    records: int = None
    timestamp_step: int = None
    interlace_rows: int = None
    stream_name: str = None
    cal_time: str = None


class Parser:
    """
    A class that builds a command-line argument parser for the DataMigration program.

    Attributes:
        prog (str): The name of the program.
    """

    prog = "DataMigration"

    def __init__(self):
        pass

    def buildCmdLineParser(self):
        """
        Builds and returns the command-line argument parser.

        Returns:
            argparse.ArgumentParser: The command-line argument parser.
        """
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            prog=self.prog,
            usage=f"\n   python3 data_migration.py [options]",
            add_help=False,
            description=textwrap.dedent('''\
                DataMigration: 

                '''))
        parser.add_argument(
            '--help',
            action="help",
            help="show this help message and exit"
        )
        parser.add_argument(
            '-h',
            '--host',
            action='store',
            default='localhost',
            type=str,
            metavar="hostname",
            help='The server FQDN to connect. The default host is localhost.'
        )
        parser.add_argument(
            '-p',
            '--port',
            action='store',
            default='6030',
            type=str,
            metavar="port",
            help='The password to use when connecting to the server.'
        )
        parser.add_argument(
            '-c',
            '--config-dir',
            action='store',
            default='/etc/taos',
            type=str,
            metavar="config_dir",
            help='Configuration directory.'
        )
        parser.add_argument(
            '-T',
            '--thread-count',
            action='store',
            default=os.cpu_count(),
            type=int,
            metavar="thread_count",
            help='The number of thread when insert data, default is cpu_count.'
        )
        parser.add_argument(
            '-r',
            '--num-of-records-per-req',
            action='store',
            default=1000,
            type=int,
            metavar="num_of_records_per_req",
            help='Number of records in each insert request, default is 1000.'
        )
        parser.add_argument(
            '-sdb',
            '--source-dbname',
            action='store',
            default="test1",
            type=str,
            metavar="source_dbname",
            help='Source dbname, default is test1.'
        )
        parser.add_argument(
            '-stb',
            '--source-stbname',
            action='store',
            default="meters",
            type=str,
            metavar="source_stbname",
            help='Source stbname, default is meters.'
        )
        parser.add_argument(
            '-tdb',
            '--target-dbname',
            action='store',
            default="test2",
            type=str,
            metavar="target_dbname",
            help='Target dbname, default is test2.'
        )
        parser.add_argument(
            '-ttb',
            '--target-stbname',
            action='store',
            default="stream_tb",
            type=str,
            metavar="target_stbname",
            help='Target stbname, default is stream_tb.'
        )
        parser.add_argument(
            '-v',
            '--vgroups',
            action='store',
            default=os.cpu_count(),
            type=int,
            metavar="vgroups",
            help='Specify Vgroups number for creating database.'
        )
        parser.add_argument(
            '-t',
            '--tables',
            action='store',
            default=10000,
            type=int,
            metavar="tables",
            help='Number of child tables, default is 10000.'
        )
        parser.add_argument(
            '-n',
            '--records',
            action='store',
            default=10000,
            type=int,
            metavar="records",
            help='Number of records for each table, default is 10000.'
        )
        parser.add_argument(
            '-s',
            '--timestamp-step',
            action='store',
            default=1000,
            type=int,
            metavar="timestamp_step",
            help='Timestamp step in milliseconds, default is 1000.'
        )
        parser.add_argument(
            '-B',
            '--interlace-rows',
            action='store',
            default="0",
            type=int,
            metavar="interlace_rows",
            help='The number of interlace rows insert into tables, default is 0.'
        )
        parser.add_argument(
            '-stn',
            '--stream-name',
            action='store',
            default="test_stream",
            type=str,
            metavar="stream_name",
            help='stream, default is test_stream.'
        )
        parser.add_argument(
            '-ot',
            '--cal-time',
            action='store',
            default=7200,
            type=int,
            metavar="cal_time",
            help='max cal time.'
        )
        return parser

    def get_opts(self, parser):
        """
        Parses the command-line arguments and returns the options.

        Args:
            parser (argparse.ArgumentParser): The command-line argument parser.

        Returns:
            CmdOption: The parsed command-line options.
        """
        opts = CmdOption()
        opts.host = parser.host if parser.host else "localhost"
        opts.port = parser.port if parser.port else 6030
        opts.config_dir = parser.config_dir if parser.config_dir else self.config_dir
        opts.thread_count = parser.thread_count if parser.thread_count else os.cpu_count()
        opts.num_of_records_per_req = parser.num_of_records_per_req if parser.num_of_records_per_req else 1000
        opts.source_dbname = parser.source_dbname if parser.source_dbname else "test1"
        opts.source_stbname = parser.source_stbname if parser.source_stbname else "meters"
        opts.target_dbname = parser.target_dbname if parser.target_dbname else "test2"
        opts.target_stbname = parser.target_stbname if parser.target_stbname else "stream_tb"
        opts.vgroups = parser.vgroups if parser.vgroups else os.cpu_count()
        opts.tables = parser.tables if parser.tables else 10000
        opts.records = parser.records if parser.records else 10000
        opts.timestamp_step = parser.timestamp_step if parser.timestamp_step else 1000
        opts.interlace_rows = parser.interlace_rows if parser.interlace_rows else 0
        opts.stream_name = parser.stream_name if parser.stream_name else "test_stream"
        opts.cal_time = parser.cal_time if parser.cal_time else "test_stream"
        origin_cmds = sys.argv[1:]
        origin_cmds.insert(0, self.prog)
        str_cmds = ' '.join(origin_cmds)
        opts.cmds = str_cmds
        return opts

class DB:
    """
    Represents a database connection.

    Args:
        host (str): The hostname or IP address of the database server.
        port (int): The port number of the database server.
        config_dir (str): The directory path where the database configuration files are located.

    Attributes:
        host (str): The hostname or IP address of the database server.
        port (int): The port number of the database server.
        config_dir (str): The directory path where the database configuration files are located.
        conn: The database connection object.
        timeout (int): The timeout value for the database connection.

    Methods:
        get_connection: Establishes a connection to the database server.

    """

    def __init__(self, host, port, config_dir):
        self.host = host
        self.port = port
        self.config_dir = config_dir
        self.conn = self.get_connection()
        self.timeout = 7200

    def get_connection(self):
        """
        Establishes a connection to the database server.

        Returns:
            The database connection object.

        """
        return taos.connect(host=self.host, port=int(self.port), config=self.config_dir, user='root', password='taosdata')

class DataMigration(DB):
    def __init__(self, host, port, config_dir):
        """
        Initialize the DataMigration class.

        Args:
            host (str): The host address.
            port (int): The port number.
            config_dir (str): The directory path for the configuration.

        """
        super().__init__(host, port, config_dir)
        self.taosBenchmark_json = os.path.dirname(__file__) + "/prepare.json"

    def prepare_json(self, thread_count, num_of_records_per_req, source_dbname, source_stbname, target_dbname, vgroups, childtable_count, row_count, timestamp_step):
        """
        Prepare a JSON template for data migration.

        Args:
            thread_count (int): Number of threads for data migration.
            num_of_records_per_req (int): Number of records per request.
            source_dbname (str): Name of the source database.
            source_stbname (str): Name of the source super table.
            target_dbname (str): Name of the target database.
            vgroups (list): List of virtual groups.
            childtable_count (int): Number of child tables.
            row_count (int): Number of rows to insert.
            timestamp_step (int): Step size for timestamps.

        Returns:
            None
        """
        json_template = {
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": self.host,
            "port": self.port,
            "user": "root",
            "password": "taosdata",
            "connection_pool_size": 8,
            "thread_count": thread_count,
            "create_table_thread_count": 100,
            "result_file": "./insert_res.txt",
            "confirm_parameter_prompt": "no",
            "num_of_records_per_req": num_of_records_per_req,
            "prepared_rand": 10000,
            "chinese": "no",
            "escape_character": "yes",
            "continue_if_fail": "no",
            "databases": [
                {
                    "dbinfo": {
                        "name": source_dbname,
                        "drop": "yes",
                        "vgroups": vgroups,
                        "precision": "ms"
                    },
                    "super_tables": [
                        {
                            "name": "meters",
                            "child_table_exists": "no",
                            "childtable_count": childtable_count,
                            "childtable_prefix": "d",
                            "auto_create_table": "no",
                            "batch_create_tbl_num": 1000,
                            "data_source": "rand",
                            "insert_mode": "taosc",
                            "non_stop_mode": "no",
                            "line_protocol": "line",
                            "insert_rows": row_count,
                            "childtable_limit": 0,
                            "childtable_offset": 0,
                            "interlace_rows": 0,
                            "insert_interval": 0,
                            "partial_col_num": 0,
                            "timestamp_step": timestamp_step,
                            "start_timestamp": "2020-10-01 00:00:00.000",
                            "sample_format": "csv",
                            "sample_file": "./sample.csv",
                            "use_sample_ts": "no",
                            "tags_file": "",
                            "columns": [
                                {"type": "FLOAT", "name": "current", "count": 1, "max": 12, "min": 8 },
                                { "type": "INT", "name": "voltage", "max": 225, "min": 215 },
                                { "type": "FLOAT", "name": "phase", "max": 1, "min": 0 }
                            ],
                            "tags": [
                                {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1},
                                {"type": "BINARY",  "name": "location", "len": 16,
                                    "values": ["San Francisco", "Los Angles", "San Diego",
                                        "San Jose", "Palo Alto", "Campbell", "Mountain View",
                                        "Sunnyvale", "Santa Clara", "Cupertino"]
                                }
                            ]
                        }
                    ]
                },
                {
                    "dbinfo": {
                        "name": target_dbname,
                        "drop": "yes",
                        "vgroups": vgroups,
                        "precision": "ms"
                    }
                }
            ]
        }
        with open(self.taosBenchmark_json, "w") as f:
            json.dump(json_template, f, indent=4)

    def prepare_data(self):
        """
        Prepares the data for migration using the taosBenchmark tool.

        Returns:
            bool: True if the data preparation is successful, False otherwise.
        """
        cmd = f'taosBenchmark -f {self.taosBenchmark_json}'
        try:
            with subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:
                for line in proc.stdout:
                    print(line, end='')
                # Check if the process has any error output
                error = proc.stderr.read()
                if error:
                    print("error_msg:", error)
                return True
        except subprocess.CalledProcessError as e:
            print("Command execution failed:", e)
            return False

    def prepare_data(self):
            """
            Prepares the data for migration.

            Executes the taosBenchmark command with the specified JSON file.
            """
            cmd = f'taosBenchmark -f {self.taosBenchmark_json}'
            self.exec_cmd(cmd)

    def create_stream(self, stream_name, source_dbname, source_stbname, target_dbname, target_stbname):
        """
        Create a stream for data migration.

        Args:
            stream_name (str): The name of the stream to be created.
            source_dbname (str): The name of the source database.
            source_stbname (str): The name of the source stable table.
            target_dbname (str): The name of the target database.
            target_stbname (str): The name of the target stable table.
        """
        print("creating stream...")
        cmd = f'CREATE STREAM IF NOT EXISTS {stream_name} TRIGGER at_once IGNORE UPDATE 0 IGNORE EXPIRED 0 FILL_HISTORY 1 INTO {target_dbname}.{target_stbname} TAGS(loc binary(16)) as select _wstart, last(current) as last_current,last(voltage) as last_voltage,last(phase) as last_phase from {source_dbname}.{source_stbname} partition by location as loc interval(60s)'
        self.conn.execute(cmd)

    def exec_cmd(self, cmd):
        """
        Executes a command in the shell and captures the output.

        Args:
            cmd (str): The command to be executed.

        Returns:
            bool: True if the command executed successfully, False otherwise.
        """
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print("error_msg:", result.stderr)
            return True
        except subprocess.CalledProcessError as e:
            print("Command execution failed:", e)
            return False

    def wait_fill_history_start(self, stream_name):
        cnt = 0
        cmd = f'select distinct history_task_id from information_schema.ins_stream_tasks where stream_name = "{stream_name}"'
        res = self.conn.query(cmd)
        query_result = res.fetch_all()
        while len(query_result) == 0 or (len(query_result) > 0 and query_result[0][0]) is None:
            time.sleep(1)
            res = self.conn.query(cmd)
            query_result = res.fetch_all()
            if cnt < self.timeout:
                cnt += 1
            else:
                return
    
    def wait_stream_finished(self, stream_name, cal_time, process):
        """
        Waits for the specified stream to finish its tasks in the database.

        Args:
            stream_name (str): The name of the stream.

        Returns:
            int: The number of seconds waited for the stream to finish. Returns None if the timeout is reached.
        """
        self.wait_fill_history_start(stream_name=stream_name)
        cnt = 0
        cmd = f'select distinct history_task_id from information_schema.ins_stream_tasks where stream_name = "{stream_name}"'
        res = self.conn.query(cmd)
        cpu_list = list()
        mem_list = list()
        while len(res.fetch_all()) != 0 or (len(res.fetch_all()) != 1 or res.fetch_all()[0][0] is not None):
            time.sleep(1)
            res = self.conn.query(cmd)
            cpu_usage = process.cpu_percent(interval=1)
            memory_info = process.memory_info().rss
            cpu_list.append(cpu_usage)
            mem_list.append(memory_info)
            if res.fetch_all()[0][0] is None:
                return cnt, cpu_list, mem_list
            if cnt < cal_time:
                cnt += 1
            else:
                return cnt, cpu_list, mem_list
        return cnt, cpu_list, mem_list

    def find_process_pid(self, process_name):
        """
        Find the PID of a process by its name.
        
        :param process_name: str. Name of the process to find.
        :return: list of PIDs matching the process name.
        """
        pids = []
        for proc in psutil.process_iter(['pid', 'name']):
            if proc.info['name'] == process_name:
                pids.append(proc.info['pid'])
        
        return pids
    
    def get_process(self, pid):
        try:
            process = psutil.Process(pid)
            return process
        except psutil.NoSuchProcess:
            print(f"No process found with PID {pid}.")
            return

# class Monitor:
#     def __init__(self):
#         pass

#     def find_process_pid(self, process_name):
#         """
#         Find the PID of a process by its name.
        
#         :param process_name: str. Name of the process to find.
#         :return: list of PIDs matching the process name.
#         """
#         pids = []
#         for proc in psutil.process_iter(['pid', 'name']):
#             if proc.info['name'] == process_name:
#                 pids.append(proc.info['pid'])
        
#         return pids

#     def monitor_process_cpu(self, pid, start_time, end_time):
#         """
#         Monitor CPU usage of a process between start_time and end_time.
        
#         :param pid: int. Process ID of the target process.
#         :param start_time: datetime. When to start monitoring.
#         :param end_time: datetime. When to stop monitoring.
#         """
#         try:
#             process = psutil.Process(pid)
#         except psutil.NoSuchProcess:
#             print(f"No process with PID {pid} found.")
#             return

#         while datetime.now() < start_time:
#             time.sleep((start_time - datetime.now()).total_seconds())

#         print(f"Starting to monitor PID {pid} at {datetime.now()}")

#         usage = []
        
#         while datetime.now() < end_time:
#             try:
#                 cpu_usage = process.cpu_percent(interval=1)
#                 print("CPU Usage: ", cpu_usage)
#                 usage.append(cpu_usage)
#                 print(f"Time: {datetime.now()}, CPU Usage: {cpu_usage}%")
#             except psutil.NoSuchProcess:
#                 print(f"Process with PID {pid} terminated before end_time.")
#                 break

#         if usage:
#             average_usage = sum(usage) / len(usage)
#             print(f"Average CPU Usage from {start_time} to {end_time}: {average_usage}%")
#             return average_usage
#         else:
#             print("No CPU usage data collected.")


if __name__ == "__main__":
    
    pars = Parser()
    parser = pars.buildCmdLineParser()
    opts = pars.get_opts(parser.parse_args())
    dmg = DataMigration(opts.host, opts.port, opts.config_dir)
    dmg.prepare_json(opts.thread_count, opts.num_of_records_per_req, opts.source_dbname, opts.source_stbname, opts.target_dbname, opts.vgroups, opts.tables, opts.records, opts.timestamp_step)
    dmg.conn.execute(f'drop stream if exists {opts.stream_name}')
    dmg.prepare_data()
    taosd_pid = dmg.find_process_pid("taosd")
    process = dmg.get_process(taosd_pid[0])
    memory_bf_stream = process.memory_info().rss
    time.sleep(10)
    start_time = time.time()
    
    
    dmg.create_stream(opts.stream_name, opts.source_dbname, opts.source_stbname, opts.target_dbname, opts.target_stbname)
    
    rtn = dmg.wait_stream_finished(opts.stream_name, opts.cal_time, process)
    end_time = time.time()
    time_usage = int(end_time-start_time)
    if not rtn[0]:
        print(f"Stream task is not finished in {dmg.timeout}s.")
    else:
        res = dmg.conn.query(f'select count(*) from {opts.target_dbname}.{opts.target_stbname};')
        perftime = res.fetch_all()[0][0]/time_usage
        print(f"Stream task finished in {time_usage}s and cal-perf is {perftime}rows/s.")
        print(f"CPU Usage during stream-computing --- [avg, min, max]: [{sum(rtn[1])/len(rtn[1]):.2f}%, {min(rtn[1]):.2f}, {max(rtn[1]):.2f}]")
        print(f"MEM Usage during stream-computing --- [avg, min, max]: [{(sum(rtn[2])/len(rtn[2])-memory_bf_stream)/1024/1024:.2f}MB, {(min(rtn[2])-memory_bf_stream)/1024/1024:.2f}MB, {(max(rtn[2])-memory_bf_stream)/1024/1024:.2f}MB]")
    # monitor = Monitor()
    # taosd_pid = monitor.find_process_pid("taosd")
    # print(taosd_pid)
    # monitor.monitor_process_cpu(taosd_pid[0], start_time, end_time)
    



