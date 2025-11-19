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
import taos

from baseStep import BaseStep
from scene import Scene
from outMetrics import metrics

class PrepareEnv(BaseStep):
    def __init__(self, scene):
        self.scene = scene
        
    def exec_sql_file(self, conn, sql_file):
        with open(sql_file, 'r') as file:
            try:
                sql_commands = file.readlines()
                for command in sql_commands:
                    command = command.strip()
                    if command:
                        conn.execute(command)
                        print(f"exe success: {command}")
            except Exception as e:
                print(f"Error executing SQL file {sql_file}: {e}")
                metrics.set_status(self.scene.name, "Failed")

    def wait_stream_ready(self, conn, stream_name="", timeout=120):
        sql = "select * from information_schema.ins_stream_tasks where type = 'Trigger' and status != 'Running'"
        if len(stream_name) > 0:
            sql += f" and name = '{stream_name}'"
        cursor = conn.cursor()   
        print(f"Wait stream ready...")
        time.sleep(5)
        
        try:
            for i in range(timeout):
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) == 0:
                    print(f"wait {i} seconds stream is Running.")
                    return
                time.sleep(1)
            
            # Timeout reached
            info = f"stream task status not ready in {timeout} seconds"
            print(info)
            raise Exception(info)
        finally:
            # Always close cursor, even if exception occurs
            cursor.close()

        info = f"stream task status not ready in {timeout} seconds"
        print(info)
        raise Exception(info)

    def run(self):
        print("Prepare running...")
        conn = taos.connect()        
        # execute sql
        for table in self.scene.tables:
            sql_file = self.scene.get_sql_file(table)
            print(f"prepare environment using SQL file: {sql_file}")
            self.exec_sql_file(conn, sql_file)
        
        # create stream sql
        print("prepare execute main sql:")
        conn.execute(self.scene.sql)
        
        # wait for stream to be ready
        print("prepare wait stream ready...")
        self.wait_stream_ready(conn)
        conn.close()