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
import taos
import time

from cmdLine import cmd
from outLog import log


def taos_connect():
    host = cmd.get_host()
    port = cmd.get_port()
    user = cmd.get_user()
    return taos.connect(host=host, port=port, user=user, password=cmd.get_password())

def db_first_value(sql):
    conn = taos_connect()
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    conn.close()
    if len(results) > 0 and len(results[0]) > 0:
        return results[0][0]
    return None

def db_get_rows(sql):
    conn = taos_connect()
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()    
    conn.close()
    if results is None:
        return 0
    return len(results)

def wait_transactions_zero(seconds = 300, interval = 1):
    # wait end
    for i in range(seconds):
        sql ="show transactions;"
        rows =db_get_rows(sql)
        if rows == 0:
            log.out("waited all transactions are over.")
            return True
        time.sleep(interval)
    
    return False    