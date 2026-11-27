# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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

import taos

def create_connection():
    # all parameters are optional.
    conn = None
    host = "localhost"
    port = 6030
    try:
        conn = taos.connect(
            user="root",
            password="taosdata",
            host=host,
            port=port,
        )
        print(f"Connected to {host}:{port} successfully.");
    except Exception as err:
        print(f"Failed to connect to {host}:{port} , ErrMessage:{err}")
        raise err
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    create_connection()
