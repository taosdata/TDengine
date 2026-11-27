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

import json
import math

def init():
    pass

def destroy():
    pass

def start():
    return json.dumps(0.0).encode('utf-8')

def finish(buf):
    sum_squares = json.loads(buf)
    result = math.sqrt(sum_squares)
    return result

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    sum_squares = json.loads(buf)
    
    for i in range(rows):
        for j in range(cols):
            cell = datablock.data(i,j)
            if cell is not None:
                sum_squares += cell * cell
    return json.dumps(sum_squares).encode('utf-8') 
