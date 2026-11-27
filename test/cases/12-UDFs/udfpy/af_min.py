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

import pickle

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps([])

def finish(buf):
    mins = pickle.loads(buf)
    min_val = None
    for min in mins:
        if min_val is None or (min is not None and min < min_val):
            min_val = min
    return min_val

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    mins = pickle.loads(buf)
    min = None
    for i in range(rows):
        val = datablock.data(i, 0)
        if min is None or (val is not None and val < min) :
            min = val
    if min is not None: 
       mins.append(min)
    return pickle.dumps(mins) 
