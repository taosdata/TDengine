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
    return pickle.dumps(None)

def finish(buf):
    sum = pickle.loads(buf)
    return sum

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    sum = pickle.loads(buf)
    for i in range(rows):
        val = datablock.data(i, 0)
        if val is not None:
           if sum is None:
               sum = val
           else:
               sum += val               
    return pickle.dumps(sum) 
