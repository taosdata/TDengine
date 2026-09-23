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
import numpy as np

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps(0.0)

def finish(buf):
    return pickle.loads(buf)

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    state = pickle.loads(buf)
    row = []
    for i in range(rows):
        for j in range(cols):
            cell = datablock.data(i, j)
            if cell is not None:
                row.append(datablock.data(i, j))
    if len(row) > 1:
        new_state = np.cumsum(row)[-1]
    else:
        new_state = state
    return pickle.dumps(new_state)
