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

def init():
    pass

# destroy
def destroy():
    pass

def process(block):
    (nrows, ncols) = block.shape()
    results = []
    for i in range(nrows):
        row = []
        for j in range(ncols):
            val = block.data(i, j)
            if val is None:
                row = None
                break
            row.append(val.decode('utf_32_le'))
        if row is None:
            results.append(None)
        else:        
            row_str = ''.join(row)
            results.append(row_str.encode('utf_32_le'))
    return results


