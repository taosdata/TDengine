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

# return origin column one value 
def process(block):
    (nrows, ncols) = block.shape()
    results = []
    for i in range(nrows):
        rows = []
        for j in range(ncols):
            val = block.data(i, j)
            if type(val) is bytes:
                rows.append(val.decode('utf-8'))
            else:
                rows.append(repr(val))
        results.append(','.join(rows))
    return results

