#!/bin/bash

# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

set +e

FILE=/usr/local/lib/libtaospyudf.so
if [ ! -f "$FILE" ]; then
    echo "$FILE does not exist."
    apt install -y python3 python3-dev python3-venv
    /usr/bin/python3 -m venv /udfenv
    source /udfenv/bin/activate
    pip3 install taospyudf
    ldconfig
    deactivate
else
    echo "show dependencies of $FILE"
    ldd $FILE
fi
