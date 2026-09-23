#!/bin/bash

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

set -e
#set -x

# -----------------------Variables definition---------------------

if [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "Darwin"
else
    OS=$(cat /etc/*-release | grep "^NAME=" | cut -d= -f2)
    len=$(echo ${#OS})
    len=$((len-2))
    retval=$(echo -ne ${OS:1:${len}} | cut -d" " -f1)
    echo -ne $retval
fi
