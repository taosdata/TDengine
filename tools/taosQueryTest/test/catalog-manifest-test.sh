#!/usr/bin/env bash

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

set -euo pipefail

generator=$1
ds=$2
header=$3
source=$4
temporary_header=$(mktemp "${TMPDIR:-/tmp}/taosQueryTest-dimension-ids.XXXXXX")
temporary_source=$(mktemp "${TMPDIR:-/tmp}/taosQueryTest-dimension-source.XXXXXX")

cleanup() {
  rm -f -- "$temporary_header" "$temporary_source"
}
trap cleanup EXIT

python3 "$generator" --ds "$ds" --header "$temporary_header" --source "$temporary_source"
cmp -- "$temporary_header" "$header"
cmp -- "$temporary_source" "$source"
