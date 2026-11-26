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

_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_COMMUNITY_DIR="$(cd "${_SCRIPT_DIR}/../../.." && pwd)"
_TAOSX_EXAMPLES_DIR="${_COMMUNITY_DIR}/docs/examples/taosx"

cd "${_TAOSX_EXAMPLES_DIR}"

exec python3 tests/test_migrate_active_active.py -v "$@"
