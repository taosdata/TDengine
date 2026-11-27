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

import ctypes

TAOS_SYSTEM_ERROR = ctypes.c_int32(0x80ff0000).value
TAOS_DEF_ERROR_CODE = ctypes.c_int32(0x80000000).value


TSDB_CODE_MND_FUNC_NOT_EXIST    = (TAOS_DEF_ERROR_CODE | 0x0374)


TSDB_CODE_TSC_INVALID_OPERATION = (TAOS_DEF_ERROR_CODE | 0x0200)

TSDB_CODE_UDF_FUNC_EXEC_FAILURE = (TAOS_DEF_ERROR_CODE | 0x290A)


TSDB_CODE_TSC_INTERNAL_ERROR = (TAOS_DEF_ERROR_CODE | 0x02FF)

TSDB_CODE_PAR_SYNTAX_ERROR = (TAOS_DEF_ERROR_CODE | 0x2600)

TSDB_CODE_PAR_INVALID_COLS_FUNCTION = (TAOS_DEF_ERROR_CODE | 0x2689)

